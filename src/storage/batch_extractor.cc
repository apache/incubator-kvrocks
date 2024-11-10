/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "batch_extractor.h"

#include <glog/logging.h>

#include "cluster/redis_slot.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "types/redis_bitmap.h"

void WriteBatchExtractor::LogData(const rocksdb::Slice &blob) {
  // Currently, we only have two kinds of log data
  if (ServerLogData::IsServerLogData(blob.data())) {
    ServerLogData server_log;
    if (server_log.Decode(blob).IsOK()) {
      // We don't handle server log currently
    }
  } else {
    // Redis type log data
    if (auto s = log_data_.Decode(blob); !s.IsOK()) {
      LOG(WARNING) << "Failed to decode Redis type log: " << s.Msg();
    }
  }
}

rocksdb::Status WriteBatchExtractor::PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) {
  if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::SecondarySubkey)) {
    return rocksdb::Status::OK();
  }

  std::string ns, user_key;
  std::vector<std::string> command_args;

  if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
    std::tie(ns, user_key) = ExtractNamespaceKey<std::string>(key, is_slot_id_encoded_);
    auto key_slot_id = GetSlotIdFromKey(user_key);
    if (slot_range_.IsValid() && !slot_range_.Contains(key_slot_id)) {
      return rocksdb::Status::OK();
    }

    Metadata metadata(kRedisNone);
    auto s = metadata.Decode(value);
    if (!s.ok()) return s;

    if (metadata.Type() == kRedisString) {
      command_args = {"SET", user_key, value.ToString().substr(Metadata::GetOffsetAfterExpire(value[0]))};
      resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
      if (metadata.expire > 0) {
        command_args = {"PEXPIREAT", user_key, std::to_string(metadata.expire)};
        resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
      }
    } else if (metadata.Type() == kRedisJson) {
      JsonValue json_value;
      s = redis::Json::FromRawString(value.ToString(), &json_value);
      if (!s.ok()) return s;
      auto json_bytes = json_value.Dump();
      if (!json_bytes) return rocksdb::Status::Corruption(json_bytes.Msg());
      command_args = {"JSON.SET", user_key, "$", json_bytes.GetValue()};
      resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
      if (metadata.expire > 0) {
        command_args = {"PEXPIREAT", user_key, std::to_string(metadata.expire)};
        resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
      }
    } else if (metadata.expire > 0) {
      auto args = log_data_.GetArguments();
      if (args->size() > 0) {
        auto parse_result = ParseInt<int>((*args)[0], 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(
              fmt::format("failed to parse Redis command from log data: {}", parse_result.Msg()));
        }

        auto cmd = static_cast<RedisCommand>(*parse_result);
        if (cmd == kRedisCmdExpire) {
          command_args = {"PEXPIREAT", user_key, std::to_string(metadata.expire)};
          resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
        }
      }
    }

    if (metadata.Type() == kRedisStream) {
      auto args = log_data_.GetArguments();
      bool is_set_id = args && args->size() > 0 && (*args)[0] == "XSETID";
      if (!is_set_id) {
        return rocksdb::Status::OK();
      }

      StreamMetadata stream_metadata;
      auto s = stream_metadata.Decode(value);
      if (!s.ok()) return s;

      command_args = {"XSETID",
                      user_key,
                      stream_metadata.last_entry_id.ToString(),
                      "ENTRIESADDED",
                      std::to_string(stream_metadata.entries_added),
                      "MAXDELETEDID",
                      stream_metadata.max_deleted_entry_id.ToString()};
      resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
    }

    return rocksdb::Status::OK();
  }

  if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
    InternalKey ikey(key, is_slot_id_encoded_);
    user_key = ikey.GetKey().ToString();
    auto key_slot_id = GetSlotIdFromKey(user_key);
    if (slot_range_.IsValid() && !slot_range_.Contains(key_slot_id)) {
      return rocksdb::Status::OK();
    }

    std::string sub_key = ikey.GetSubKey().ToString();
    ns = ikey.GetNamespace().ToString();

    switch (log_data_.GetRedisType()) {
      case kRedisHash:
        command_args = {"HSET", user_key, sub_key, value.ToString()};
        break;
      case kRedisList: {
        auto args = log_data_.GetArguments();
        if (args->empty()) {
          LOG(ERROR)
              << "Failed to parse write_batch in PutCF. Type=List: no arguments, at least should contain a command";
          return rocksdb::Status::OK();
        }

        auto parse_result = ParseInt<int>((*args)[0], 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(
              fmt::format("failed to parse Redis command from log data: {}", parse_result.Msg()));
        }

        auto cmd = static_cast<RedisCommand>(*parse_result);
        switch (cmd) {
          case kRedisCmdLSet:
            if (args->size() < 2) {
              LOG(ERROR) << "Failed to parse write_batch in PutCF. Command=LSET: no enough arguments, at least should "
                            "contain an index";
              return rocksdb::Status::OK();
            }

            command_args = {"LSET", user_key, (*args)[1], value.ToString()};
            break;
          case kRedisCmdLInsert:
            if (first_seen_) {
              if (args->size() < 4) {
                LOG(ERROR) << "Failed to parse write_batch in PutCF. Command=LINSERT: no enough arguments, should "
                              "contain before pivot value";
                return rocksdb::Status::OK();
              }

              command_args = {"LINSERT", user_key, (*args)[1] == "1" ? "before" : "after", (*args)[2], (*args)[3]};
              first_seen_ = false;
            }
            break;
          case kRedisCmdLPush:
            command_args = {"LPUSH", user_key, value.ToString()};
            break;
          case kRedisCmdRPush:
            command_args = {"RPUSH", user_key, value.ToString()};
            break;
          case kRedisCmdLRem:
            // LREM will be parsed in DeleteCF, so ignore it here
            break;
          case kRedisCmdLMove:
            // LMOVE will be parsed in DeleteCF, so ignore it here
            break;
          default:
            LOG(ERROR) << "Failed to parse write_batch in PutCF. Type=List: unhandled command with code "
                       << *parse_result;
        }
        break;
      }
      case kRedisSet:
        command_args = {"SADD", user_key, sub_key};
        break;
      case kRedisZSet: {
        double score = DecodeDouble(value.data());
        command_args = {"ZADD", user_key, std::to_string(score), sub_key};
        break;
      }
      case kRedisBitmap: {
        auto args = log_data_.GetArguments();
        if (args->empty()) {
          LOG(ERROR)
              << "Failed to parse write_batch in PutCF. Type=Bitmap: no arguments, at least should contain a command";
          return rocksdb::Status::OK();
        }

        auto parsed_cmd = ParseInt<int>((*args)[0], 10);
        if (!parsed_cmd) {
          return rocksdb::Status::InvalidArgument(
              fmt::format("failed to parse Redis command from log data: {}", parsed_cmd.Msg()));
        }

        auto cmd = static_cast<RedisCommand>(*parsed_cmd);
        switch (cmd) {
          case kRedisCmdSetBit: {
            if (args->size() < 2) {
              LOG(ERROR) << "Failed to parse write_batch in PutCF. Command=SETBIT: no enough arguments, should contain "
                            "an offset";
              return rocksdb::Status::OK();
            }

            auto parsed_offset = ParseInt<int>((*args)[1], 10);
            if (!parsed_offset) {
              return rocksdb::Status::InvalidArgument(
                  fmt::format("failed to parse an offset of SETBIT: {}", parsed_offset.Msg()));
            }
            bool bit_value = redis::Bitmap::GetBitFromValueAndOffset(value.ToStringView(), *parsed_offset);
            command_args = {"SETBIT", user_key, (*args)[1], bit_value ? "1" : "0"};
            break;
          }
          case kRedisCmdBitOp:
            if (first_seen_) {
              if (args->size() < 4) {
                LOG(ERROR) << "Failed to parse write_batch in PutCF. Command=BITOP: no enough arguments, at least "
                              "should contain srckey";
                return rocksdb::Status::OK();
              }

              command_args = {"BITOP", (*args)[1], user_key};
              command_args.insert(command_args.end(), args->begin() + 2, args->end());
              first_seen_ = false;
            }
            break;
          case kRedisCmdBitfield:
            command_args = {"BITFIELD", user_key};
            command_args.insert(command_args.end(), args->begin() + 1, args->end());
            break;
          default:
            LOG(ERROR) << "Failed to parse write_batch in PutCF. Type=Bitmap: unhandled command with code "
                       << *parsed_cmd;
            return rocksdb::Status::OK();
        }
        break;
      }
      case kRedisSortedint: {
        if (!to_redis_) {
          command_args = {"SIADD", user_key, std::to_string(DecodeFixed64(sub_key.data()))};
        }
        break;
      }
        // TODO: to implement the case of kRedisBloomFilter
      default:
        break;
    }
  } else if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::Stream)) {
    auto s = ExtractStreamAddCommand(is_slot_id_encoded_, key, value, &command_args);
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed to parse write_batch in PutCF. Type=Stream: " << s.Msg();
      return rocksdb::Status::OK();
    }
  }

  if (!command_args.empty()) {
    resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteCF(uint32_t column_family_id, const Slice &key) {
  if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::SecondarySubkey)) {
    return rocksdb::Status::OK();
  }

  std::vector<std::string> command_args;
  std::string ns;

  if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
    std::string user_key;
    std::tie(ns, user_key) = ExtractNamespaceKey<std::string>(key, is_slot_id_encoded_);

    auto key_slot_id = GetSlotIdFromKey(user_key);
    if (slot_range_.IsValid() && !slot_range_.Contains(key_slot_id)) {
      return rocksdb::Status::OK();
    }

    command_args = {"DEL", user_key};
  } else if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
    InternalKey ikey(key, is_slot_id_encoded_);
    std::string user_key = ikey.GetKey().ToString();
    auto key_slot_id = GetSlotIdFromKey(user_key);
    if (slot_range_.IsValid() && !slot_range_.Contains(key_slot_id)) {
      return rocksdb::Status::OK();
    }

    std::string sub_key = ikey.GetSubKey().ToString();
    ns = ikey.GetNamespace().ToString();

    switch (log_data_.GetRedisType()) {
      case kRedisHash:
        command_args = {"HDEL", user_key, sub_key};
        break;
      case kRedisSet:
        command_args = {"SREM", user_key, sub_key};
        break;
      case kRedisZSet:
        command_args = {"ZREM", user_key, sub_key};
        break;
      case kRedisList: {
        auto args = log_data_.GetArguments();
        if (args->empty()) {
          LOG(ERROR)
              << "Failed to parse write_batch in DeleteCF. Type=List: no arguments, at least should contain a command";
          return rocksdb::Status::OK();
        }

        auto parse_result = ParseInt<int>((*args)[0], 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(
              fmt::format("failed to parse Redis command from log data: {}", parse_result.Msg()));
        }

        auto cmd = static_cast<RedisCommand>(*parse_result);
        switch (cmd) {
          case kRedisCmdLTrim:
            if (first_seen_) {
              if (args->size() < 3) {
                LOG(ERROR) << "Failed to parse write_batch in DeleteCF; Command=LTRIM: no enough arguments, should "
                              "contain start and stop";
                return rocksdb::Status::OK();
              }

              command_args = {"LTRIM", user_key, (*args)[1], (*args)[2]};
              first_seen_ = false;
            }
            break;
          case kRedisCmdLRem:
            if (first_seen_) {
              if (args->size() < 3) {
                LOG(ERROR) << "Failed to parse write_batch in DeleteCF. Command=LREM: no enough arguments, should "
                              "contain count and value";
                return rocksdb::Status::OK();
              }

              command_args = {"LREM", user_key, (*args)[1], (*args)[2]};
              first_seen_ = false;
            }
            break;
          case kRedisCmdLPop:
            command_args = {"LPOP", user_key};
            break;
          case kRedisCmdRPop:
            command_args = {"RPOP", user_key};
            break;
          case kRedisCmdLMove:
            if (first_seen_) {
              if (args->size() < 5) {
                LOG(ERROR) << "Failed to parse write_batch in DeleteCF; Command=LMOVE: no enough arguments, should "
                              "contain source, destination and where/from arguments";
                return rocksdb::Status::OK();
              }
              command_args = {"LMOVE", (*args)[1], (*args)[2], (*args)[3], (*args)[4]};
              first_seen_ = false;
            }
            break;
          default:
            LOG(ERROR) << "Failed to parse write_batch in DeleteCF. Type=List: unhandled command with code "
                       << *parse_result;
        }
        break;
      }
      case kRedisSortedint: {
        if (!to_redis_) {
          command_args = {"SIREM", user_key, std::to_string(DecodeFixed64(sub_key.data()))};
        }
        break;
      }
      default:
        break;
    }
  } else if (column_family_id == static_cast<uint32_t>(ColumnFamilyID::Stream)) {
    InternalKey ikey(key, is_slot_id_encoded_);
    Slice encoded_id = ikey.GetSubKey();
    redis::StreamEntryID entry_id;
    GetFixed64(&encoded_id, &entry_id.ms);
    GetFixed64(&encoded_id, &entry_id.seq);
    command_args = {"XDEL", ikey.GetKey().ToString(), entry_id.ToString()};
  }

  if (!command_args.empty()) {
    resp_commands_[ns].emplace_back(redis::ArrayOfBulkStrings(command_args));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteRangeCF([[maybe_unused]] uint32_t column_family_id,
                                                   [[maybe_unused]] const Slice &begin_key,
                                                   [[maybe_unused]] const Slice &end_key) {
  // Do nothing with DeleteRange operations
  return rocksdb::Status::OK();
}

Status WriteBatchExtractor::ExtractStreamAddCommand(bool is_slot_id_encoded, const Slice &subkey, const Slice &value,
                                                    std::vector<std::string> *command_args) {
  InternalKey ikey(subkey, is_slot_id_encoded);
  std::string user_key = ikey.GetKey().ToString();
  *command_args = {"XADD", user_key};

  std::vector<std::string> values;
  auto s = redis::DecodeRawStreamEntryValue(value.ToString(), &values);
  if (!s.IsOK()) {
    return s.Prefixed("failed to decode stream values");
  }

  Slice encoded_id = ikey.GetSubKey();
  redis::StreamEntryID entry_id;
  GetFixed64(&encoded_id, &entry_id.ms);
  GetFixed64(&encoded_id, &entry_id.seq);

  command_args->emplace_back(entry_id.ToString());
  command_args->insert(command_args->end(), values.begin(), values.end());

  return Status::OK();
}
