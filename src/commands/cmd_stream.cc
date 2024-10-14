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

#include <algorithm>
#include <limits>
#include <memory>
#include <stdexcept>

#include "command_parser.h"
#include "commander.h"
#include "error_constants.h"
#include "event_util.h"
#include "server/server.h"
#include "status.h"
#include "time_util.h"
#include "types/redis_stream.h"

namespace redis {

class CommandXAck : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];
    group_name_ = args[2];
    StreamEntryID tmp_id;
    for (size_t i = 3; i < args.size(); ++i) {
      auto s = ParseStreamEntryID(args[i], &tmp_id);
      if (!s.IsOK()) return s;
      entry_ids_.emplace_back(tmp_id);
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    uint64_t acknowledged = 0;

    auto s = stream_db.DeletePelEntries(ctx, stream_name_, group_name_, entry_ids_, &acknowledged);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::Integer(acknowledged);

    return Status::OK();
  }

 private:
  std::string stream_name_;
  std::string group_name_;
  std::vector<StreamEntryID> entry_ids_;
};

class CommandXAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    bool entry_id_found = false;
    stream_name_ = args[1];

    for (size_t i = 2; i < args.size();) {
      auto val = entry_id_found ? args[i] : util::ToLower(args[i]);

      if (val == "nomkstream" && !entry_id_found) {
        nomkstream_ = true;
        ++i;
        continue;
      }

      if (val == "maxlen" && !entry_id_found) {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        size_t max_len_idx = 0;
        bool eq_sign_found = false;
        if (args[i + 1] == "=") {
          max_len_idx = i + 2;
          eq_sign_found = true;
        } else {
          max_len_idx = i + 1;
        }

        if (max_len_idx >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        auto parse_result = ParseInt<uint64_t>(args[max_len_idx], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        max_len_ = *parse_result;
        with_max_len_ = true;

        i += eq_sign_found ? 3 : 2;
        continue;
      }

      if (val == "minid" && !entry_id_found) {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        size_t min_id_idx = 0;
        bool eq_sign_found = false;
        if (args[i + 1] == "=") {
          min_id_idx = i + 2;
          eq_sign_found = true;
        } else {
          min_id_idx = i + 1;
        }

        if (min_id_idx >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        auto s = ParseStreamEntryID(args[min_id_idx], &min_id_);
        if (!s.IsOK()) return s;

        with_min_id_ = true;
        i += eq_sign_found ? 3 : 2;
        continue;
      }

      if (val == "limit" && !entry_id_found) {
        return {Status::RedisParseErr, errLimitOptionNotAllowed};
      }

      if (!entry_id_found) {
        auto result = ParseNextStreamEntryIDStrategy(val);
        if (!result.IsOK()) return result;

        next_id_strategy_ = std::move(*result);

        entry_id_found = true;
        ++i;
        continue;
      }

      name_value_pairs_.push_back(val);
      ++i;
    }

    if (name_value_pairs_.empty() || name_value_pairs_.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::StreamAddOptions options;
    options.nomkstream = nomkstream_;
    if (with_max_len_) {
      options.trim_options.strategy = StreamTrimStrategy::MaxLen;
      options.trim_options.max_len = max_len_;
    }
    if (with_min_id_) {
      options.trim_options.strategy = StreamTrimStrategy::MinID;
      options.trim_options.min_id = min_id_;
    }
    options.next_id_strategy = std::move(next_id_strategy_);

    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    StreamEntryID entry_id;

    auto s = stream_db.Add(ctx, stream_name_, options, name_value_pairs_, &entry_id);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound() && nomkstream_) {
      *output = conn->NilString();
      return Status::OK();
    }

    *output = redis::BulkString(entry_id.ToString());

    srv->OnEntryAddedToStream(conn->GetNamespace(), stream_name_, entry_id);

    return Status::OK();
  }

 private:
  std::string stream_name_;
  uint64_t max_len_ = 0;
  redis::StreamEntryID min_id_;
  std::unique_ptr<redis::NextStreamEntryIDGenerationStrategy> next_id_strategy_;
  std::vector<std::string> name_value_pairs_;
  bool nomkstream_ = false;
  bool with_max_len_ = false;
  bool with_min_id_ = false;
};

class CommandXDel : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); ++i) {
      redis::StreamEntryID id;
      auto s = ParseStreamEntryID(args[i], &id);
      if (!s.IsOK()) {
        return s;
      }

      ids_.push_back(id);
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    uint64_t deleted = 0;

    auto s = stream_db.DeleteEntries(ctx, args_[1], ids_, &deleted);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(deleted);

    return Status::OK();
  }

 private:
  std::vector<redis::StreamEntryID> ids_;
};

class CommandXClaim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() < 6) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    CommandParser parser(args, 1);
    stream_name_ = GET_OR_RET(parser.TakeStr());
    group_name_ = GET_OR_RET(parser.TakeStr());
    consumer_name_ = GET_OR_RET(parser.TakeStr());
    auto parse_result = parser.TakeInt<int64_t>();
    if (!parse_result.IsOK()) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    min_idle_time_ms_ = parse_result.GetValue();
    if (min_idle_time_ms_ < 0) {
      min_idle_time_ms_ = 0;
    }

    while (parser.Good() && !isOption(parser.RawPeek())) {
      auto raw_id = GET_OR_RET(parser.TakeStr());
      redis::StreamEntryID id;
      auto s = ParseStreamEntryID(raw_id, &id);
      if (!s.IsOK()) {
        return s;
      }
      entry_ids_.emplace_back(id);
    }

    while (parser.Good()) {
      if (parser.EatEqICase("idle")) {
        auto parse_result = parser.TakeInt<int64_t>();
        if (!parse_result.IsOK()) {
          return {Status::RedisParseErr, errValueNotInteger};
        }
        if (parse_result.GetValue() < 0) {
          return {Status::RedisParseErr, "IDLE for XCLAIM must be non-negative"};
        }
        stream_claim_options_.idle_time_ms = parse_result.GetValue();
      } else if (parser.EatEqICase("time")) {
        auto parse_result = parser.TakeInt<int64_t>();
        if (!parse_result.IsOK()) {
          return {Status::RedisParseErr, errValueNotInteger};
        }
        if (parse_result.GetValue() < 0) {
          return {Status::RedisParseErr, "TIME for XCLAIM must be non-negative"};
        }
        stream_claim_options_.with_time = true;
        stream_claim_options_.last_delivery_time_ms = parse_result.GetValue();
      } else if (parser.EatEqICase("retrycount")) {
        auto parse_result = parser.TakeInt<int64_t>();
        if (!parse_result.IsOK()) {
          return {Status::RedisParseErr, errValueNotInteger};
        }
        if (parse_result.GetValue() < 0) {
          return {Status::RedisParseErr, "RETRYCOUNT for XCLAIM must be non-negative"};
        }
        stream_claim_options_.with_retry_count = true;
        stream_claim_options_.last_delivery_count = parse_result.GetValue();
      } else if (parser.EatEqICase("force")) {
        stream_claim_options_.force = true;
      } else if (parser.EatEqICase("justid")) {
        stream_claim_options_.just_id = true;
      } else if (parser.EatEqICase("lastid")) {
        auto last_id = GET_OR_RET(parser.TakeStr());
        auto s = ParseStreamEntryID(last_id, &stream_claim_options_.last_delivered_id);
        if (!s.IsOK()) {
          return s;
        }
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    StreamClaimResult result;

    auto s = stream_db.ClaimPelEntries(ctx, stream_name_, group_name_, consumer_name_, min_idle_time_ms_, entry_ids_,
                                       stream_claim_options_, &result);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisNoGroup, "No such key '" + stream_name_ + "' or consumer group '" + group_name_ + "'"};
      }
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!stream_claim_options_.just_id) {
      output->append(redis::MultiLen(result.entries.size()));

      for (const auto &e : result.entries) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(e.key));
        output->append(conn->MultiBulkString(e.values));
      }
    } else {
      output->append(redis::MultiLen(result.ids.size()));
      for (const auto &id : result.ids) {
        output->append(redis::BulkString(id));
      }
    }

    return Status::OK();
  }

 private:
  std::string stream_name_;
  std::string group_name_;
  std::string consumer_name_;
  uint64_t min_idle_time_ms_;
  std::vector<StreamEntryID> entry_ids_;
  StreamClaimOptions stream_claim_options_;

  bool static isOption(const std::string &arg) {
    static const std::unordered_set<std::string> options = {"idle", "time", "retrycount", "force", "justid", "lastid"};
    return options.find(util::ToLower(arg)) != options.end();
  }
};

class CommandAutoClaim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    key_name_ = GET_OR_RET(parser.TakeStr());
    group_name_ = GET_OR_RET(parser.TakeStr());
    consumer_name_ = GET_OR_RET(parser.TakeStr());
    if (auto parse_status = parser.TakeInt<uint64_t>(); !parse_status.IsOK()) {
      return {Status::RedisParseErr, "Invalid min-idle-time argument for XAUTOCLAIM"};
    } else {
      options_.min_idle_time_ms = parse_status.GetValue();
    }

    auto start_str = GET_OR_RET(parser.TakeStr());
    if (!start_str.empty() && start_str.front() == '(') {
      options_.exclude_start = true;
      start_str = start_str.substr(1);
    }
    if (!options_.exclude_start && start_str == "-") {
      options_.start_id = StreamEntryID::Minimum();
    } else {
      auto parse_status = ParseRangeStart(start_str, &options_.start_id);
      if (!parse_status.IsOK()) {
        return parse_status;
      }
    }

    if (parser.EatEqICase("count")) {
      uint64_t count = GET_OR_RET(parser.TakeInt<uint64_t>());
      constexpr uint64_t min_count = 1;
      uint64_t max_count = std::numeric_limits<int64_t>::max() /
                           (std::max(static_cast<uint64_t>(sizeof(StreamEntryID)), options_.attempts_factors));
      if (count < min_count || count > max_count) {
        return {Status::RedisParseErr, "COUNT must be > 0"};
      }
      options_.count = count;
    }

    if (parser.Good() && parser.EatEqICase("justid")) {
      options_.just_id = true;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    StreamAutoClaimResult result;

    auto s = stream_db.AutoClaim(ctx, key_name_, group_name_, consumer_name_, options_, &result);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        return {Status::RedisNoGroup, "No such key '" + key_name_ + "' or consumer group '" + group_name_ + "'"};
      }
      return {Status::RedisExecErr, s.ToString()};
    }
    return sendResults(conn, result, output);
  }

 private:
  Status sendResults([[maybe_unused]] Connection *conn, const StreamAutoClaimResult &result,
                     std::string *output) const {
    output->append(redis::MultiLen(3));
    output->append(redis::BulkString(result.next_claim_id));
    output->append(redis::MultiLen(result.entries.size()));
    for (const auto &item : result.entries) {
      if (options_.just_id) {
        output->append(redis::BulkString(item.key));
      } else {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(item.key));
        output->append(redis::MultiLen(item.values.size()));
        for (const auto &value_item : item.values) {
          output->append(redis::BulkString(value_item));
        }
      }
    }

    output->append(redis::MultiLen(result.deleted_ids.size()));
    for (const auto &item : result.deleted_ids) {
      output->append(redis::BulkString(item));
    }

    return Status::OK();
  }

  std::string key_name_;
  std::string group_name_;
  std::string consumer_name_;
  StreamAutoClaimOptions options_;
};

class CommandXGroup : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    subcommand_ = util::ToLower(GET_OR_RET(parser.TakeStr()));
    stream_name_ = GET_OR_RET(parser.TakeStr());
    group_name_ = GET_OR_RET(parser.TakeStr());

    if (subcommand_ == "create") {
      if (args.size() < 5 || args.size() > 8) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }

      xgroup_create_options_.last_id = GET_OR_RET(parser.TakeStr());

      while (parser.Good()) {
        if (parser.EatEqICase("mkstream")) {
          xgroup_create_options_.mkstream = true;
        } else if (parser.EatEqICase("entriesread")) {
          auto parse_result = parser.TakeInt<int64_t>();
          if (!parse_result.IsOK()) {
            return {Status::RedisParseErr, errValueNotInteger};
          }
          if (parse_result.GetValue() < 0 && parse_result.GetValue() != -1) {
            return {Status::RedisParseErr, "value for ENTRIESREAD must be positive or -1"};
          }
          xgroup_create_options_.entries_read = parse_result.GetValue();
        } else {
          return parser.InvalidSyntax();
        }
      }

      return Status::OK();
    }

    if (subcommand_ == "destroy") {
      if (args.size() != 4) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }

      return Status::OK();
    }

    if (subcommand_ == "createconsumer" || subcommand_ == "delconsumer") {
      if (args.size() != 5) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }
      consumer_name_ = GET_OR_RET(parser.TakeStr());

      return Status::OK();
    }

    if (subcommand_ == "setid") {
      if (args.size() != 5 && args.size() != 7) {
        return {Status::RedisParseErr, errWrongNumOfArguments};
      }

      xgroup_create_options_.last_id = GET_OR_RET(parser.TakeStr());
      if (args.size() == 7) {
        if (parser.EatEqICase("entriesread")) {
          auto parse_result = parser.TakeInt<int64_t>();
          if (!parse_result.IsOK()) {
            return {Status::RedisParseErr, errValueNotInteger};
          }
          if (parse_result.GetValue() < 0 && parse_result.GetValue() != -1) {
            return {Status::RedisParseErr, "value for ENTRIESREAD must be positive or -1"};
          }
          xgroup_create_options_.entries_read = parse_result.GetValue();
        } else {
          return parser.InvalidSyntax();
        }
      }

      return Status::OK();
    }

    return {Status::RedisParseErr, "unknown subcommand"};
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());

    if (subcommand_ == "create") {
      auto s = stream_db.CreateGroup(ctx, stream_name_, xgroup_create_options_, group_name_);
      if (!s.ok()) {
        if (s.IsBusy()) {
          return {Status::RedisBusyGroup, "consumer group name '" + group_name_ + "' already exists"};
        }
        return {Status::RedisExecErr, s.ToString()};
      }

      *output = redis::SimpleString("OK");
    }

    if (subcommand_ == "destroy") {
      uint64_t delete_cnt = 0;
      auto s = stream_db.DestroyGroup(ctx, stream_name_, group_name_, &delete_cnt);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (delete_cnt > 0) {
        *output = redis::Integer(1);
      } else {
        *output = redis::Integer(0);
      }
    }

    if (subcommand_ == "createconsumer") {
      int created_number = 0;
      auto s = stream_db.CreateConsumer(ctx, stream_name_, group_name_, consumer_name_, &created_number);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          return {Status::RedisNoGroup, "No such consumer group " + group_name_ + " for key name " + stream_name_};
        }
        return {Status::RedisExecErr, s.ToString()};
      }

      *output = redis::Integer(created_number);
    }

    if (subcommand_ == "delconsumer") {
      uint64_t deleted_pel = 0;
      auto s = stream_db.DestroyConsumer(ctx, stream_name_, group_name_, consumer_name_, deleted_pel);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          return {Status::RedisNoGroup, "No such consumer group " + group_name_ + " for key name " + stream_name_};
        }
        return {Status::RedisExecErr, s.ToString()};
      }

      *output = redis::Integer(deleted_pel);
    }

    if (subcommand_ == "setid") {
      auto s = stream_db.GroupSetId(ctx, stream_name_, group_name_, xgroup_create_options_);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          return {Status::RedisNoGroup, "No such consumer group " + group_name_ + " for key name " + stream_name_};
        }
        return {Status::RedisExecErr, s.ToString()};
      }

      *output = redis::SimpleString("OK");
    }

    return Status::OK();
  }

 private:
  std::string subcommand_;
  std::string stream_name_;
  std::string group_name_;
  std::string consumer_name_;
  StreamXGroupCreateOptions xgroup_create_options_;
};

class CommandXLen : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 2) {
      redis::StreamEntryID id;
      auto s = redis::ParseStreamEntryID(args[2], &id);
      if (!s.IsOK()) return s;

      options_.with_entry_id = true;
      options_.entry_id = id;

      if (args.size() > 3) {
        if (args[3] == "-") {
          options_.to_first = true;
        } else if (args[3] != "+") {
          return {Status::RedisParseErr, errInvalidSyntax};
        }
      }
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    uint64_t len = 0;

    auto s = stream_db.Len(ctx, args_[1], options_, &len);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(len);

    return Status::OK();
  }

 private:
  redis::StreamLenOptions options_;
};

class CommandXInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto val = util::ToLower(args[1]);
    if (val == "stream" && args.size() >= 2) {
      subcommand_ = "stream";

      if (args.size() > 3 && util::ToLower(args[3]) == "full") {
        full_ = true;
      }

      if (args.size() > 5 && util::ToLower(args[4]) == "count") {
        auto parse_result = ParseInt<uint64_t>(args[5], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
      }
    } else if (val == "groups" && args.size() == 3) {
      subcommand_ = "groups";
    } else if (val == "consumers" && args.size() == 4) {
      subcommand_ = "consumers";
    } else {
      return {Status::RedisParseErr, errUnknownSubcommandOrWrongArguments};
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "stream") {
      return getStreamInfo(ctx, srv, conn, output);
    }

    if (subcommand_ == "groups") {
      return getGroupInfo(ctx, srv, conn, output);
    }

    if (subcommand_ == "consumers") {
      return getConsumerInfo(ctx, srv, conn, output);
    }
    return Status::OK();
  }

 private:
  uint64_t count_ = 10;  // default Redis value
  std::string subcommand_;
  bool full_ = false;

  Status getStreamInfo(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    redis::StreamInfo info;

    auto s = stream_db.GetStreamInfo(ctx, args_[2], full_, count_, &info);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, errNoSuchKey};
    }

    if (!full_) {
      output->append(conn->HeaderOfMap(7));
    } else {
      output->append(conn->HeaderOfMap(6));
    }
    output->append(redis::BulkString("length"));
    output->append(redis::Integer(info.size));
    output->append(redis::BulkString("last-generated-id"));
    output->append(redis::BulkString(info.last_generated_id.ToString()));
    output->append(redis::BulkString("max-deleted-entry-id"));
    output->append(redis::BulkString(info.max_deleted_entry_id.ToString()));
    output->append(redis::BulkString("entries-added"));
    output->append(redis::Integer(info.entries_added));
    output->append(redis::BulkString("recorded-first-entry-id"));
    output->append(redis::BulkString(info.recorded_first_entry_id.ToString()));
    if (!full_) {
      output->append(redis::BulkString("first-entry"));
      if (info.first_entry) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(info.first_entry->key));
        output->append(conn->MultiBulkString(info.first_entry->values));
      } else {
        output->append(conn->NilString());
      }
      output->append(redis::BulkString("last-entry"));
      if (info.last_entry) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(info.last_entry->key));
        output->append(conn->MultiBulkString(info.last_entry->values));
      } else {
        output->append(conn->NilString());
      }
    } else {
      output->append(redis::BulkString("entries"));
      output->append(redis::MultiLen(info.entries.size()));
      for (const auto &e : info.entries) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(e.key));
        output->append(conn->MultiBulkString(e.values));
      }
    }

    return Status::OK();
  }

  Status getGroupInfo(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    std::vector<std::pair<std::string, StreamConsumerGroupMetadata>> result_vector;

    auto s = stream_db.GetGroupInfo(ctx, args_[2], result_vector);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, errNoSuchKey};
    }

    output->append(redis::MultiLen(result_vector.size()));
    for (auto const &it : result_vector) {
      output->append(conn->HeaderOfMap(6));
      output->append(redis::BulkString("name"));
      output->append(redis::BulkString(it.first));
      output->append(redis::BulkString("consumers"));
      output->append(redis::Integer(it.second.consumer_number));
      output->append(redis::BulkString("pending"));
      output->append(redis::Integer(it.second.pending_number));
      output->append(redis::BulkString("last-delivered-id"));
      output->append(redis::BulkString(it.second.last_delivered_id.ToString()));
      output->append(redis::BulkString("entries-read"));
      if (it.second.entries_read == -1) {
        output->append(conn->NilString());
      } else {
        output->append(redis::Integer(it.second.entries_read));
      }
      output->append(redis::BulkString("lag"));
      if (it.second.lag == UINT64_MAX) {
        output->append(conn->NilString());
      } else {
        output->append(redis::Integer(it.second.lag));
      }
    }

    return Status::OK();
  }

  Status getConsumerInfo(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    std::vector<std::pair<std::string, redis::StreamConsumerMetadata>> result_vector;

    auto s = stream_db.GetConsumerInfo(ctx, args_[2], args_[3], result_vector);

    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, errNoSuchKey};
    }

    output->append(redis::MultiLen(result_vector.size()));
    auto now_ms = util::GetTimeStampMS();
    for (auto const &it : result_vector) {
      output->append(conn->HeaderOfMap(4));
      output->append(redis::BulkString("name"));
      output->append(redis::BulkString(it.first));
      output->append(redis::BulkString("pending"));
      output->append(redis::Integer(it.second.pending_number));
      output->append(redis::BulkString("idle"));
      output->append(redis::Integer(now_ms - it.second.last_attempted_interaction_ms));
      output->append(redis::BulkString("inactive"));
      output->append(redis::Integer(now_ms - it.second.last_successful_interaction_ms));
    }

    return Status::OK();
  }
};

class CommandXPending : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    stream_name_ = GET_OR_RET(parser.TakeStr());
    group_name_ = GET_OR_RET(parser.TakeStr());
    if (parser.EatEqICase("idle")) {
      options_.idle_time = GET_OR_RET(parser.TakeInt<uint64_t>());
      options_.with_time = true;
    }

    if (parser.Good()) {
      std::string start_id, end_id;
      start_id = GET_OR_RET(parser.TakeStr());
      end_id = GET_OR_RET(parser.TakeStr());
      if (start_id != "-") {
        auto s = ParseStreamEntryID(start_id, &options_.start_id);
        if (!s.IsOK()) {
          return s;
        }
      }

      if (end_id != "+") {
        auto s = ParseStreamEntryID(start_id, &options_.end_id);
        if (!s.IsOK()) {
          return s;
        }
      }

      options_.count = GET_OR_RET(parser.TakeInt<uint64_t>());
      options_.with_count = true;
      if (parser.Good()) {
        options_.consumer = GET_OR_RET(parser.TakeStr());
        options_.with_consumer = true;
      }
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());
    std::vector<std::pair<std::string, int>> pending_infos;
    StreamGetPendingEntryResult results;
    options_.stream_name = stream_name_;
    options_.group_name = group_name_;
    std::vector<StreamNACK> ext_results;

    auto s = stream_db.GetPendingEntries(ctx, options_, results, ext_results);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    if (options_.with_count) {
      return SendExtResults(conn, output, ext_results);
    }
    return SendResults(conn, output, results);
  }

  static Status SendResults([[maybe_unused]] Connection *conn, std::string *output,
                            StreamGetPendingEntryResult &results) {
    // NOTE: In the case that our stream has no pending elements, Redis will
    // return NilString for the first and last entry IDs, and a nil array
    // for the consumer infos. Make this a special case to maintain consistency
    // with Redis.
    if (results.pending_number == 0) {
      output->append(redis::MultiLen(4));
      output->append(redis::Integer(0));
      output->append(conn->NilString());
      output->append(conn->NilString());
      output->append(conn->NilArray());
    } else {
      output->append(redis::MultiLen(4));
      output->append(redis::Integer(results.pending_number));
      output->append(redis::BulkString(results.first_entry_id.ToString()));
      output->append(redis::BulkString(results.last_entry_id.ToString()));
      output->append(redis::MultiLen(results.consumer_infos.size()));
      for (const auto &entry : results.consumer_infos) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(entry.first));
        output->append(redis::BulkString(std::to_string(entry.second)));
      }
    }

    return Status::OK();
  }

  static Status SendExtResults([[maybe_unused]] Connection *conn, std::string *output,
                               std::vector<StreamNACK> &ext_results) {
    output->append(redis::MultiLen(ext_results.size()));
    for (const auto &entry : ext_results) {
      output->append(redis::MultiLen(4));
      output->append(redis::BulkString(entry.id.ToString()));
      output->append(redis::BulkString(entry.pel_entry.consumer_name));
      output->append(redis::Integer(entry.pel_entry.last_delivery_time_ms));
      output->append(redis::Integer(entry.pel_entry.last_delivery_count));
    }

    return Status::OK();
  }

 private:
  std::string group_name_;
  std::string stream_name_;
  std::string consumer_name_;
  StreamPendingOptions options_;
};

class CommandXRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    if (args[2] == "-") {
      start_ = redis::StreamEntryID::Minimum();
    } else if (args[2][0] == '(') {
      exclude_start_ = true;
      auto s = ParseRangeStart(args[2].substr(1), &start_);
      if (!s.IsOK()) return s;
    } else if (args[2] == "+") {
      start_ = redis::StreamEntryID::Maximum();
    } else {
      auto s = ParseRangeStart(args[2], &start_);
      if (!s.IsOK()) return s;
    }

    if (args[3] == "+") {
      end_ = redis::StreamEntryID::Maximum();
    } else if (args[3][0] == '(') {
      exclude_end_ = true;
      auto s = ParseRangeEnd(args[3].substr(1), &end_);
      if (!s.IsOK()) return s;
    } else if (args[3] == "-") {
      end_ = redis::StreamEntryID::Minimum();
    } else {
      auto s = ParseRangeEnd(args[3], &end_);
      if (!s.IsOK()) return s;
    }

    if (args.size() >= 5 && util::ToLower(args[4]) == "count") {
      if (args.size() != 6) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      with_count_ = true;

      auto parse_result = ParseInt<uint64_t>(args[5], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (with_count_ && count_ == 0) {
      *output = conn->NilString();
      return Status::OK();
    }

    redis::Stream stream_db(srv->storage, conn->GetNamespace());

    redis::StreamRangeOptions options;
    options.reverse = false;
    options.start = start_;
    options.end = end_;
    options.with_count = with_count_;
    options.count = count_;
    options.exclude_start = exclude_start_;
    options.exclude_end = exclude_end_;

    std::vector<StreamEntry> result;

    auto s = stream_db.Range(ctx, stream_name_, options, &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(redis::MultiLen(result.size()));

    for (const auto &e : result) {
      output->append(redis::MultiLen(2));
      output->append(redis::BulkString(e.key));
      output->append(conn->MultiBulkString(e.values));
    }

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID start_;
  StreamEntryID end_;
  uint64_t count_ = 0;
  bool exclude_start_ = false;
  bool exclude_end_ = false;
  bool with_count_ = false;
};

class CommandXRevRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    if (args[2] == "+") {
      start_ = redis::StreamEntryID::Maximum();
    } else if (args[2][0] == '(') {
      exclude_start_ = true;
      auto s = ParseRangeEnd(args[2].substr(1), &start_);
      if (!s.IsOK()) return s;
    } else if (args[2] == "-") {
      start_ = redis::StreamEntryID::Minimum();
    } else {
      auto s = ParseRangeEnd(args[2], &start_);
      if (!s.IsOK()) return s;
    }

    if (args[3] == "-") {
      end_ = redis::StreamEntryID::Minimum();
    } else if (args[3][0] == '(') {
      exclude_end_ = true;
      auto s = ParseRangeStart(args[3].substr(1), &end_);
      if (!s.IsOK()) return s;
    } else if (args[3] == "+") {
      end_ = redis::StreamEntryID::Maximum();
    } else {
      auto s = ParseRangeStart(args[3], &end_);
      if (!s.IsOK()) return s;
    }

    if (args.size() >= 5 && util::ToLower(args[4]) == "count") {
      if (args.size() != 6) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      with_count_ = true;

      auto parse_result = ParseInt<uint64_t>(args[5]);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (with_count_ && count_ == 0) {
      *output = conn->NilString();
      return Status::OK();
    }

    redis::Stream stream_db(srv->storage, conn->GetNamespace());

    redis::StreamRangeOptions options;
    options.reverse = true;
    options.start = start_;
    options.end = end_;
    options.with_count = with_count_;
    options.count = count_;
    options.exclude_start = exclude_start_;
    options.exclude_end = exclude_end_;

    std::vector<StreamEntry> result;

    auto s = stream_db.Range(ctx, stream_name_, options, &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(redis::MultiLen(result.size()));

    for (const auto &e : result) {
      output->append(redis::MultiLen(2));
      output->append(redis::BulkString(e.key));
      output->append(conn->MultiBulkString(e.values));
    }

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID start_;
  StreamEntryID end_;
  uint64_t count_ = 0;
  bool exclude_start_ = false;
  bool exclude_end_ = false;
  bool with_count_ = false;
};

class CommandXRead : public Commander,
                     private EvbufCallbackBase<CommandXRead, false>,
                     private EventCallbackBase<CommandXRead> {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t streams_word_idx = 0;

    for (size_t i = 1; i < args.size();) {
      auto arg = util::ToLower(args[i]);

      if (arg == "streams") {
        streams_word_idx = i;
        break;
      }

      if (arg == "count") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        with_count_ = true;

        auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
        continue;
      }

      if (arg == "block") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        block_ = true;

        auto parse_result = ParseInt<int64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        if (*parse_result < 0) {
          return {Status::RedisParseErr, errTimeoutIsNegative};
        }

        block_timeout_ = *parse_result;
        i += 2;
        continue;
      }

      ++i;
    }

    if (streams_word_idx == 0) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if ((args.size() - streams_word_idx - 1) % 2 != 0) {
      return {Status::RedisParseErr, errUnbalancedStreamList};
    }

    size_t number_of_streams = (args.size() - streams_word_idx - 1) / 2;

    for (size_t i = streams_word_idx + 1; i <= streams_word_idx + number_of_streams; ++i) {
      streams_.push_back(args[i]);
      const auto &id_str = args[i + number_of_streams];
      bool get_latest = id_str == "$";
      latest_marks_.push_back(get_latest);
      StreamEntryID id;
      if (!get_latest) {
        auto s = ParseStreamEntryID(id_str, &id);
        if (!s.IsOK()) {
          return s;
        }
      }
      ids_.push_back(id);
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());

    std::vector<redis::StreamReadResult> results;

    for (size_t i = 0; i < streams_.size(); ++i) {
      if (latest_marks_[i]) {
        continue;
      }

      redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = ids_[i];
      options.end = StreamEntryID::Maximum();
      options.with_count = with_count_;
      options.count = count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.Range(ctx, streams_[i], options, &result);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (result.size() > 0) {
        results.emplace_back(streams_[i], result);
      }
    }

    if (block_ && results.empty()) {
      if (conn->IsInExec()) {
        *output = conn->NilArray();
        return Status::OK();  // No blocking in multi-exec
      }

      return BlockingRead(srv, conn, &stream_db);
    }

    if (!block_ && results.empty()) {
      *output = conn->NilArray();
      return Status::OK();
    }

    return SendResults(conn, output, results);
  }

  static Status SendResults(Connection *conn, std::string *output, const std::vector<StreamReadResult> &results) {
    output->append(redis::MultiLen(results.size()));

    for (const auto &result : results) {
      output->append(redis::MultiLen(2));
      output->append(redis::BulkString(result.name));
      output->append(redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(entry.key));
        output->append(conn->MultiBulkString(entry.values));
      }
    }

    return Status::OK();
  }

  Status BlockingRead(Server *srv, Connection *conn, redis::Stream *stream_db) {
    if (!with_count_) {
      with_count_ = true;
      count_ = blocked_default_count_;
    }

    for (size_t i = 0; i < streams_.size(); ++i) {
      if (latest_marks_[i]) {
        StreamEntryID last_generated_id;
        engine::Context ctx(srv->storage);
        auto s = stream_db->GetLastGeneratedID(ctx, streams_[i], &last_generated_id);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }

        ids_[i] = last_generated_id;
      }
    }

    srv_ = srv;
    conn_ = conn;

    srv_->BlockOnStreams(streams_, ids_, conn_);

    auto bev = conn->GetBufferEvent();
    SetCB(bev);

    if (block_timeout_ > 0) {
      timer_.reset(NewTimer(bufferevent_get_base(bev)));
      timeval tm;
      if (block_timeout_ > 1000) {
        tm.tv_sec = block_timeout_ / 1000;
        tm.tv_usec = (block_timeout_ % 1000) * 1000;
      } else {
        tm.tv_sec = 0;
        tm.tv_usec = block_timeout_ * 1000;
      }

      evtimer_add(timer_.get(), &tm);
    }

    return {Status::BlockingCmd};
  }

  void OnWrite(bufferevent *bev) {
    if (timer_ != nullptr) {
      timer_.reset();
    }

    unblockAll();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);

    redis::Stream stream_db(srv_->storage, conn_->GetNamespace());

    std::vector<StreamReadResult> results;
    engine::Context ctx(srv_->storage);
    for (size_t i = 0; i < streams_.size(); ++i) {
      redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = ids_[i];
      options.end = StreamEntryID::Maximum();
      options.with_count = with_count_;
      options.count = count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.Range(ctx, streams_[i], options, &result);
      if (!s.ok() && !s.IsNotFound()) {
        conn_->Reply(redis::Error({Status::NotOK, s.ToString()}));
        return;
      }

      if (result.size() > 0) {
        results.emplace_back(streams_[i], result);
      }
    }

    if (results.empty()) {
      conn_->Reply(conn_->NilArray());
    }

    SendReply(results);
  }

  void SendReply(const std::vector<StreamReadResult> &results) {
    std::string output;

    output.append(redis::MultiLen(results.size()));

    for (const auto &result : results) {
      output.append(redis::MultiLen(2));
      output.append(redis::BulkString(result.name));
      output.append(redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output.append(redis::MultiLen(2));
        output.append(redis::BulkString(entry.key));
        output.append(conn_->MultiBulkString(entry.values));
      }
    }

    conn_->Reply(output);
  }

  void OnEvent(bufferevent *bev, int16_t events) {
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (timer_ != nullptr) {
        timer_.reset();
      }
      unblockAll();
    }
    conn_->OnEvent(bev, events);
  }

  void TimerCB(int, [[maybe_unused]] int16_t events) {
    conn_->Reply(conn_->NilString());

    timer_.reset();

    unblockAll();

    auto bev = conn_->GetBufferEvent();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  std::vector<std::string> streams_;
  std::vector<StreamEntryID> ids_;
  std::vector<bool> latest_marks_;
  Server *srv_ = nullptr;
  Connection *conn_ = nullptr;
  UniqueEvent timer_;
  uint64_t count_ = 0;
  int64_t block_timeout_ = 0;
  int blocked_default_count_ = 1000;
  bool with_count_ = false;
  bool block_ = false;

  void unblockAll() { srv_->UnblockOnStreams(streams_, conn_); }
};

class CommandXReadGroup : public Commander,
                          private EvbufCallbackBase<CommandXReadGroup, false>,
                          private EventCallbackBase<CommandXReadGroup> {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t streams_word_idx = 0;
    if (util::ToLower(args[1]) != "group") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    group_name_ = args[2];
    consumer_name_ = args[3];

    for (size_t i = 4; i < args.size();) {
      auto arg = util::ToLower(args[i]);

      if (arg == "streams") {
        streams_word_idx = i;
        break;
      }

      if (arg == "count") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        with_count_ = true;

        auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
        continue;
      }

      if (arg == "block") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        block_ = true;

        auto parse_result = ParseInt<int64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        if (*parse_result < 0) {
          return {Status::RedisParseErr, errTimeoutIsNegative};
        }

        block_timeout_ = *parse_result;
        i += 2;
        continue;
      }

      if (arg == "noack") {
        noack_ = true;
      }

      ++i;
    }

    if (streams_word_idx == 0) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if ((args.size() - streams_word_idx - 1) % 2 != 0) {
      return {Status::RedisParseErr, errUnbalancedStreamList};
    }

    size_t number_of_streams = (args.size() - streams_word_idx - 1) / 2;

    for (size_t i = streams_word_idx + 1; i <= streams_word_idx + number_of_streams; ++i) {
      streams_.push_back(args[i]);
      const auto &id_str = args[i + number_of_streams];
      bool get_latest = id_str == ">";
      latest_marks_.push_back(get_latest);
      if (!get_latest) {
        block_ = false;
      }
      StreamEntryID id;
      if (!get_latest) {
        auto s = ParseStreamEntryID(id_str, &id);
        if (!s.IsOK()) {
          return s;
        }
      }
      ids_.push_back(id);
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());

    std::vector<redis::StreamReadResult> results;

    for (size_t i = 0; i < streams_.size(); ++i) {
      redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = ids_[i];
      options.end = StreamEntryID::Maximum();
      options.with_count = with_count_;
      options.count = count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.RangeWithPending(ctx, streams_[i], options, &result, group_name_, consumer_name_, noack_,
                                          latest_marks_[i]);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (result.size() > 0) {
        results.emplace_back(streams_[i], result);
      }
    }

    if (block_ && results.empty()) {
      if (conn->IsInExec()) {
        output->append(redis::MultiLen(streams_.size()));
        for (const auto &stream_name : streams_) {
          output->append(redis::MultiLen(2));
          output->append(redis::BulkString(stream_name));
          output->append(redis::MultiLen(0));
        }
        return Status::OK();  // No blocking in multi-exec
      }

      return BlockingRead(srv, conn, &stream_db);
    }

    if (!block_ && results.empty()) {
      output->append(redis::MultiLen(streams_.size()));
      for (const auto &stream_name : streams_) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(stream_name));
        output->append(redis::MultiLen(0));
      }
      return Status::OK();
    }

    return SendResults(conn, output, results);
  }

  Status SendResults(Connection *conn, std::string *output, const std::vector<StreamReadResult> &results) {
    output->append(redis::MultiLen(results.size()));
    int id = 0;
    for (const auto &result : results) {
      output->append(redis::MultiLen(2));
      output->append(redis::BulkString(result.name));
      output->append(redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output->append(redis::MultiLen(2));
        output->append(redis::BulkString(entry.key));
        if (entry.values.size() == 0 && !latest_marks_[id]) {
          output->append(conn->NilString());
          continue;
        }
        output->append(conn->MultiBulkString(entry.values));
      }
      ++id;
    }

    return Status::OK();
  }

  Status BlockingRead(Server *srv, Connection *conn, [[maybe_unused]] redis::Stream *stream_db) {
    if (!with_count_) {
      with_count_ = true;
      count_ = blocked_default_count_;
    }

    srv_ = srv;
    conn_ = conn;

    srv_->BlockOnStreams(streams_, ids_, conn_);

    auto bev = conn->GetBufferEvent();
    SetCB(bev);

    if (block_timeout_ > 0) {
      timer_.reset(NewTimer(bufferevent_get_base(bev)));
      timeval tm;
      if (block_timeout_ > 1000) {
        tm.tv_sec = block_timeout_ / 1000;
        tm.tv_usec = (block_timeout_ % 1000) * 1000;
      } else {
        tm.tv_sec = 0;
        tm.tv_usec = block_timeout_ * 1000;
      }

      evtimer_add(timer_.get(), &tm);
    }

    return {Status::BlockingCmd};
  }

  void OnWrite(bufferevent *bev) {
    if (timer_ != nullptr) {
      timer_.reset();
    }

    unblockAll();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);

    redis::Stream stream_db(srv_->storage, conn_->GetNamespace());

    std::vector<StreamReadResult> results;
    engine::Context ctx(srv_->storage);
    for (size_t i = 0; i < streams_.size(); ++i) {
      redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = ids_[i];
      options.end = StreamEntryID::Maximum();
      options.with_count = with_count_;
      options.count = count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.RangeWithPending(ctx, streams_[i], options, &result, group_name_, consumer_name_, noack_,
                                          latest_marks_[i]);
      if (!s.ok() && !s.IsNotFound()) {
        conn_->Reply(redis::Error({Status::NotOK, s.ToString()}));
        return;
      }

      if (result.size() > 0) {
        results.emplace_back(streams_[i], result);
      }
    }

    if (results.empty()) {
      conn_->Reply(redis::MultiLen(-1));
    }

    SendReply(results);
  }

  void SendReply(const std::vector<StreamReadResult> &results) {
    std::string output;

    output.append(redis::MultiLen(results.size()));

    for (const auto &result : results) {
      output.append(redis::MultiLen(2));
      output.append(redis::BulkString(result.name));
      output.append(redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output.append(redis::MultiLen(2));
        output.append(redis::BulkString(entry.key));
        output.append(conn_->MultiBulkString(entry.values));
      }
    }

    conn_->Reply(output);
  }

  void OnEvent(bufferevent *bev, int16_t events) {
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (timer_ != nullptr) {
        timer_.reset();
      }
      unblockAll();
    }
    conn_->OnEvent(bev, events);
  }

  void TimerCB(int, [[maybe_unused]] int16_t events) {
    conn_->Reply(conn_->NilString());

    timer_.reset();

    unblockAll();

    auto bev = conn_->GetBufferEvent();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  std::vector<std::string> streams_;
  std::vector<StreamEntryID> ids_;
  std::vector<bool> latest_marks_;
  std::string group_name_;
  std::string consumer_name_;
  bool noack_ = false;

  Server *srv_ = nullptr;
  Connection *conn_ = nullptr;
  UniqueEvent timer_;
  uint64_t count_ = 0;
  int64_t block_timeout_ = 0;
  int blocked_default_count_ = 1000;
  bool with_count_ = false;
  bool block_ = false;

  void unblockAll() { srv_->UnblockOnStreams(streams_, conn_); }
};

class CommandXTrim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    bool eq_sign_found = false;

    auto trim_strategy = util::ToLower(args[2]);
    if (trim_strategy == "maxlen") {
      strategy_ = StreamTrimStrategy::MaxLen;

      size_t max_len_idx = 0;
      if (args[3] != "=") {
        max_len_idx = 3;
      } else {
        max_len_idx = 4;
        eq_sign_found = true;
      }

      if (max_len_idx >= args.size()) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto parse_result = ParseInt<uint64_t>(args[max_len_idx], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      max_len_ = *parse_result;
    } else if (trim_strategy == "minid") {
      strategy_ = StreamTrimStrategy::MinID;

      size_t min_id_idx = 0;
      if (args[3] != "=") {
        min_id_idx = 3;
      } else {
        min_id_idx = 4;
        eq_sign_found = true;
      }

      if (min_id_idx >= args.size()) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto s = ParseStreamEntryID(args[min_id_idx], &min_id_);
      if (!s.IsOK()) {
        return s;
      }
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    bool limit_option_found = false;
    if (eq_sign_found) {
      if (args.size() > 6 && util::ToLower(args[5]) == "limit") {
        limit_option_found = true;
      }
    } else {
      if (args.size() > 5 && util::ToLower(args[4]) == "limit") {
        limit_option_found = true;
      }
    }

    if (limit_option_found) {
      return {Status::RedisParseErr, errLimitOptionNotAllowed};
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());

    StreamTrimOptions options;
    options.strategy = strategy_;
    options.max_len = max_len_;
    options.min_id = min_id_;

    uint64_t removed = 0;

    auto s = stream_db.Trim(ctx, args_[1], options, &removed);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(removed);

    return Status::OK();
  }

 private:
  uint64_t max_len_ = 0;
  StreamEntryID min_id_;
  StreamTrimStrategy strategy_ = StreamTrimStrategy::None;
};

class CommandXSetId : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    auto s = redis::ParseStreamEntryID(args[2], &last_id_);
    if (!s.IsOK()) return s;

    if (args.size() == 3) {
      return Status::OK();
    }

    for (size_t i = 3; i < args.size(); /* manual increment */) {
      if (util::EqualICase(args[i], "entriesadded") && i + 1 < args.size()) {
        auto parse_result = ParseInt<uint64_t>(args[i + 1]);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        entries_added_ = *parse_result;
        i += 2;
      } else if (util::EqualICase(args[i], "maxdeletedid") && i + 1 < args.size()) {
        StreamEntryID id;
        s = redis::ParseStreamEntryID(args[i + 1], &id);
        if (!s.IsOK()) return s;

        max_deleted_id_ = std::make_optional<StreamEntryID>(id.ms, id.seq);
        i += 2;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Stream stream_db(srv->storage, conn->GetNamespace());

    auto s = stream_db.SetId(ctx, stream_name_, last_id_, entries_added_, max_deleted_id_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID last_id_;
  std::optional<StreamEntryID> max_deleted_id_;
  std::optional<uint64_t> entries_added_;
};

REDIS_REGISTER_COMMANDS(Stream, MakeCmdAttr<CommandXAck>("xack", -4, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandXAdd>("xadd", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandXDel>("xdel", -3, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandXClaim>("xclaim", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandAutoClaim>("xautoclaim", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandXGroup>("xgroup", -4, "write", 2, 2, 1),
                        MakeCmdAttr<CommandXLen>("xlen", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandXInfo>("xinfo", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandXPending>("xpending", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandXRange>("xrange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandXRevRange>("xrevrange", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandXRead>("xread", -4, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandXReadGroup>("xreadgroup", -7, "write", 0, 0, 0),
                        MakeCmdAttr<CommandXTrim>("xtrim", -4, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandXSetId>("xsetid", -3, "write", 1, 1, 1))

}  // namespace redis
