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

#include "redis_stream.h"

#include <rocksdb/status.h>

#include <memory>
#include <utility>
#include <vector>

#include "db_util.h"
#include "time_util.h"

namespace redis {

const char *errSetEntryIdSmallerThanLastGenerated =
    "The ID specified in XSETID is smaller than the target stream top item";
const char *errEntriesAddedSmallerThanStreamSize =
    "The entries_added specified in XSETID is smaller than the target stream length";
const char *errMaxDeletedIdGreaterThanLastGenerated =
    "The ID specified in XSETID is smaller than the provided max_deleted_entry_id";
const char *errEntriesAddedNotSpecifiedForEmptyStream = "an empty stream should have non-zero value of ENTRIESADDED";
const char *errMaxDeletedIdNotSpecifiedForEmptyStream = "an empty stream should have MAXDELETEDID";
const char *errXGroupSubcommandRequiresKeyExist =
    "The XGROUP subcommand requires the key to exist.\
Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.";

rocksdb::Status Stream::GetMetadata(engine::Context &ctx, const Slice &stream_name, StreamMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisStream}, stream_name, metadata);
}

rocksdb::Status Stream::GetLastGeneratedID(engine::Context &ctx, const Slice &stream_name, StreamEntryID *id) {
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    id->ms = 0;
    id->seq = 0;
  } else {
    *id = metadata.last_generated_id;
  }

  return rocksdb::Status::OK();
}

StreamEntryID Stream::entryIDFromInternalKey(const rocksdb::Slice &key) const {
  InternalKey ikey(key, storage_->IsSlotIdEncoded());
  Slice entry_id = ikey.GetSubKey();
  StreamEntryID id;
  GetFixed64(&entry_id, &id.ms);
  GetFixed64(&entry_id, &id.seq);
  return id;
}

std::string Stream::internalKeyFromEntryID(const std::string &ns_key, const StreamMetadata &metadata,
                                           const StreamEntryID &id) const {
  std::string sub_key;
  PutFixed64(&sub_key, id.ms);
  PutFixed64(&sub_key, id.seq);
  std::string entry_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return entry_key;
}

rocksdb::Status Stream::Add(engine::Context &ctx, const Slice &stream_name, const StreamAddOptions &options,
                            const std::vector<std::string> &args, StreamEntryID *id) {
  for (auto const &v : args) {
    if (v.size() > INT32_MAX) {
      return rocksdb::Status::InvalidArgument("argument length is too high");
    }
  }

  std::string entry_value = EncodeStreamEntryValue(args);

  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  if (s.IsNotFound() && options.nomkstream) {
    return s;
  }

  StreamEntryID next_entry_id;
  auto status = options.next_id_strategy->GenerateID(metadata.last_generated_id, &next_entry_id);
  if (!status.IsOK()) return rocksdb::Status::InvalidArgument(status.Msg());

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  bool should_add = true;

  // trim the stream before adding a new entry to provide atomic XADD + XTRIM
  if (options.trim_options.strategy != StreamTrimStrategy::None) {
    StreamTrimOptions trim_options = options.trim_options;
    if (trim_options.strategy == StreamTrimStrategy::MaxLen) {
      // because one entry will be added, we can trim up to (MAXLEN-1) if MAXLEN was specified
      trim_options.max_len = options.trim_options.max_len > 0 ? options.trim_options.max_len - 1 : 0;
    }

    uint64_t delete_cnt = 0;
    s = trim(ctx, ns_key, trim_options, &metadata, batch->GetWriteBatch(), delete_cnt);
    if (!s.ok()) return s;

    if (trim_options.strategy == StreamTrimStrategy::MinID && next_entry_id < trim_options.min_id) {
      // there is no sense to add this element because it would be removed, so just modify metadata and return it's ID
      should_add = false;
    }

    if (trim_options.strategy == StreamTrimStrategy::MaxLen && options.trim_options.max_len == 0) {
      // there is no sense to add this element because it would be removed, so just modify metadata and return it's ID
      should_add = false;
    }
  }

  if (should_add) {
    std::string entry_key = internalKeyFromEntryID(ns_key, metadata, next_entry_id);
    s = batch->Put(stream_cf_handle_, entry_key, entry_value);
    if (!s.ok()) return s;

    metadata.last_generated_id = next_entry_id;
    metadata.last_entry_id = next_entry_id;
    metadata.size += 1;

    if (metadata.size == 1) {
      metadata.first_entry_id = next_entry_id;
      metadata.recorded_first_entry_id = next_entry_id;
    }
  } else {
    metadata.last_generated_id = next_entry_id;
    metadata.max_deleted_entry_id = next_entry_id;
  }

  metadata.entries_added += 1;

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;

  *id = next_entry_id;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

std::string Stream::internalKeyFromGroupName(const std::string &ns_key, const StreamMetadata &metadata,
                                             const std::string &group_name) const {
  std::string sub_key;
  PutFixed64(&sub_key, UINT64_MAX);
  PutFixed8(&sub_key, (uint8_t)StreamSubkeyType::StreamConsumerGroupMetadata);
  PutFixed64(&sub_key, group_name.size());
  sub_key += group_name;
  std::string entry_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return entry_key;
}

std::string Stream::groupNameFromInternalKey(rocksdb::Slice key) const {
  InternalKey ikey(key, storage_->IsSlotIdEncoded());
  Slice group_name_metadata = ikey.GetSubKey();
  uint64_t entry_delimiter = 0;
  GetFixed64(&group_name_metadata, &entry_delimiter);
  uint8_t type_delimiter = 0;
  GetFixed8(&group_name_metadata, &type_delimiter);
  uint64_t len = 0;
  GetFixed64(&group_name_metadata, &len);
  std::string group_name;
  group_name = group_name_metadata.ToString().substr(0, len);
  return group_name;
}

std::string Stream::encodeStreamConsumerGroupMetadataValue(const StreamConsumerGroupMetadata &consumer_group_metadata) {
  std::string dst;
  PutFixed64(&dst, consumer_group_metadata.consumer_number);
  PutFixed64(&dst, consumer_group_metadata.pending_number);
  PutFixed64(&dst, consumer_group_metadata.last_delivered_id.ms);
  PutFixed64(&dst, consumer_group_metadata.last_delivered_id.seq);
  PutFixed64(&dst, static_cast<uint64_t>(consumer_group_metadata.entries_read));
  PutFixed64(&dst, consumer_group_metadata.lag);
  return dst;
}

StreamConsumerGroupMetadata Stream::decodeStreamConsumerGroupMetadataValue(const std::string &value) {
  StreamConsumerGroupMetadata consumer_group_metadata;
  rocksdb::Slice input(value);
  GetFixed64(&input, &consumer_group_metadata.consumer_number);
  GetFixed64(&input, &consumer_group_metadata.pending_number);
  GetFixed64(&input, &consumer_group_metadata.last_delivered_id.ms);
  GetFixed64(&input, &consumer_group_metadata.last_delivered_id.seq);
  uint64_t entries_read = 0;
  GetFixed64(&input, &entries_read);
  consumer_group_metadata.entries_read = static_cast<int64_t>(entries_read);
  GetFixed64(&input, &consumer_group_metadata.lag);
  return consumer_group_metadata;
}

std::string Stream::internalKeyFromConsumerName(const std::string &ns_key, const StreamMetadata &metadata,
                                                const std::string &group_name, const std::string &consumer_name) const {
  std::string sub_key;
  PutFixed64(&sub_key, UINT64_MAX);
  PutFixed8(&sub_key, (uint8_t)StreamSubkeyType::StreamConsumerMetadata);
  PutFixed64(&sub_key, group_name.size());
  sub_key += group_name;
  PutFixed64(&sub_key, consumer_name.size());
  sub_key += consumer_name;
  std::string entry_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return entry_key;
}

std::string Stream::consumerNameFromInternalKey(rocksdb::Slice key) const {
  InternalKey ikey(key, storage_->IsSlotIdEncoded());
  Slice subkey = ikey.GetSubKey();
  uint64_t entry_delimiter = 0;
  GetFixed64(&subkey, &entry_delimiter);
  uint8_t type_delimiter = 0;
  GetFixed8(&subkey, &type_delimiter);
  uint64_t group_name_len = 0;
  GetFixed64(&subkey, &group_name_len);
  subkey.remove_prefix(group_name_len);
  uint64_t consumer_name_len = 0;
  GetFixed64(&subkey, &consumer_name_len);
  return subkey.ToString().substr(0, consumer_name_len);
}

std::string Stream::encodeStreamConsumerMetadataValue(const StreamConsumerMetadata &consumer_metadata) {
  std::string dst;
  PutFixed64(&dst, consumer_metadata.pending_number);
  PutFixed64(&dst, consumer_metadata.last_attempted_interaction_ms);
  PutFixed64(&dst, consumer_metadata.last_successful_interaction_ms);
  return dst;
}

StreamConsumerMetadata Stream::decodeStreamConsumerMetadataValue(const std::string &value) {
  StreamConsumerMetadata consumer_metadata;
  rocksdb::Slice input(value);
  GetFixed64(&input, &consumer_metadata.pending_number);
  GetFixed64(&input, &consumer_metadata.last_attempted_interaction_ms);
  GetFixed64(&input, &consumer_metadata.last_successful_interaction_ms);
  return consumer_metadata;
}

std::string Stream::internalPelKeyFromGroupAndEntryId(const std::string &ns_key, const StreamMetadata &metadata,
                                                      const std::string &group_name, const StreamEntryID &id) {
  std::string sub_key;
  PutFixed64(&sub_key, UINT64_MAX);
  PutFixed8(&sub_key, (uint8_t)StreamSubkeyType::StreamPelEntry);
  PutFixed64(&sub_key, group_name.size());
  sub_key += group_name;
  PutFixed64(&sub_key, id.ms);
  PutFixed64(&sub_key, id.seq);
  std::string entry_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return entry_key;
}

StreamEntryID Stream::groupAndEntryIdFromPelInternalKey(rocksdb::Slice key, std::string &group_name) {
  InternalKey ikey(key, storage_->IsSlotIdEncoded());
  Slice subkey = ikey.GetSubKey();
  uint64_t entry_delimiter = 0;
  GetFixed64(&subkey, &entry_delimiter);
  uint8_t type_delimiter = 0;
  GetFixed8(&subkey, &type_delimiter);
  uint64_t group_name_len = 0;
  GetFixed64(&subkey, &group_name_len);
  group_name = subkey.ToString().substr(0, group_name_len);
  subkey.remove_prefix(group_name_len);
  StreamEntryID entry_id;
  GetFixed64(&subkey, &entry_id.ms);
  GetFixed64(&subkey, &entry_id.seq);
  return entry_id;
}

std::string Stream::encodeStreamPelEntryValue(const StreamPelEntry &pel_entry) {
  std::string dst;
  PutFixed64(&dst, pel_entry.last_delivery_time_ms);
  PutFixed64(&dst, pel_entry.last_delivery_count);
  PutFixed64(&dst, pel_entry.consumer_name.size());
  dst += pel_entry.consumer_name;
  return dst;
}

StreamPelEntry Stream::decodeStreamPelEntryValue(const std::string &value) {
  StreamPelEntry pel_entry;
  rocksdb::Slice input(value);
  GetFixed64(&input, &pel_entry.last_delivery_time_ms);
  GetFixed64(&input, &pel_entry.last_delivery_count);
  uint64_t consumer_name_len = 0;
  GetFixed64(&input, &consumer_name_len);
  pel_entry.consumer_name = input.ToString().substr(0, consumer_name_len);
  return pel_entry;
}

StreamSubkeyType Stream::identifySubkeyType(const rocksdb::Slice &key) const {
  InternalKey ikey(key, storage_->IsSlotIdEncoded());
  Slice subkey = ikey.GetSubKey();
  uint64_t entry_delimiter = 0;
  GetFixed64(&subkey, &entry_delimiter);
  if (entry_delimiter != UINT64_MAX) {
    return StreamSubkeyType::StreamEntry;
  }
  uint8_t type_delimiter = 0;
  GetFixed8(&subkey, &type_delimiter);
  return (StreamSubkeyType)type_delimiter;
}

rocksdb::Status Stream::DeletePelEntries(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                         const std::vector<StreamEntryID> &entry_ids, uint64_t *acknowledged) {
  *acknowledged = 0;

  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  std::string group_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string get_group_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, group_key, &get_group_value);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::map<std::string, uint64_t> consumer_acknowledges;
  for (const auto &id : entry_ids) {
    std::string entry_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, id);
    std::string value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, entry_key, &value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    if (s.ok()) {
      *acknowledged += 1;
      s = batch->Delete(stream_cf_handle_, entry_key);
      if (!s.ok()) return s;

      // increment ack for each related consumer
      auto pel_entry = decodeStreamPelEntryValue(value);
      consumer_acknowledges[pel_entry.consumer_name]++;
    }
  }
  if (*acknowledged > 0) {
    StreamConsumerGroupMetadata group_metadata = decodeStreamConsumerGroupMetadataValue(get_group_value);
    group_metadata.pending_number -= *acknowledged;
    std::string group_value = encodeStreamConsumerGroupMetadataValue(group_metadata);
    s = batch->Put(stream_cf_handle_, group_key, group_value);
    if (!s.ok()) return s;

    for (const auto &[consumer_name, ack_count] : consumer_acknowledges) {
      auto consumer_meta_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer_name);
      std::string consumer_meta_original;
      s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_meta_key, &consumer_meta_original);
      if (!s.ok() && !s.IsNotFound()) {
        return s;
      }
      if (s.ok()) {
        auto consumer_metadata = decodeStreamConsumerMetadataValue(consumer_meta_original);
        consumer_metadata.pending_number -= ack_count;
        s = batch->Put(stream_cf_handle_, consumer_meta_key, encodeStreamConsumerMetadataValue(consumer_metadata));
        if (!s.ok()) return s;
      }
    }
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::ClaimPelEntries(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                        const std::string &consumer_name, const uint64_t min_idle_time_ms,
                                        const std::vector<StreamEntryID> &entry_ids, const StreamClaimOptions &options,
                                        StreamClaimResult *result) {
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::string group_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string get_group_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, group_key, &get_group_value);
  if (!s.ok()) return s;

  StreamConsumerGroupMetadata group_metadata = decodeStreamConsumerGroupMetadataValue(get_group_value);
  std::string consumer_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer_name);
  std::string get_consumer_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    int created_number = 0;
    s = createConsumerWithoutLock(ctx, stream_name, group_name, consumer_name, &created_number);
    if (!s.ok()) {
      return s;
    }
    group_metadata.consumer_number += created_number;
  }
  StreamConsumerMetadata consumer_metadata;
  if (!s.IsNotFound()) {
    consumer_metadata = decodeStreamConsumerMetadataValue(get_consumer_value);
  }
  auto now = util::GetTimeStampMS();
  consumer_metadata.last_attempted_interaction_ms = now;
  consumer_metadata.last_successful_interaction_ms = now;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  for (const auto &id : entry_ids) {
    std::string raw_value;
    rocksdb::Status s = getEntryRawValue(ctx, ns_key, metadata, id, &raw_value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    if (s.IsNotFound()) continue;

    std::string entry_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, id);
    std::string value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, entry_key, &value);
    StreamPelEntry pel_entry;

    if (!s.ok() && s.IsNotFound() && options.force) {
      pel_entry = {0, 0, ""};
      group_metadata.pending_number += 1;
    }

    if (s.ok()) {
      pel_entry = decodeStreamPelEntryValue(value);
    }

    if (s.ok() || (s.IsNotFound() && options.force)) {
      if (now - pel_entry.last_delivery_time_ms < min_idle_time_ms) continue;

      std::vector<std::string> values;
      if (options.just_id) {
        result->ids.emplace_back(id.ToString());
      } else {
        auto rv = DecodeRawStreamEntryValue(raw_value, &values);
        if (!rv.IsOK()) {
          return rocksdb::Status::InvalidArgument(rv.Msg());
        }
        result->entries.emplace_back(id.ToString(), std::move(values));
      }

      if (pel_entry.consumer_name != "") {
        std::string original_consumer_key =
            internalKeyFromConsumerName(ns_key, metadata, group_name, pel_entry.consumer_name);
        std::string get_original_consumer_value;
        s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, original_consumer_key,
                          &get_original_consumer_value);
        if (!s.ok()) {
          return s;
        }
        StreamConsumerMetadata original_consumer_metadata =
            decodeStreamConsumerMetadataValue(get_original_consumer_value);
        original_consumer_metadata.pending_number -= 1;
        s = batch->Put(stream_cf_handle_, original_consumer_key,
                       encodeStreamConsumerMetadataValue(original_consumer_metadata));
        if (!s.ok()) return s;
      }

      pel_entry.consumer_name = consumer_name;
      consumer_metadata.pending_number += 1;
      if (options.with_time) {
        pel_entry.last_delivery_time_ms = options.last_delivery_time_ms;
      } else {
        pel_entry.last_delivery_time_ms = now - options.idle_time_ms;
      }

      if (pel_entry.last_delivery_time_ms < 0 || pel_entry.last_delivery_time_ms > now) {
        pel_entry.last_delivery_time_ms = now;
      }

      if (options.with_retry_count) {
        pel_entry.last_delivery_count = options.last_delivery_count;
      } else if (!options.just_id) {
        pel_entry.last_delivery_count += 1;
      }

      std::string pel_value = encodeStreamPelEntryValue(pel_entry);
      s = batch->Put(stream_cf_handle_, entry_key, pel_value);
      if (!s.ok()) return s;
    }
  }

  if (options.with_last_id && options.last_delivered_id > group_metadata.last_delivered_id) {
    group_metadata.last_delivered_id = options.last_delivered_id;
  }

  s = batch->Put(stream_cf_handle_, consumer_key, encodeStreamConsumerMetadataValue(consumer_metadata));
  if (!s.ok()) return s;
  s = batch->Put(stream_cf_handle_, group_key, encodeStreamConsumerGroupMetadataValue(group_metadata));
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::AutoClaim(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                  const std::string &consumer_name, const StreamAutoClaimOptions &options,
                                  StreamAutoClaimResult *result) {
  if (options.exclude_start && options.start_id.IsMaximum()) {
    return rocksdb::Status::InvalidArgument("invalid start ID for the interval");
  }

  std::string ns_key = AppendNamespacePrefix(stream_name);
  StreamMetadata metadata(false);

  auto s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {  // not found will be caught by outside with no such key or consumer group
    return s;
  }

  std::string consumer_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer_name);
  std::string get_consumer_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    int created_number = 0;
    s = createConsumerWithoutLock(ctx, stream_name, group_name, consumer_name, &created_number);
    if (!s.ok()) {
      return s;
    }
    s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
    if (!s.ok()) {
      return s;
    }
  }

  StreamConsumerMetadata current_consumer_metadata = decodeStreamConsumerMetadataValue(get_consumer_value);
  std::map<std::string, uint64_t> claimed_consumer_entity_count;
  std::string prefix_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, options.start_id);
  std::string end_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, StreamEntryID::Maximum());

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice lower_bound(prefix_key);
  rocksdb::Slice upper_bound(end_key);
  read_options.iterate_lower_bound = &lower_bound;
  read_options.iterate_upper_bound = &upper_bound;

  auto count = options.count;
  uint64_t attempts = options.attempts_factors * count;
  auto now_ms = util::GetTimeStampMS();
  std::vector<StreamEntryID> deleted_entries;
  std::vector<StreamEntry> pending_entries;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  uint64_t total_claimed_count = 0;
  for (iter->SeekToFirst(); iter->Valid() && count > 0 && attempts > 0; iter->Next()) {
    std::string tmp_group_name;
    StreamEntryID entry_id = groupAndEntryIdFromPelInternalKey(iter->key(), tmp_group_name);

    if (options.exclude_start && entry_id == options.start_id) {
      continue;
    }

    attempts--;

    StreamPelEntry penl_entry = decodeStreamPelEntryValue(iter->value().ToString());
    if ((now_ms - penl_entry.last_delivery_time_ms) < options.min_idle_time_ms) {
      continue;
    }

    auto entry_key = internalKeyFromEntryID(ns_key, metadata, entry_id);
    std::string entry_value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, entry_key, &entry_value);
    if (!s.ok()) {
      if (s.IsNotFound()) {
        deleted_entries.push_back(entry_id);
        s = batch->Delete(stream_cf_handle_, iter->key());
        if (!s.ok()) return s;
        --count;
        continue;
      }
      return s;
    }

    StreamEntry entry(entry_id.ToString(), {});
    if (!options.just_id) {
      auto rv_status = DecodeRawStreamEntryValue(entry_value, &entry.values);
      if (!rv_status.OK()) {
        return rocksdb::Status::InvalidArgument(rv_status.Msg());
      }
    }

    pending_entries.emplace_back(std::move(entry));
    --count;

    if (penl_entry.consumer_name != consumer_name) {
      ++total_claimed_count;
      claimed_consumer_entity_count[penl_entry.consumer_name] += 1;
      penl_entry.consumer_name = consumer_name;
      penl_entry.last_delivery_time_ms = now_ms;
      // Increment the delivery attempts counter unless JUSTID option provided
      if (!options.just_id) {
        penl_entry.last_delivery_count += 1;
      }
      s = batch->Put(stream_cf_handle_, iter->key(), encodeStreamPelEntryValue(penl_entry));
      if (!s.ok()) return s;
    }
  }

  if (total_claimed_count > 0 && !pending_entries.empty()) {
    current_consumer_metadata.pending_number += total_claimed_count;
    current_consumer_metadata.last_attempted_interaction_ms = now_ms;

    s = batch->Put(stream_cf_handle_, consumer_key, encodeStreamConsumerMetadataValue(current_consumer_metadata));
    if (!s.ok()) return s;

    for (const auto &[consumer, count] : claimed_consumer_entity_count) {
      std::string tmp_consumer_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer);
      std::string tmp_consumer_value;
      s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, tmp_consumer_key, &tmp_consumer_value);
      if (!s.ok()) {
        return s;
      }
      StreamConsumerMetadata tmp_consumer_metadata = decodeStreamConsumerMetadataValue(tmp_consumer_value);
      tmp_consumer_metadata.pending_number -= count;
      s = batch->Put(stream_cf_handle_, tmp_consumer_key, encodeStreamConsumerMetadataValue(tmp_consumer_metadata));
      if (!s.ok()) return s;
    }
  }

  bool has_next_entry = false;
  for (; iter->Valid(); iter->Next()) {
    has_next_entry = true;
    break;
  }

  if (auto s = iter->status(); !s.ok()) {
    return s;
  }

  if (has_next_entry) {
    std::string tmp_group_name;
    StreamEntryID entry_id = groupAndEntryIdFromPelInternalKey(iter->key(), tmp_group_name);
    result->next_claim_id = entry_id.ToString();
  } else {
    result->next_claim_id = StreamEntryID::Minimum().ToString();
  }

  result->entries = std::move(pending_entries);
  result->deleted_ids.clear();
  result->deleted_ids.reserve(deleted_entries.size());
  std::transform(deleted_entries.cbegin(), deleted_entries.cend(), std::back_inserter(result->deleted_ids),
                 [](const StreamEntryID &id) { return id.ToString(); });

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}
rocksdb::Status Stream::CreateGroup(engine::Context &ctx, const Slice &stream_name,
                                    const StreamXGroupCreateOptions &options, const std::string &group_name) {
  if (std::isdigit(group_name[0])) {
    return rocksdb::Status::InvalidArgument("group name cannot start with number");
  }
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound() && !options.mkstream) {
    return rocksdb::Status::InvalidArgument(errXGroupSubcommandRequiresKeyExist);
  }

  StreamConsumerGroupMetadata consumer_group_metadata;
  if (options.last_id == "$") {
    consumer_group_metadata.last_delivered_id = metadata.last_entry_id;
  } else {
    auto s = ParseStreamEntryID(options.last_id, &consumer_group_metadata.last_delivered_id);
    if (!s.IsOK()) {
      return rocksdb::Status::InvalidArgument(s.Msg());
    }
  }
  consumer_group_metadata.entries_read = options.entries_read;
  std::string entry_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string entry_value = encodeStreamConsumerGroupMetadataValue(consumer_group_metadata);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string get_entry_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, entry_key, &get_entry_value);
  if (!s.IsNotFound()) {
    if (!s.ok()) {
      return s;
    }
    return rocksdb::Status::Busy();
  }

  s = batch->Put(stream_cf_handle_, entry_key, entry_value);
  if (!s.ok()) return s;
  metadata.group_number += 1;
  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::DestroyGroup(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                     uint64_t *delete_cnt) {
  *delete_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument(errXGroupSubcommandRequiresKeyExist);
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string sub_key_prefix;
  PutFixed64(&sub_key_prefix, group_name.size());
  sub_key_prefix += group_name;
  std::string next_version_prefix_key =
      InternalKey(ns_key, sub_key_prefix, metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, sub_key_prefix, metadata.version, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    s = batch->Delete(stream_cf_handle_, iter->key());
    if (!s.ok()) return s;
    *delete_cnt += 1;
  }

  if (auto s = iter->status(); !s.ok()) {
    return s;
  }

  if (*delete_cnt != 0) {
    metadata.group_number -= 1;
    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::createConsumerWithoutLock(engine::Context &ctx, const Slice &stream_name,
                                                  const std::string &group_name, const std::string &consumer_name,
                                                  int *created_number) {
  if (std::isdigit(consumer_name[0])) {
    return rocksdb::Status::InvalidArgument("consumer name cannot start with number");
  }
  std::string ns_key = AppendNamespacePrefix(stream_name);
  StreamMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument(errXGroupSubcommandRequiresKeyExist);
  }

  std::string entry_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string get_entry_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, entry_key, &get_entry_value);
  if (!s.ok()) return s;

  StreamConsumerMetadata consumer_metadata;
  auto now = util::GetTimeStampMS();
  consumer_metadata.last_attempted_interaction_ms = now;
  consumer_metadata.last_successful_interaction_ms = now;
  std::string consumer_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer_name);
  std::string consumer_value = encodeStreamConsumerMetadataValue(consumer_metadata);
  std::string get_consumer_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
  if (!s.IsNotFound()) {
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  s = batch->Put(stream_cf_handle_, consumer_key, consumer_value);
  if (!s.ok()) return s;
  StreamConsumerGroupMetadata consumer_group_metadata = decodeStreamConsumerGroupMetadataValue(get_entry_value);
  consumer_group_metadata.consumer_number += 1;
  std::string consumer_group_metadata_bytes = encodeStreamConsumerGroupMetadataValue(consumer_group_metadata);
  s = batch->Put(stream_cf_handle_, entry_key, consumer_group_metadata_bytes);
  if (!s.ok()) return s;
  s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  if (s.ok()) *created_number = 1;
  return s;
}

rocksdb::Status Stream::CreateConsumer(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                       const std::string &consumer_name, int *created_number) {
  std::string ns_key = AppendNamespacePrefix(stream_name);

  return createConsumerWithoutLock(ctx, stream_name, group_name, consumer_name, created_number);
}

rocksdb::Status Stream::DestroyConsumer(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                        const std::string &consumer_name, uint64_t &deleted_pel) {
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument(errXGroupSubcommandRequiresKeyExist);
  }

  std::string group_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string get_group_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, group_key, &get_group_value);
  if (!s.ok()) return s;

  std::string consumer_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer_name);
  std::string get_consumer_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    return rocksdb::Status::OK();
  }

  StreamConsumerMetadata consumer_metadata = decodeStreamConsumerMetadataValue(get_consumer_value);
  deleted_pel = consumer_metadata.pending_number;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string prefix_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, StreamEntryID::Minimum());
  std::string end_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, StreamEntryID::Maximum());

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(end_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    StreamPelEntry pel_entry = decodeStreamPelEntryValue(iter->value().ToString());
    if (pel_entry.consumer_name == consumer_name) {
      s = batch->Delete(stream_cf_handle_, iter->key());
      if (!s.ok()) return s;
    }
  }

  if (auto s = iter->status(); !s.ok()) {
    return s;
  }

  s = batch->Delete(stream_cf_handle_, consumer_key);
  if (!s.ok()) return s;
  StreamConsumerGroupMetadata group_metadata = decodeStreamConsumerGroupMetadataValue(get_group_value);
  group_metadata.consumer_number -= 1;
  group_metadata.pending_number -= deleted_pel;
  s = batch->Put(stream_cf_handle_, group_key, encodeStreamConsumerGroupMetadataValue(group_metadata));
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::GroupSetId(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                   const StreamXGroupCreateOptions &options) {
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument(errXGroupSubcommandRequiresKeyExist);
  }

  std::string entry_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string get_entry_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, entry_key, &get_entry_value);
  if (!s.ok()) return s;

  StreamConsumerGroupMetadata consumer_group_metadata = decodeStreamConsumerGroupMetadataValue(get_entry_value);
  if (options.last_id == "$") {
    consumer_group_metadata.last_delivered_id = metadata.last_entry_id;
  } else {
    auto s = ParseStreamEntryID(options.last_id, &consumer_group_metadata.last_delivered_id);
    if (!s.IsOK()) {
      return rocksdb::Status::InvalidArgument(s.Msg());
    }
  }
  consumer_group_metadata.entries_read = options.entries_read;
  std::string entry_value = encodeStreamConsumerGroupMetadataValue(consumer_group_metadata);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  s = batch->Put(stream_cf_handle_, entry_key, entry_value);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::DeleteEntries(engine::Context &ctx, const Slice &stream_name,
                                      const std::vector<StreamEntryID> &ids, uint64_t *deleted_cnt) {
  *deleted_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);

  for (const auto &id : ids) {
    std::string entry_key = internalKeyFromEntryID(ns_key, metadata, id);
    std::string value;
    s = storage_->Get(ctx, read_options, stream_cf_handle_, entry_key, &value);
    if (s.ok()) {
      *deleted_cnt += 1;
      s = batch->Delete(stream_cf_handle_, entry_key);
      if (!s.ok()) return s;

      if (metadata.max_deleted_entry_id < id) {
        metadata.max_deleted_entry_id = id;
      }

      if (*deleted_cnt == metadata.size) {
        metadata.first_entry_id.Clear();
        metadata.last_entry_id.Clear();
        metadata.recorded_first_entry_id.Clear();
        break;
      }

      if (id == metadata.first_entry_id) {
        iter->Seek(entry_key);
        iter->Next();
        if (iter->Valid()) {
          metadata.first_entry_id = entryIDFromInternalKey(iter->key());
          metadata.recorded_first_entry_id = metadata.first_entry_id;
        } else {
          metadata.first_entry_id.Clear();
          metadata.recorded_first_entry_id.Clear();
        }
      }

      if (id == metadata.last_entry_id) {
        iter->Seek(entry_key);
        iter->Prev();
        if (iter->Valid()) {
          metadata.last_entry_id = entryIDFromInternalKey(iter->key());
        } else {
          metadata.last_entry_id.Clear();
        }
      }
    }
  }

  if (*deleted_cnt > 0) {
    metadata.size -= *deleted_cnt;

    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

// If `options` is StreamLenOptions{} the function just returns the number of entries in the stream.
// Additionally, if a specific entry ID is provided via `StreamLenOptions::entry_id`,
// the function starts counting entries from that ID. With only entry ID specified, the function counts elements
// between that ID and the last element in the stream.
// If `StreamLenOptions::to_first` is set to true, the function will count elements
// between specified ID and the first element in the stream.
// The entry with the ID `StreamLenOptions::entry_id` has not taken into account (it serves as exclusive boundary).
rocksdb::Status Stream::Len(engine::Context &ctx, const Slice &stream_name, const StreamLenOptions &options,
                            uint64_t *size) {
  *size = 0;
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  if (!options.with_entry_id) {
    *size = metadata.size;
    return rocksdb::Status::OK();
  }

  if (options.entry_id > metadata.last_entry_id) {
    *size = options.to_first ? metadata.size : 0;
    return rocksdb::Status::OK();
  }

  if (options.entry_id < metadata.first_entry_id) {
    *size = options.to_first ? 0 : metadata.size;
    return rocksdb::Status::OK();
  }

  if ((!options.to_first && options.entry_id == metadata.first_entry_id) ||
      (options.to_first && options.entry_id == metadata.last_entry_id)) {
    *size = metadata.size - 1;
    return rocksdb::Status::OK();
  }

  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  std::string start_key = internalKeyFromEntryID(ns_key, metadata, options.entry_id);

  iter->Seek(start_key);
  if (!iter->Valid()) {
    return rocksdb::Status::OK();
  }

  if (options.to_first) {
    iter->Prev();
  } else if (iter->key().ToString() == start_key) {
    iter->Next();
  }

  for (; iter->Valid(); options.to_first ? iter->Prev() : iter->Next()) {
    if (identifySubkeyType(iter->key()) == StreamSubkeyType::StreamEntry) {
      *size += 1;
    }
  }

  if (auto s = iter->status(); !s.ok()) {
    return s;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Stream::range(engine::Context &ctx, const std::string &ns_key, const StreamMetadata &metadata,
                              const StreamRangeOptions &options, std::vector<StreamEntry> *entries) const {
  std::string start_key = internalKeyFromEntryID(ns_key, metadata, options.start);
  std::string end_key = internalKeyFromEntryID(ns_key, metadata, options.end);

  if (start_key == end_key) {
    if (options.exclude_start || options.exclude_end) {
      return rocksdb::Status::OK();
    }

    std::string entry_value;
    auto s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, start_key, &entry_value);
    if (!s.ok()) {
      return s.IsNotFound() ? rocksdb::Status::OK() : s;
    }

    std::vector<std::string> values;
    auto rv = DecodeRawStreamEntryValue(entry_value, &values);
    if (!rv.IsOK()) {
      return rocksdb::Status::InvalidArgument(rv.Msg());
    }

    entries->emplace_back(options.start.ToString(), std::move(values));
    return rocksdb::Status::OK();
  }

  if ((!options.reverse && options.end < options.start) || (options.reverse && options.start < options.end)) {
    return rocksdb::Status::OK();
  }

  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  iter->Seek(start_key);
  if (options.reverse && (!iter->Valid() || iter->key().ToString() != start_key)) {
    iter->SeekForPrev(start_key);
  }

  for (; iter->Valid() && (options.reverse ? iter->key().ToString() >= end_key : iter->key().ToString() <= end_key);
       options.reverse ? iter->Prev() : iter->Next()) {
    if (identifySubkeyType(iter->key()) != StreamSubkeyType::StreamEntry) {
      continue;
    }
    if (options.exclude_start && iter->key().ToString() == start_key) {
      continue;
    }

    if (options.exclude_end && iter->key().ToString() == end_key) {
      break;
    }

    std::vector<std::string> values;
    auto rv = DecodeRawStreamEntryValue(iter->value().ToString(), &values);
    if (!rv.IsOK()) {
      return rocksdb::Status::InvalidArgument(rv.Msg());
    }

    entries->emplace_back(entryIDFromInternalKey(iter->key()).ToString(), std::move(values));

    if (options.with_count && entries->size() == options.count) {
      break;
    }
  }

  if (auto s = iter->status(); !s.ok()) {
    return s;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Stream::getEntryRawValue(engine::Context &ctx, const std::string &ns_key,
                                         const StreamMetadata &metadata, const StreamEntryID &id,
                                         std::string *value) const {
  std::string entry_key = internalKeyFromEntryID(ns_key, metadata, id);
  return storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, entry_key, value);
}

rocksdb::Status Stream::GetStreamInfo(engine::Context &ctx, const rocksdb::Slice &stream_name, bool full,
                                      uint64_t count, StreamInfo *info) {
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  info->size = metadata.size;
  info->entries_added = metadata.entries_added;
  info->last_generated_id = metadata.last_generated_id;
  info->max_deleted_entry_id = metadata.max_deleted_entry_id;
  info->recorded_first_entry_id = metadata.recorded_first_entry_id;

  if (metadata.size == 0) {
    return rocksdb::Status::OK();
  }

  if (full) {
    uint64_t need_entries = metadata.size;
    if (count != 0 && count < metadata.size) {
      need_entries = count;
    }

    info->entries.reserve(need_entries);

    StreamRangeOptions options;
    options.start = metadata.first_entry_id;
    options.end = metadata.last_entry_id;
    options.with_count = true;
    options.count = need_entries;
    options.reverse = false;
    options.exclude_start = false;
    options.exclude_end = false;

    s = range(ctx, ns_key, metadata, options, &info->entries);
    if (!s.ok()) {
      return s;
    }
  } else {
    std::string first_value;
    s = getEntryRawValue(ctx, ns_key, metadata, metadata.first_entry_id, &first_value);
    if (!s.ok()) {
      return s;
    }

    std::vector<std::string> values;
    auto rv = DecodeRawStreamEntryValue(first_value, &values);
    if (!rv.IsOK()) {
      return rocksdb::Status::InvalidArgument(rv.Msg());
    }

    info->first_entry = std::make_unique<StreamEntry>(metadata.first_entry_id.ToString(), std::move(values));

    std::string last_value;
    s = getEntryRawValue(ctx, ns_key, metadata, metadata.last_entry_id, &last_value);
    if (!s.ok()) {
      return s;
    }

    rv = DecodeRawStreamEntryValue(last_value, &values);
    if (!rv.IsOK()) {
      return rocksdb::Status::InvalidArgument(rv.Msg());
    }

    info->last_entry = std::make_unique<StreamEntry>(metadata.last_entry_id.ToString(), std::move(values));
  }

  return rocksdb::Status::OK();
}

static bool StreamRangeHasTombstones(const StreamMetadata &metadata, StreamEntryID start_id) {
  StreamEntryID end_id = StreamEntryID::Maximum();
  if (metadata.size == 0 || metadata.max_deleted_entry_id == StreamEntryID{0, 0}) {
    return false;
  }
  if (metadata.first_entry_id > metadata.max_deleted_entry_id) {
    return false;
  }
  return (start_id <= metadata.max_deleted_entry_id && metadata.max_deleted_entry_id <= end_id);
}

static int64_t StreamEstimateDistanceFromFirstEverEntry(const StreamMetadata &metadata, StreamEntryID id) {
  if (metadata.entries_added == 0) {
    return 0;
  }
  if (metadata.size == 0 && id < metadata.last_entry_id) {
    return static_cast<int64_t>(metadata.entries_added);
  }
  if (id == metadata.last_entry_id) {
    return static_cast<int64_t>(metadata.entries_added);
  } else if (id > metadata.last_entry_id) {
    return -1;
  }
  if (metadata.max_deleted_entry_id == StreamEntryID{0, 0} || metadata.max_deleted_entry_id < metadata.first_entry_id) {
    if (id < metadata.first_entry_id) {
      return static_cast<int64_t>(metadata.entries_added - metadata.size);
    } else if (id == metadata.first_entry_id) {
      return static_cast<int64_t>(metadata.entries_added - metadata.size + 1);
    }
  }
  return -1;
}

static void CheckLagValid(const StreamMetadata &stream_metadata, StreamConsumerGroupMetadata &group_metadata) {
  bool valid = false;
  if (stream_metadata.entries_added == 0) {
    group_metadata.lag = 0;
    valid = true;
  } else if (group_metadata.entries_read != -1 &&
             !StreamRangeHasTombstones(stream_metadata, group_metadata.last_delivered_id)) {
    group_metadata.lag = stream_metadata.entries_added - group_metadata.entries_read;
    valid = true;
  } else {
    int64_t entries_read = StreamEstimateDistanceFromFirstEverEntry(stream_metadata, group_metadata.last_delivered_id);
    if (entries_read != -1) {
      group_metadata.lag = stream_metadata.entries_added - entries_read;
      valid = true;
    }
  }
  if (!valid) {
    group_metadata.lag = UINT64_MAX;
  }
}

rocksdb::Status Stream::GetGroupInfo(engine::Context &ctx, const Slice &stream_name,
                                     std::vector<std::pair<std::string, StreamConsumerGroupMetadata>> &group_metadata) {
  std::string ns_key = AppendNamespacePrefix(stream_name);
  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::string subkey_type_delimiter;
  PutFixed64(&subkey_type_delimiter, UINT64_MAX);
  PutFixed8(&subkey_type_delimiter, (uint8_t)StreamSubkeyType::StreamConsumerGroupMetadata);
  std::string next_version_prefix_key =
      InternalKey(ns_key, subkey_type_delimiter, metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key =
      InternalKey(ns_key, subkey_type_delimiter, metadata.version, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (identifySubkeyType(iter->key()) != StreamSubkeyType::StreamConsumerGroupMetadata) {
      continue;
    }

    std::string group_name = groupNameFromInternalKey(iter->key());
    StreamConsumerGroupMetadata cg_metadata = decodeStreamConsumerGroupMetadataValue(iter->value().ToString());
    CheckLagValid(metadata, cg_metadata);
    std::pair<std::string, StreamConsumerGroupMetadata> tmp_item(group_name, cg_metadata);
    group_metadata.push_back(tmp_item);
  }
  return iter->status();
}

rocksdb::Status Stream::GetConsumerInfo(
    engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
    std::vector<std::pair<std::string, StreamConsumerMetadata>> &consumer_metadata) {
  std::string ns_key = AppendNamespacePrefix(stream_name);
  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::string subkey_type_delimiter;
  PutFixed64(&subkey_type_delimiter, UINT64_MAX);
  PutFixed8(&subkey_type_delimiter, (uint8_t)StreamSubkeyType::StreamConsumerMetadata);
  PutFixed64(&subkey_type_delimiter, group_name.size());
  subkey_type_delimiter += group_name;
  std::string next_version_prefix_key =
      InternalKey(ns_key, subkey_type_delimiter, metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key =
      InternalKey(ns_key, subkey_type_delimiter, metadata.version, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (identifySubkeyType(iter->key()) != StreamSubkeyType::StreamConsumerMetadata) {
      continue;
    }

    std::string consumer_name = consumerNameFromInternalKey(iter->key());
    StreamConsumerMetadata c_metadata = decodeStreamConsumerMetadataValue(iter->value().ToString());
    std::pair<std::string, StreamConsumerMetadata> tmp_item(consumer_name, c_metadata);
    consumer_metadata.push_back(tmp_item);
  }
  return iter->status();
}

rocksdb::Status Stream::Range(engine::Context &ctx, const Slice &stream_name, const StreamRangeOptions &options,
                              std::vector<StreamEntry> *entries) {
  entries->clear();

  if (options.with_count && options.count == 0) {
    return rocksdb::Status::OK();
  }

  if (options.exclude_start && options.start.IsMaximum()) {
    return rocksdb::Status::InvalidArgument("invalid start ID for the interval");
  }

  if (options.exclude_end && options.end.IsMinimum()) {
    return rocksdb::Status::InvalidArgument("invalid end ID for the interval");
  }

  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  return range(ctx, ns_key, metadata, options, entries);
}

rocksdb::Status Stream::RangeWithPending(engine::Context &ctx, const Slice &stream_name, StreamRangeOptions &options,
                                         std::vector<StreamEntry> *entries, std::string &group_name,
                                         std::string &consumer_name, bool noack, bool latest) {
  entries->clear();

  if (options.with_count && options.count == 0) {
    return rocksdb::Status::OK();
  }

  if (options.exclude_start && options.start.IsMaximum()) {
    return rocksdb::Status::InvalidArgument("invalid start ID for the interval");
  }

  if (options.exclude_end && options.end.IsMinimum()) {
    return rocksdb::Status::InvalidArgument("invalid end ID for the interval");
  }

  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  std::string group_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string get_group_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, group_key, &get_group_value);
  if (!s.ok()) return s;

  std::string consumer_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer_name);
  std::string get_consumer_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  if (s.IsNotFound()) {
    int created_number = 0;
    s = createConsumerWithoutLock(ctx, stream_name, group_name, consumer_name, &created_number);
    if (!s.ok()) {
      return s;
    }
    s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, group_key, &get_group_value);
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  StreamConsumerGroupMetadata consumergroup_metadata = decodeStreamConsumerGroupMetadataValue(get_group_value);
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  StreamConsumerMetadata consumer_metadata = decodeStreamConsumerMetadataValue(get_consumer_value);
  auto now_ms = util::GetTimeStampMS();
  consumer_metadata.last_attempted_interaction_ms = now_ms;
  consumer_metadata.last_successful_interaction_ms = now_ms;

  if (latest) {
    options.start = consumergroup_metadata.last_delivered_id;
    s = range(ctx, ns_key, metadata, options, entries);
    if (!s.ok()) {
      return s;
    }
    StreamEntryID maxid = {0, 0};
    for (const auto &entry : *entries) {
      StreamEntryID id;
      Status st = ParseStreamEntryID(entry.key, &id);
      if (!st.IsOK()) {
        return rocksdb::Status::InvalidArgument(st.Msg());
      }
      if (id > maxid) {
        maxid = id;
      }
      if (!noack) {
        std::string pel_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, id);
        StreamPelEntry pel_entry = {0, 0, consumer_name};
        std::string pel_value = encodeStreamPelEntryValue(pel_entry);
        s = batch->Put(stream_cf_handle_, pel_key, pel_value);
        if (!s.ok()) return s;
        consumergroup_metadata.entries_read += 1;
        consumergroup_metadata.pending_number += 1;
        consumer_metadata.pending_number += 1;
      }
    }
    if (maxid > consumergroup_metadata.last_delivered_id) {
      consumergroup_metadata.last_delivered_id = maxid;
    }
  } else {
    std::string prefix_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, options.start);
    std::string end_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, StreamEntryID::Maximum());

    rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
    rocksdb::Slice upper_bound(end_key);
    read_options.iterate_upper_bound = &upper_bound;
    rocksdb::Slice lower_bound(prefix_key);
    read_options.iterate_lower_bound = &lower_bound;

    auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
    uint64_t count = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      std::string tmp_group_name;
      StreamEntryID entry_id = groupAndEntryIdFromPelInternalKey(iter->key(), tmp_group_name);
      StreamPelEntry pel_entry = decodeStreamPelEntryValue(iter->value().ToString());
      if (pel_entry.consumer_name != consumer_name) continue;
      std::string raw_value;
      rocksdb::Status st = getEntryRawValue(ctx, ns_key, metadata, entry_id, &raw_value);
      if (!st.ok() && !st.IsNotFound()) {
        return st;
      }
      std::vector<std::string> values;
      auto rv = DecodeRawStreamEntryValue(raw_value, &values);
      if (!rv.IsOK()) {
        return rocksdb::Status::InvalidArgument(rv.Msg());
      }
      entries->emplace_back(entry_id.ToString(), std::move(values));
      pel_entry.last_delivery_count += 1;
      pel_entry.last_delivery_time_ms = now_ms;
      s = batch->Put(stream_cf_handle_, iter->key(), encodeStreamPelEntryValue(pel_entry));
      if (!s.ok()) return s;
      ++count;
      if (count >= options.count) break;
    }

    if (auto s = iter->status(); !s.ok()) {
      return s;
    }
  }
  s = batch->Put(stream_cf_handle_, group_key, encodeStreamConsumerGroupMetadataValue(consumergroup_metadata));
  if (!s.ok()) return s;
  s = batch->Put(stream_cf_handle_, consumer_key, encodeStreamConsumerMetadataValue(consumer_metadata));
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::Trim(engine::Context &ctx, const Slice &stream_name, const StreamTrimOptions &options,
                             uint64_t *delete_cnt) {
  *delete_cnt = 0;

  if (options.strategy == StreamTrimStrategy::None) {
    return rocksdb::Status::OK();
  }

  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  s = trim(ctx, ns_key, options, &metadata, batch->GetWriteBatch(), *delete_cnt);
  if (!s.ok()) {
    return s;
  }

  if (*delete_cnt > 0) {
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;

    return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Stream::trim(engine::Context &ctx, const std::string &ns_key, const StreamTrimOptions &options,
                             StreamMetadata *metadata, rocksdb::WriteBatch *batch, uint64_t &delete_cnt) {
  if (metadata->size == 0) {
    delete_cnt = 0;
    return rocksdb::Status::OK();
  }

  if (options.strategy == StreamTrimStrategy::MaxLen && metadata->size <= options.max_len) {
    delete_cnt = 0;
    return rocksdb::Status::OK();
  }

  if (options.strategy == StreamTrimStrategy::MinID && metadata->first_entry_id >= options.min_id) {
    delete_cnt = 0;
    return rocksdb::Status::OK();
  }

  delete_cnt = 0;

  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata->version + 1, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata->version, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  std::string start_key = internalKeyFromEntryID(ns_key, *metadata, metadata->first_entry_id);
  iter->Seek(start_key);

  std::string last_deleted;
  while (iter->Valid() && metadata->size > 0) {
    if (options.strategy == StreamTrimStrategy::MaxLen && metadata->size <= options.max_len) {
      break;
    }

    if (options.strategy == StreamTrimStrategy::MinID && metadata->first_entry_id >= options.min_id) {
      break;
    }

    auto s = batch->Delete(stream_cf_handle_, iter->key());
    if (!s.ok()) {
      return s;
    }

    delete_cnt += 1;
    metadata->size -= 1;
    last_deleted = iter->key().ToString();

    iter->Next();

    if (iter->Valid()) {
      metadata->first_entry_id = entryIDFromInternalKey(iter->key());
      metadata->recorded_first_entry_id = metadata->first_entry_id;
    } else {
      metadata->first_entry_id.Clear();
      metadata->recorded_first_entry_id.Clear();
    }
  }

  if (metadata->size == 0) {
    metadata->first_entry_id.Clear();
    metadata->last_entry_id.Clear();
    metadata->recorded_first_entry_id.Clear();
  }

  if (delete_cnt > 0) {
    metadata->max_deleted_entry_id = entryIDFromInternalKey(last_deleted);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Stream::SetId(engine::Context &ctx, const Slice &stream_name, const StreamEntryID &last_generated_id,
                              std::optional<uint64_t> entries_added, std::optional<StreamEntryID> max_deleted_id) {
  if (max_deleted_id && last_generated_id < max_deleted_id) {
    return rocksdb::Status::InvalidArgument(errMaxDeletedIdGreaterThanLastGenerated);
  }

  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  if (s.IsNotFound()) {
    if (!entries_added || entries_added == 0) {
      return rocksdb::Status::InvalidArgument(errEntriesAddedNotSpecifiedForEmptyStream);
    }

    if (!max_deleted_id || (max_deleted_id->ms == 0 && max_deleted_id->seq == 0)) {
      return rocksdb::Status::InvalidArgument(errMaxDeletedIdNotSpecifiedForEmptyStream);
    }

    // create an empty stream
    metadata = StreamMetadata();
  }

  if (metadata.size > 0 && last_generated_id < metadata.last_generated_id) {
    return rocksdb::Status::InvalidArgument(errSetEntryIdSmallerThanLastGenerated);
  }

  if (metadata.size > 0 && entries_added && entries_added < metadata.size) {
    return rocksdb::Status::InvalidArgument(errEntriesAddedSmallerThanStreamSize);
  }

  metadata.last_generated_id = last_generated_id;
  if (entries_added) {
    metadata.entries_added = *entries_added;
  }
  if (max_deleted_id && (max_deleted_id->ms != 0 || max_deleted_id->seq != 0)) {
    metadata.max_deleted_entry_id = *max_deleted_id;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisStream, {"XSETID"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Stream::GetPendingEntries(engine::Context &ctx, StreamPendingOptions &options,
                                          StreamGetPendingEntryResult &pending_infos,
                                          std::vector<StreamNACK> &ext_results) {
  const std::string &stream_name = options.stream_name;
  const std::string &group_name = options.group_name;
  std::string ns_key = AppendNamespacePrefix(stream_name);

  StreamMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  std::string group_key = internalKeyFromGroupName(ns_key, metadata, group_name);
  std::string get_group_value;
  s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, group_key, &get_group_value);
  if (!s.ok()) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }

  std::string prefix_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, options.start_id);
  std::string end_key = internalPelKeyFromGroupAndEntryId(ns_key, metadata, group_name, options.end_id);

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(end_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, stream_cf_handle_);
  std::unordered_set<std::string> consumer_names;
  StreamEntryID first_entry_id{StreamEntryID::Maximum()};
  StreamEntryID last_entry_id{StreamEntryID::Minimum()};
  uint64_t ext_result_count = 0;
  uint64_t summary_result_count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    if (options.with_count && options.count <= ext_result_count) {
      break;
    }
    std::string tmp_group_name;
    StreamEntryID entry_id = groupAndEntryIdFromPelInternalKey(iter->key(), tmp_group_name);

    if (first_entry_id > entry_id) {
      first_entry_id = entry_id;
    }
    if (last_entry_id < entry_id) {
      last_entry_id = entry_id;
    }
    StreamPelEntry pel_entry = decodeStreamPelEntryValue(iter->value().ToString());
    if (options.with_time && util::GetTimeStampMS() - pel_entry.last_delivery_time_ms < options.idle_time) {
      continue;
    }

    const std::string &consumer_name = pel_entry.consumer_name;

    if (options.with_consumer && options.consumer != consumer_name) {
      continue;
    }

    if (options.with_count) {
      ext_results.push_back(
          {entry_id, {pel_entry.last_delivery_time_ms, pel_entry.last_delivery_count, consumer_name}});
      ext_result_count++;
      continue;
    }
    std::string consumer_key = internalKeyFromConsumerName(ns_key, metadata, group_name, consumer_name);
    std::string get_consumer_value;
    s = storage_->Get(ctx, ctx.GetReadOptions(), stream_cf_handle_, consumer_key, &get_consumer_value);
    if (!s.ok() && !s.IsNotFound()) {
      return s;
    }
    if (s.IsNotFound()) {
      return rocksdb::Status::OK();
    }

    StreamConsumerMetadata consumer_metadata = decodeStreamConsumerMetadataValue(get_consumer_value);
    if (consumer_names.find(consumer_name) == consumer_names.end()) {
      consumer_names.insert(consumer_name);
      pending_infos.consumer_infos.emplace_back(consumer_name, consumer_metadata.pending_number);
    }
    summary_result_count++;
  }
  pending_infos.last_entry_id = last_entry_id;
  pending_infos.first_entry_id = first_entry_id;
  pending_infos.pending_number = summary_result_count;
  return rocksdb::Status::OK();
}

}  // namespace redis
