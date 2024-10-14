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

#pragma once

#include <rocksdb/status.h>

#include <optional>
#include <string>
#include <vector>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

using rocksdb::Slice;

namespace redis {

class Stream : public SubKeyScanner {
 public:
  explicit Stream(engine::Storage *storage, const std::string &ns)
      : SubKeyScanner(storage, ns), stream_cf_handle_(storage->GetCFHandle(ColumnFamilyID::Stream)) {}
  rocksdb::Status Add(engine::Context &ctx, const Slice &stream_name, const StreamAddOptions &options,
                      const std::vector<std::string> &values, StreamEntryID *id);
  rocksdb::Status CreateGroup(engine::Context &ctx, const Slice &stream_name, const StreamXGroupCreateOptions &options,
                              const std::string &group_name);
  rocksdb::Status DestroyGroup(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                               uint64_t *delete_cnt);
  rocksdb::Status CreateConsumer(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                 const std::string &consumer_name, int *created_number);
  rocksdb::Status DestroyConsumer(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                  const std::string &consumer_name, uint64_t &deleted_pel);
  rocksdb::Status GroupSetId(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                             const StreamXGroupCreateOptions &options);
  rocksdb::Status DeleteEntries(engine::Context &ctx, const Slice &stream_name, const std::vector<StreamEntryID> &ids,
                                uint64_t *deleted_cnt);
  rocksdb::Status DeletePelEntries(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                   const std::vector<StreamEntryID> &entry_ids, uint64_t *acknowledged);
  rocksdb::Status ClaimPelEntries(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                  const std::string &consumer_name, uint64_t min_idle_time_ms,
                                  const std::vector<StreamEntryID> &entry_ids, const StreamClaimOptions &options,
                                  StreamClaimResult *result);
  rocksdb::Status AutoClaim(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                            const std::string &consumer_name, const StreamAutoClaimOptions &options,
                            StreamAutoClaimResult *result);
  rocksdb::Status Len(engine::Context &ctx, const Slice &stream_name, const StreamLenOptions &options, uint64_t *size);
  rocksdb::Status GetStreamInfo(engine::Context &ctx, const Slice &stream_name, bool full, uint64_t count,
                                StreamInfo *info);
  rocksdb::Status GetGroupInfo(engine::Context &ctx, const Slice &stream_name,
                               std::vector<std::pair<std::string, StreamConsumerGroupMetadata>> &group_metadata);
  rocksdb::Status GetConsumerInfo(engine::Context &ctx, const Slice &stream_name, const std::string &group_name,
                                  std::vector<std::pair<std::string, StreamConsumerMetadata>> &consumer_metadata);
  rocksdb::Status GetPendingEntries(engine::Context &ctx, StreamPendingOptions &options,
                                    StreamGetPendingEntryResult &pending_infos, std::vector<StreamNACK> &ext_results);
  rocksdb::Status Range(engine::Context &ctx, const Slice &stream_name, const StreamRangeOptions &options,
                        std::vector<StreamEntry> *entries);
  rocksdb::Status RangeWithPending(engine::Context &ctx, const Slice &stream_name, StreamRangeOptions &options,
                                   std::vector<StreamEntry> *entries, std::string &group_name,
                                   std::string &consumer_name, bool noack, bool latest);
  rocksdb::Status Trim(engine::Context &ctx, const Slice &stream_name, const StreamTrimOptions &options,
                       uint64_t *delete_cnt);
  rocksdb::Status GetMetadata(engine::Context &ctx, const Slice &stream_name, StreamMetadata *metadata);
  rocksdb::Status GetLastGeneratedID(engine::Context &ctx, const Slice &stream_name, StreamEntryID *id);
  rocksdb::Status SetId(engine::Context &ctx, const Slice &stream_name, const StreamEntryID &last_generated_id,
                        std::optional<uint64_t> entries_added, std::optional<StreamEntryID> max_deleted_id);

 private:
  rocksdb::ColumnFamilyHandle *stream_cf_handle_;

  rocksdb::Status range(engine::Context &ctx, const std::string &ns_key, const StreamMetadata &metadata,
                        const StreamRangeOptions &options, std::vector<StreamEntry> *entries) const;
  rocksdb::Status getEntryRawValue(engine::Context &ctx, const std::string &ns_key, const StreamMetadata &metadata,
                                   const StreamEntryID &id, std::string *value) const;
  StreamEntryID entryIDFromInternalKey(const rocksdb::Slice &key) const;
  std::string internalKeyFromEntryID(const std::string &ns_key, const StreamMetadata &metadata,
                                     const StreamEntryID &id) const;
  rocksdb::Status trim(engine::Context &ctx, const std::string &ns_key, const StreamTrimOptions &options,
                       StreamMetadata *metadata, rocksdb::WriteBatch *batch, uint64_t &delete_cnt);
  std::string internalKeyFromGroupName(const std::string &ns_key, const StreamMetadata &metadata,
                                       const std::string &group_name) const;
  std::string groupNameFromInternalKey(rocksdb::Slice key) const;
  static std::string encodeStreamConsumerGroupMetadataValue(const StreamConsumerGroupMetadata &consumer_group_metadata);
  static StreamConsumerGroupMetadata decodeStreamConsumerGroupMetadataValue(const std::string &value);
  std::string internalKeyFromConsumerName(const std::string &ns_key, const StreamMetadata &metadata,
                                          const std::string &group_name, const std::string &consumer_name) const;
  std::string consumerNameFromInternalKey(rocksdb::Slice key) const;
  static std::string encodeStreamConsumerMetadataValue(const StreamConsumerMetadata &consumer_metadata);
  static StreamConsumerMetadata decodeStreamConsumerMetadataValue(const std::string &value);
  rocksdb::Status createConsumerWithoutLock(engine::Context &ctx, const Slice &stream_name,
                                            const std::string &group_name, const std::string &consumer_name,
                                            int *created_number);
  std::string internalPelKeyFromGroupAndEntryId(const std::string &ns_key, const StreamMetadata &metadata,
                                                const std::string &group_name, const StreamEntryID &id);
  StreamEntryID groupAndEntryIdFromPelInternalKey(rocksdb::Slice key, std::string &group_name);
  static std::string encodeStreamPelEntryValue(const StreamPelEntry &pel_entry);
  static StreamPelEntry decodeStreamPelEntryValue(const std::string &value);
  StreamSubkeyType identifySubkeyType(const rocksdb::Slice &key) const;
};

}  // namespace redis
