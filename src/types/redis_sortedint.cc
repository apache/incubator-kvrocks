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

#include "redis_sortedint.h"

#include <iostream>
#include <limits>

#include "db_util.h"
#include "parse_util.h"

namespace redis {

rocksdb::Status Sortedint::GetMetadata(engine::Context &ctx, const Slice &ns_key, SortedintMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisSortedint}, ns_key, metadata);
}

rocksdb::Status Sortedint::Add(engine::Context &ctx, const Slice &user_key, const std::vector<uint64_t> &ids,
                               uint64_t *added_cnt) {
  *added_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  SortedintMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string value;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisSortedint);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  for (const auto id : ids) {
    std::string id_buf;
    PutFixed64(&id_buf, id);
    std::string sub_key = InternalKey(ns_key, id_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (s.ok()) continue;
    s = batch->Put(sub_key, Slice());
    if (!s.ok()) return s;
    *added_cnt += 1;
  }

  if (*added_cnt == 0) return rocksdb::Status::OK();

  metadata.size += *added_cnt;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Sortedint::Remove(engine::Context &ctx, const Slice &user_key, const std::vector<uint64_t> &ids,
                                  uint64_t *removed_cnt) {
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string value;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisSortedint);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  for (const auto id : ids) {
    std::string id_buf;
    PutFixed64(&id_buf, id);
    std::string sub_key = InternalKey(ns_key, id_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (!s.ok()) continue;
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    *removed_cnt += 1;
  }
  if (*removed_cnt == 0) return rocksdb::Status::OK();
  metadata.size -= *removed_cnt;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Sortedint::Card(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Sortedint::Range(engine::Context &ctx, const Slice &user_key, uint64_t cursor_id, uint64_t offset,
                                 uint64_t limit, bool reversed, std::vector<uint64_t> *ids) {
  ids->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_buf;
  uint64_t start_id = cursor_id;
  if (reversed && cursor_id == 0) {
    start_id = std::numeric_limits<uint64_t>::max();
  }
  PutFixed64(&start_buf, start_id);
  std::string start_key = InternalKey(ns_key, start_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  uint64_t id = 0, pos = 0;
  auto iter = util::UniqueIterator(ctx, read_options);
  for (!reversed ? iter->Seek(start_key) : iter->SeekForPrev(start_key);
       iter->Valid() && iter->key().starts_with(prefix); !reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice sub_key = ikey.GetSubKey();
    GetFixed64(&sub_key, &id);
    if (id == cursor_id || pos++ < offset) continue;
    ids->emplace_back(id);
    if (limit > 0 && ids->size() >= limit) break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Sortedint::RangeByValue(engine::Context &ctx, const Slice &user_key, SortedintRangeSpec spec,
                                        std::vector<uint64_t> *ids, int *size) {
  if (size) *size = 0;
  if (ids) ids->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string start_buf;
  PutFixed64(&start_buf, spec.reversed ? spec.max : spec.min);
  std::string start_key = InternalKey(ns_key, start_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix_key = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix_key =
      InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix_key);
  read_options.iterate_lower_bound = &lower_bound;

  int pos = 0;
  auto iter = util::UniqueIterator(ctx, read_options);
  if (!spec.reversed) {
    iter->Seek(start_key);
  } else {
    iter->SeekForPrev(start_key);
  }

  uint64_t id = 0;
  for (; iter->Valid() && iter->key().starts_with(prefix_key); !spec.reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice sub_key = ikey.GetSubKey();
    GetFixed64(&sub_key, &id);
    if (spec.reversed) {
      if ((spec.minex && id == spec.min) || id < spec.min) break;
      if ((spec.maxex && id == spec.max) || id > spec.max) continue;
    } else {
      if ((spec.minex && id == spec.min) || id < spec.min) continue;
      if ((spec.maxex && id == spec.max) || id > spec.max) break;
    }
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    if (ids) ids->emplace_back(id);
    if (size) *size += 1;
    if (spec.count > 0 && ids && ids->size() >= static_cast<unsigned>(spec.count)) break;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Sortedint::MExist(engine::Context &ctx, const Slice &user_key, const std::vector<uint64_t> &ids,
                                  std::vector<int> *exists) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  SortedintMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::string value;
  for (const auto id : ids) {
    std::string id_buf;
    PutFixed64(&id_buf, id);
    std::string sub_key = InternalKey(ns_key, id_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (!s.ok() && !s.IsNotFound()) return s;
    if (s.IsNotFound()) {
      exists->emplace_back(0);
    } else {
      exists->emplace_back(1);
    }
  }
  return rocksdb::Status::OK();
}

Status Sortedint::ParseRangeSpec(const std::string &min, const std::string &max, SortedintRangeSpec *spec) {
  if (min == "+inf" || max == "-inf") {
    return {Status::NotOK, "min > max"};
  }

  if (min == "-inf") {
    spec->min = std::numeric_limits<uint64_t>::lowest();
  } else {
    const char *sptr = min.data();
    if (!min.empty() && min[0] == '(') {
      spec->minex = true;
      sptr++;
    }
    auto parse_result = ParseInt<uint64_t>(sptr, 10);
    if (!parse_result) {
      return {Status::NotOK, "the min isn't integer"};
    }
    spec->min = *parse_result;
  }

  if (max == "+inf") {
    spec->max = std::numeric_limits<uint64_t>::max();
  } else {
    const char *sptr = max.data();
    if (!max.empty() && max[0] == '(') {
      spec->maxex = true;
      sptr++;
    }
    auto parse_result = ParseInt<uint64_t>(sptr, 10);
    if (!parse_result) {
      return {Status::NotOK, "the max isn't integer"};
    }
    spec->max = *parse_result;
  }
  return Status::OK();
}

}  // namespace redis
