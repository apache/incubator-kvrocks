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

#include "redis_set.h"

#include <map>
#include <memory>
#include <optional>

#include "db_util.h"
#include "sample_helper.h"

namespace redis {

rocksdb::Status Set::GetMetadata(engine::Context &ctx, const Slice &ns_key, SetMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisSet}, ns_key, metadata);
}

// Make sure members are uniq before use Overwrite
rocksdb::Status Set::Overwrite(engine::Context &ctx, Slice user_key, const std::vector<std::string> &members) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  SetMetadata metadata;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisSet);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  for (const auto &member : members) {
    std::string sub_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Put(sub_key, Slice());
    if (!s.ok()) return s;
  }
  metadata.size = static_cast<uint32_t>(members.size());
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Set::Add(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                         uint64_t *added_cnt) {
  *added_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  SetMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  std::string value;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisSet);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  std::unordered_set<std::string_view> mset;
  for (const auto &member : members) {
    if (!mset.insert(member.ToStringView()).second) {
      continue;
    }
    std::string sub_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (s.ok()) continue;
    s = batch->Put(sub_key, Slice());
    if (!s.ok()) return s;
    *added_cnt += 1;
  }
  if (*added_cnt > 0) {
    metadata.size += *added_cnt;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Set::Remove(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                            uint64_t *removed_cnt) {
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  SetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string value;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisSet);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  std::unordered_set<std::string_view> mset;
  for (const auto &member : members) {
    if (!mset.insert(member.ToStringView()).second) {
      continue;
    }
    std::string sub_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
    if (!s.ok()) continue;
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    *removed_cnt += 1;
  }
  if (*removed_cnt > 0) {
    if (metadata.size != *removed_cnt) {
      metadata.size -= *removed_cnt;
      std::string bytes;
      metadata.Encode(&bytes);
      s = batch->Put(metadata_cf_handle_, ns_key, bytes);
      if (!s.ok()) return s;
    } else {
      s = batch->Delete(metadata_cf_handle_, ns_key);
      if (!s.ok()) return s;
    }
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Set::Card(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  SetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status Set::Members(engine::Context &ctx, const Slice &user_key, std::vector<std::string> *members) {
  members->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  SetMetadata metadata(false);

  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    members->emplace_back(ikey.GetSubKey().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Set::IsMember(engine::Context &ctx, const Slice &user_key, const Slice &member, bool *flag) {
  std::vector<int> exists;
  rocksdb::Status s = MIsMember(ctx, user_key, {member}, &exists);
  if (!s.ok()) return s;
  *flag = exists[0];
  return s;
}

rocksdb::Status Set::MIsMember(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &members,
                               std::vector<int> *exists) {
  exists->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  SetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::string value;
  for (const auto &member : members) {
    std::string sub_key = InternalKey(ns_key, member, metadata.version, storage_->IsSlotIdEncoded()).Encode();
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

rocksdb::Status Set::Take(engine::Context &ctx, const Slice &user_key, std::vector<std::string> *members, int count,
                          bool pop) {
  members->clear();
  bool unique = true;
  if (count == 0) return rocksdb::Status::OK();
  if (count < 0) {
    DCHECK(!pop);
    count = -count;
    unique = false;
  }

  std::string ns_key = AppendNamespacePrefix(user_key);

  SetMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  ObserverOrUniquePtr<rocksdb::WriteBatchBase> batch = storage_->GetWriteBatchBase();
  if (pop) {
    WriteBatchLogData log_data(kRedisSet);
    s = batch->PutLogData(log_data.Encode());
    if (!s.ok()) return s;
  }
  members->clear();
  s = ExtractRandMemberFromSet<std::string>(
      unique, count,
      [this, user_key, &ctx](std::vector<std::string> *samples) { return this->Members(ctx, user_key, samples); },
      members);
  if (!s.ok()) {
    return s;
  }
  // Avoid to write an empty op-log if just random select some members.
  if (!pop) return rocksdb::Status::OK();
  // Avoid to write an empty op-log if the set is empty.
  if (members->empty()) return rocksdb::Status::OK();
  for (std::string &user_sub_key : *members) {
    std::string sub_key = InternalKey(ns_key, user_sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
  }
  metadata.size -= members->size();
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status Set::Move(engine::Context &ctx, const Slice &src, const Slice &dst, const Slice &member, bool *flag) {
  RedisType type = kRedisNone;
  rocksdb::Status s = Type(ctx, dst, &type);
  if (!s.ok()) return s;
  if (type != kRedisNone && type != kRedisSet) {
    return rocksdb::Status::InvalidArgument(kErrMsgWrongType);
  }

  uint64_t ret = 0;
  std::vector<Slice> members{member};
  s = Remove(ctx, src, members, &ret);
  *flag = (ret != 0);
  if (!s.ok() || !*flag) {
    return s;
  }
  s = Add(ctx, dst, members, &ret);
  *flag = (ret != 0);
  return s;
}

rocksdb::Status Set::Scan(engine::Context &ctx, const Slice &user_key, const std::string &cursor, uint64_t limit,
                          const std::string &member_prefix, std::vector<std::string> *members) {
  return SubKeyScanner::Scan(ctx, kRedisSet, user_key, cursor, limit, member_prefix, members);
}

/*
 * Returns the members of the set resulting from the difference between
 * the first set and all the successive sets. For example:
 * key1 = {a,b,c,d}
 * key2 = {c}
 * key3 = {a,c,e}
 * DIFF key1 key2 key3 = {b,d}
 */
rocksdb::Status Set::Diff(engine::Context &ctx, const std::vector<Slice> &keys, std::vector<std::string> *members) {
  members->clear();
  std::vector<std::string> source_members;
  auto s = Members(ctx, keys[0], &source_members);
  if (!s.ok()) return s;

  std::map<std::string, bool> exclude_members;
  std::vector<std::string> target_members;
  for (size_t i = 1; i < keys.size(); i++) {
    s = Members(ctx, keys[i], &target_members);
    if (!s.ok()) return s;
    for (const auto &member : target_members) {
      exclude_members[member] = true;
    }
  }
  for (const auto &member : source_members) {
    if (exclude_members.find(member) == exclude_members.end()) {
      members->push_back(member);
    }
  }
  return rocksdb::Status::OK();
}

/*
 * Returns the members of the set resulting from the union of all the given sets.
 * For example:
 * key1 = {a,b,c,d}
 * key2 = {c}
 * key3 = {a,c,e}
 * UNION key1 key2 key3 = {a,b,c,d,e}
 */
rocksdb::Status Set::Union(engine::Context &ctx, const std::vector<Slice> &keys, std::vector<std::string> *members) {
  members->clear();

  std::map<std::string, bool> union_members;
  std::vector<std::string> target_members;
  for (const auto &key : keys) {
    auto s = Members(ctx, key, &target_members);
    if (!s.ok()) return s;
    for (const auto &member : target_members) {
      union_members[member] = true;
    }
  }
  for (const auto &iter : union_members) {
    members->emplace_back(iter.first);
  }
  return rocksdb::Status::OK();
}

/*
 * Returns the members of the set resulting from the intersection of all the given sets.
 * For example:
 * key1 = {a,b,c,d}
 * key2 = {c}
 * key3 = {a,c,e}
 * INTER key1 key2 key3 = {c}
 */
rocksdb::Status Set::Inter(engine::Context &ctx, const std::vector<Slice> &keys, std::vector<std::string> *members) {
  members->clear();

  std::map<std::string, size_t> member_counters;
  std::vector<std::string> target_members;
  auto s = Members(ctx, keys[0], &target_members);
  if (!s.ok() || target_members.empty()) return s;
  for (const auto &member : target_members) {
    member_counters[member] = 1;
  }
  for (size_t i = 1; i < keys.size(); i++) {
    s = Members(ctx, keys[i], &target_members);
    if (!s.ok() || target_members.empty()) return s;
    for (const auto &member : target_members) {
      if (member_counters.find(member) == member_counters.end()) continue;
      member_counters[member]++;
    }
  }
  for (const auto &iter : member_counters) {
    if (iter.second == keys.size()) {  // all the sets contain this member
      members->emplace_back(iter.first);
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status Set::InterCard(engine::Context &ctx, const std::vector<Slice> &keys, uint64_t limit,
                               uint64_t *cardinality) {
  *cardinality = 0;

  std::map<std::string, size_t> member_counters;
  std::vector<std::string> target_members;

  auto s = Members(ctx, keys[0], &target_members);
  if (!s.ok() || target_members.empty()) return s;
  for (const auto &member : target_members) {
    member_counters[member] = 1;
  }
  if (limit == 0) {
    limit = target_members.size();
  }

  size_t keys_size = keys.size();
  if (keys_size == 1) {
    *cardinality = std::min(static_cast<uint64_t>(target_members.size()), limit);
    return rocksdb::Status::OK();
  }

  bool limit_reached = false;
  for (size_t i = 1; i < keys_size; i++) {
    s = Members(ctx, keys[i], &target_members);
    if (!s.ok() || target_members.empty()) {
      return s;
    }

    for (const auto &member : target_members) {
      auto iter = member_counters.find(member);
      if (iter == member_counters.end()) continue;
      if (++iter->second == keys_size) {
        *cardinality += 1;
        if (--limit == 0) {
          limit_reached = true;
          break;
        }
      }
    }

    if (limit_reached) break;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status Set::DiffStore(engine::Context &ctx, const Slice &dst, const std::vector<Slice> &keys,
                               uint64_t *saved_cnt) {
  *saved_cnt = 0;
  std::vector<std::string> members;
  auto s = Diff(ctx, keys, &members);
  if (!s.ok()) return s;
  *saved_cnt = members.size();
  return Overwrite(ctx, dst, members);
}

rocksdb::Status Set::UnionStore(engine::Context &ctx, const Slice &dst, const std::vector<Slice> &keys,
                                uint64_t *save_cnt) {
  *save_cnt = 0;
  std::vector<std::string> members;
  auto s = Union(ctx, keys, &members);
  if (!s.ok()) return s;
  *save_cnt = members.size();
  return Overwrite(ctx, dst, members);
}

rocksdb::Status Set::InterStore(engine::Context &ctx, const Slice &dst, const std::vector<Slice> &keys,
                                uint64_t *saved_cnt) {
  *saved_cnt = 0;
  std::vector<std::string> members;
  auto s = Inter(ctx, keys, &members);
  if (!s.ok()) return s;
  *saved_cnt = members.size();
  return Overwrite(ctx, dst, members);
}
}  // namespace redis
