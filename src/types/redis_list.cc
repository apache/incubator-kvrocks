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

#include "redis_list.h"

#include <cstdlib>
#include <utility>

#include "db_util.h"

namespace redis {

rocksdb::Status List::GetMetadata(engine::Context &ctx, const Slice &ns_key, ListMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisList}, ns_key, metadata);
}

rocksdb::Status List::Size(engine::Context &ctx, const Slice &user_key, uint64_t *size) {
  *size = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;
  *size = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status List::Push(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems, bool left,
                           uint64_t *new_size) {
  return push(ctx, user_key, elems, true, left, new_size);
}

rocksdb::Status List::PushX(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems, bool left,
                            uint64_t *new_size) {
  return push(ctx, user_key, elems, false, left, new_size);
}

rocksdb::Status List::push(engine::Context &ctx, const Slice &user_key, const std::vector<Slice> &elems,
                           bool create_if_missing, bool left, uint64_t *new_size) {
  *new_size = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  ListMetadata metadata;
  auto batch = storage_->GetWriteBatchBase();
  RedisCommand cmd = left ? kRedisCmdLPush : kRedisCmdRPush;
  WriteBatchLogData log_data(kRedisList, {std::to_string(cmd)});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  LockGuard guard(storage_->GetLockManager(), ns_key);
  s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !(create_if_missing && s.IsNotFound())) {
    return s.IsNotFound() ? rocksdb::Status::OK() : s;
  }
  uint64_t index = left ? metadata.head - 1 : metadata.tail;
  for (const auto &elem : elems) {
    std::string index_buf;
    PutFixed64(&index_buf, index);
    std::string sub_key = InternalKey(ns_key, index_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Put(sub_key, elem);
    if (!s.ok()) return s;
    left ? --index : ++index;
  }
  if (left) {
    metadata.head -= elems.size();
  } else {
    metadata.tail += elems.size();
  }
  std::string bytes;
  metadata.size += elems.size();
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  *new_size = metadata.size;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::Pop(engine::Context &ctx, const Slice &user_key, bool left, std::string *elem) {
  elem->clear();

  std::vector<std::string> elems;
  auto s = PopMulti(ctx, user_key, left, 1, &elems);
  if (!s.ok()) return s;

  *elem = std::move(elems[0]);
  return rocksdb::Status::OK();
}

rocksdb::Status List::PopMulti(engine::Context &ctx, const rocksdb::Slice &user_key, bool left, uint32_t count,
                               std::vector<std::string> *elems) {
  elems->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  auto batch = storage_->GetWriteBatchBase();
  RedisCommand cmd = left ? kRedisCmdLPop : kRedisCmdRPop;
  WriteBatchLogData log_data(kRedisList, {std::to_string(cmd)});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  while (metadata.size > 0 && count > 0) {
    uint64_t index = left ? metadata.head : metadata.tail - 1;
    std::string buf;
    PutFixed64(&buf, index);
    std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    std::string elem;
    s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &elem);
    if (!s.ok()) {
      // FIXME: should be always exists??
      return s;
    }

    elems->push_back(elem);
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    metadata.size -= 1;
    left ? ++metadata.head : --metadata.tail;
    --count;
  }

  if (metadata.size == 0) {
    s = batch->Delete(metadata_cf_handle_, ns_key);
    if (!s.ok()) return s;
  } else {
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

/*
 * LRem would remove which value is equal to elem, and count limit the remove number and direction
 * Caution: The LRem timing complexity is O(N), don't use it on a long list
 * The simplified description of LRem Algorithm follows those steps:
 * 1. find out all the index of elems to delete
 * 2. determine to move the remain elems from the left or right by the length of moving elems
 * 3. move the remain elems with overlay
 * 4. trim and delete
 * For example: lrem list hello 0
 * when the list was like this:
 * | E1 | E2 | E3 | hello | E4 | E5 | hello | E6 |
 * the index of elems to delete is [3, 6], left part size is 6 and right part size is 4,
 * so move elems from right to left:
 * => | E1 | E2 | E3 | E4 | E4 | E5 | hello | E6 |
 * => | E1 | E2 | E3 | E4 | E5 | E5 | hello | E6 |
 * => | E1 | E2 | E3 | E4 | E5 | E6 | hello | E6 |
 * then trim the list from tail with num of elems to delete, here is 2.
 * and list would become: | E1 | E2 | E3 | E4 | E5 | E6 |
 */
rocksdb::Status List::Rem(engine::Context &ctx, const Slice &user_key, int count, const Slice &elem,
                          uint64_t *removed_cnt) {
  *removed_cnt = 0;

  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  uint64_t index = count >= 0 ? metadata.head : metadata.tail - 1;
  std::string buf;
  PutFixed64(&buf, index);
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  bool reversed = count < 0;
  std::vector<uint64_t> to_delete_indexes;
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix);
       !reversed ? iter->Next() : iter->Prev()) {
    if (iter->value() == elem) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      Slice sub_key = ikey.GetSubKey();
      GetFixed64(&sub_key, &index);
      to_delete_indexes.emplace_back(index);
      if (static_cast<int>(to_delete_indexes.size()) == abs(count)) break;
    }
  }
  if (to_delete_indexes.empty()) {
    return rocksdb::Status::NotFound();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLRem), std::to_string(count), elem.ToString()});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  if (to_delete_indexes.size() == metadata.size) {
    s = batch->Delete(metadata_cf_handle_, ns_key);
    if (!s.ok()) return s;
  } else {
    uint64_t min_to_delete_index = !reversed ? to_delete_indexes[0] : to_delete_indexes[to_delete_indexes.size() - 1];
    uint64_t max_to_delete_index = !reversed ? to_delete_indexes[to_delete_indexes.size() - 1] : to_delete_indexes[0];
    uint64_t left_part_len = max_to_delete_index - metadata.head;
    uint64_t right_part_len = metadata.tail - 1 - min_to_delete_index;
    reversed = left_part_len <= right_part_len;
    buf.clear();
    PutFixed64(&buf, reversed ? max_to_delete_index : min_to_delete_index);
    start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    size_t processed = 0;
    for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix);
         !reversed ? iter->Next() : iter->Prev()) {
      if (iter->value() != elem || processed >= to_delete_indexes.size()) {
        buf.clear();
        PutFixed64(&buf, reversed ? max_to_delete_index-- : min_to_delete_index++);
        std::string to_update_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
        s = batch->Put(to_update_key, iter->value());
        if (!s.ok()) return s;
      } else {
        processed++;
      }
    }

    for (uint64_t idx = 0; idx < to_delete_indexes.size(); ++idx) {
      buf.clear();
      PutFixed64(&buf, reversed ? (metadata.head + idx) : (metadata.tail - 1 - idx));
      std::string to_delete_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      s = batch->Delete(to_delete_key);
      if (!s.ok()) return s;
    }
    if (reversed) {
      metadata.head += to_delete_indexes.size();
    } else {
      metadata.tail -= to_delete_indexes.size();
    }
    metadata.size -= to_delete_indexes.size();
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  *removed_cnt = to_delete_indexes.size();
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::Insert(engine::Context &ctx, const Slice &user_key, const Slice &pivot, const Slice &elem,
                             bool before, int *new_size) {
  *new_size = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  std::string buf;
  uint64_t pivot_index = metadata.head - 1;
  PutFixed64(&buf, metadata.head);
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    if (iter->value() == pivot) {
      InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
      Slice sub_key = ikey.GetSubKey();
      GetFixed64(&sub_key, &pivot_index);
      break;
    }
  }
  if (pivot_index == (metadata.head - 1)) {
    *new_size = -1;
    return rocksdb::Status::NotFound();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList,
                             {std::to_string(kRedisCmdLInsert), before ? "1" : "0", pivot.ToString(), elem.ToString()});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  uint64_t left_part_len = pivot_index - metadata.head + (before ? 0 : 1);
  uint64_t right_part_len = metadata.tail - 1 - pivot_index + (before ? 1 : 0);
  bool reversed = left_part_len <= right_part_len;
  uint64_t new_elem_index = 0;
  if ((reversed && !before) || (!reversed && before)) {
    new_elem_index = pivot_index;
  } else {
    new_elem_index = reversed ? --pivot_index : ++pivot_index;
    !reversed ? iter->Next() : iter->Prev();
  }
  for (; iter->Valid() && iter->key().starts_with(prefix); !reversed ? iter->Next() : iter->Prev()) {
    buf.clear();
    PutFixed64(&buf, reversed ? --pivot_index : ++pivot_index);
    std::string to_update_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Put(to_update_key, iter->value());
    if (!s.ok()) return s;
  }
  buf.clear();
  PutFixed64(&buf, new_elem_index);
  std::string to_update_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = batch->Put(to_update_key, elem);
  if (!s.ok()) return s;

  if (reversed) {
    metadata.head--;
  } else {
    metadata.tail++;
  }
  metadata.size++;
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  *new_size = static_cast<int>(metadata.size);
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::Index(engine::Context &ctx, const Slice &user_key, int index, std::string *elem) {
  elem->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  if (index < 0) index += static_cast<int>(metadata.size);
  if (index < 0 || index >= static_cast<int>(metadata.size)) return rocksdb::Status::NotFound();

  std::string buf;
  PutFixed64(&buf, metadata.head + index);
  std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return storage_->Get(ctx, ctx.GetReadOptions(), sub_key, elem);
}

// The offset can also be negative, -1 is the last element, -2 the penultimate
// Out of range indexes will not produce an error.
// If start is larger than the end of the list, an empty list is returned.
// If stop is larger than the actual end of the list,
// Redis will treat it like the last element of the list.
rocksdb::Status List::Range(engine::Context &ctx, const Slice &user_key, int start, int stop,
                            std::vector<std::string> *elems) {
  elems->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (start < 0) start = static_cast<int>(metadata.size) + start;
  if (stop < 0) stop = static_cast<int>(metadata.size) + stop;
  if (start > static_cast<int>(metadata.size) || stop < 0 || start > stop) return rocksdb::Status::OK();
  if (start < 0) start = 0;

  std::string buf;
  PutFixed64(&buf, metadata.head + start);
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  for (iter->Seek(start_key); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    InternalKey ikey(iter->key(), storage_->IsSlotIdEncoded());
    Slice sub_key = ikey.GetSubKey();
    uint64_t index = 0;
    GetFixed64(&sub_key, &index);
    // index should be always >= start
    if (index > metadata.head + stop) break;
    elems->push_back(iter->value().ToString());
  }
  return rocksdb::Status::OK();
}

rocksdb::Status List::Pos(engine::Context &ctx, const Slice &user_key, const Slice &elem, const PosSpec &spec,
                          std::vector<int64_t> *indexes) {
  indexes->clear();

  std::string ns_key = AppendNamespacePrefix(user_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  // A negative rank means start from the tail.
  int64_t rank = spec.rank;
  uint64_t start = metadata.head;
  bool reversed = false;
  if (rank < 0) {
    rank = -rank;
    start = metadata.tail - 1;
    reversed = true;
  }

  std::string buf;
  PutFixed64(&buf, start);
  std::string start_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string prefix = InternalKey(ns_key, "", metadata.version, storage_->IsSlotIdEncoded()).Encode();
  std::string next_version_prefix = InternalKey(ns_key, "", metadata.version + 1, storage_->IsSlotIdEncoded()).Encode();

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(next_version_prefix);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto list_len = static_cast<int64_t>(metadata.size);
  int64_t max_len = spec.max_len;
  int64_t count = spec.count.value_or(-1);
  int64_t offset = 0, matches = 0;

  auto iter = util::UniqueIterator(ctx, read_options);
  iter->Seek(start_key);
  while (iter->Valid() && iter->key().starts_with(prefix) && (max_len == 0 || offset < max_len)) {
    if (iter->value() == elem) {
      matches++;
      if (matches >= rank) {
        int64_t pos = !reversed ? offset : list_len - offset - 1;
        indexes->push_back(pos);
        if (count != 0 && matches - rank + 1 >= count) {
          break;
        }
      }
    }
    offset++;
    !reversed ? iter->Next() : iter->Prev();
  }
  return rocksdb::Status::OK();
}

rocksdb::Status List::Set(engine::Context &ctx, const Slice &user_key, int index, Slice elem) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;
  if (index < 0) index += static_cast<int>(metadata.size);
  if (index < 0 || index >= static_cast<int>(metadata.size)) {
    return rocksdb::Status::InvalidArgument("index out of range");
  }

  std::string buf, value;
  PutFixed64(&buf, metadata.head + index);
  std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), sub_key, &value);
  if (!s.ok()) {
    return s;
  }
  if (value == elem) return rocksdb::Status::OK();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLSet), std::to_string(index)});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  s = batch->Put(sub_key, elem);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::LMove(engine::Context &ctx, const rocksdb::Slice &src, const rocksdb::Slice &dst, bool src_left,
                            bool dst_left, std::string *elem) {
  if (src == dst) {
    return lmoveOnSingleList(ctx, src, src_left, dst_left, elem);
  }
  return lmoveOnTwoLists(ctx, src, dst, src_left, dst_left, elem);
}

rocksdb::Status List::lmoveOnSingleList(engine::Context &ctx, const rocksdb::Slice &src, bool src_left, bool dst_left,
                                        std::string *elem) {
  std::string ns_key = AppendNamespacePrefix(src);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }

  elem->clear();

  uint64_t curr_index = src_left ? metadata.head : metadata.tail - 1;
  std::string curr_index_buf;
  PutFixed64(&curr_index_buf, curr_index);
  std::string curr_sub_key =
      InternalKey(ns_key, curr_index_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), curr_sub_key, elem);
  if (!s.ok()) {
    return s;
  }

  if (src_left == dst_left) {
    // no-op
    return rocksdb::Status::OK();
  }

  if (metadata.size == 1) {
    // if there is only one element in the list - do nothing, just get it
    return rocksdb::Status::OK();
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLMove), src.ToString(), src.ToString(),
                                          src_left ? "left" : "right", dst_left ? "left" : "right"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  s = batch->Delete(curr_sub_key);
  if (!s.ok()) return s;

  if (src_left) {
    ++metadata.head;
    ++metadata.tail;
  } else {
    --metadata.head;
    --metadata.tail;
  }

  uint64_t new_index = src_left ? metadata.tail - 1 : metadata.head;
  std::string new_index_buf;
  PutFixed64(&new_index_buf, new_index);
  std::string new_sub_key = InternalKey(ns_key, new_index_buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = batch->Put(new_sub_key, *elem);
  if (!s.ok()) return s;

  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status List::lmoveOnTwoLists(engine::Context &ctx, const rocksdb::Slice &src, const rocksdb::Slice &dst,
                                      bool src_left, bool dst_left, std::string *elem) {
  std::string src_ns_key = AppendNamespacePrefix(src);
  std::string dst_ns_key = AppendNamespacePrefix(dst);

  std::vector<std::string> lock_keys{src_ns_key, dst_ns_key};
  MultiLockGuard guard(storage_->GetLockManager(), lock_keys);
  ListMetadata src_metadata(false);
  auto s = GetMetadata(ctx, src_ns_key, &src_metadata);
  if (!s.ok()) {
    return s;
  }

  ListMetadata dst_metadata(false);
  s = GetMetadata(ctx, dst_ns_key, &dst_metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  elem->clear();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, {std::to_string(kRedisCmdLMove), src.ToString(), dst.ToString(),
                                          src_left ? "left" : "right", dst_left ? "left" : "right"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  uint64_t src_index = src_left ? src_metadata.head : src_metadata.tail - 1;
  std::string src_buf;
  PutFixed64(&src_buf, src_index);
  std::string src_sub_key =
      InternalKey(src_ns_key, src_buf, src_metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = storage_->Get(ctx, ctx.GetReadOptions(), src_sub_key, elem);
  if (!s.ok()) {
    return s;
  }

  s = batch->Delete(src_sub_key);
  if (!s.ok()) return s;
  if (src_metadata.size == 1) {
    s = batch->Delete(metadata_cf_handle_, src_ns_key);
    if (!s.ok()) return s;
  } else {
    std::string bytes;
    src_metadata.size -= 1;
    src_left ? ++src_metadata.head : --src_metadata.tail;
    src_metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, src_ns_key, bytes);
    if (!s.ok()) return s;
  }

  uint64_t dst_index = dst_left ? dst_metadata.head - 1 : dst_metadata.tail;
  std::string dst_buf;
  PutFixed64(&dst_buf, dst_index);
  std::string dst_sub_key =
      InternalKey(dst_ns_key, dst_buf, dst_metadata.version, storage_->IsSlotIdEncoded()).Encode();
  s = batch->Put(dst_sub_key, *elem);
  if (!s.ok()) return s;
  dst_left ? --dst_metadata.head : ++dst_metadata.tail;

  std::string bytes;
  dst_metadata.size += 1;
  dst_metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, dst_ns_key, bytes);
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

// Caution: trim the big list may block the server
rocksdb::Status List::Trim(engine::Context &ctx, const Slice &user_key, int start, int stop) {
  uint32_t trim_cnt = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  ListMetadata metadata(false);
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s.IsNotFound() ? rocksdb::Status::OK() : s;

  if (start < 0) start += static_cast<int>(metadata.size);
  if (stop < 0) stop = static_cast<int>(metadata.size) >= -1 * stop ? static_cast<int>(metadata.size) + stop : -1;
  // the result will be empty list when start > stop,
  // or start is larger than the end of list
  if (start > stop) {
    return storage_->Delete(ctx, storage_->DefaultWriteOptions(), metadata_cf_handle_, ns_key);
  }
  if (start < 0) start = 0;

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisList, std::vector<std::string>{std::to_string(kRedisCmdLTrim), std::to_string(start),
                                                                  std::to_string(stop)});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  uint64_t left_index = metadata.head + start;
  uint64_t right_index = metadata.head + stop + 1;
  for (uint64_t i = metadata.head; i < left_index; i++) {
    std::string buf;
    PutFixed64(&buf, i);
    std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    metadata.head++;
    trim_cnt++;
  }
  auto tail = metadata.tail;
  for (uint64_t i = right_index; i < tail; i++) {
    std::string buf;
    PutFixed64(&buf, i);
    std::string sub_key = InternalKey(ns_key, buf, metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Delete(sub_key);
    if (!s.ok()) return s;
    metadata.tail--;
    trim_cnt++;
  }
  if (metadata.size >= trim_cnt) {
    metadata.size -= trim_cnt;
  } else {
    metadata.size = 0;
  }
  std::string bytes;
  metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}
}  // namespace redis
