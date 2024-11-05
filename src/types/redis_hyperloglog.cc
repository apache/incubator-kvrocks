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

#include "redis_hyperloglog.h"

#include <db_util.h>
#include <stdint.h>

#include "hyperloglog.h"
#include "vendor/murmurhash2.h"

namespace redis {

namespace {
template <typename ElementType>
std::vector<nonstd::span<const uint8_t>> TransformToSpan(const std::vector<ElementType> &registers) {
  std::vector<nonstd::span<const uint8_t>> register_segments;
  register_segments.reserve(kHyperLogLogSegmentCount);
  for (const auto &register_segment : registers) {
    if (register_segment.empty()) {
      // Empty segment
      register_segments.emplace_back();
      continue;
    }
    // NOLINTNEXTLINE
    const uint8_t *segment_data_ptr = reinterpret_cast<const uint8_t *>(register_segment.data());
    register_segments.emplace_back(segment_data_ptr, register_segment.size());
  }
  return register_segments;
}
}  // namespace

/// Cache for writing to a HyperLogLog.
///
/// This is a bit like Bitmap::SegmentCacheStore, but simpler because
/// 1. We would only use it for writing, hll reading always traverses all segments.
/// 2. Some write access doesn't mark the segment as dirty, because the update value
///    is less than the current value. So that we need to export `SegmentEntry` to
///    the caller.
///
/// When read from storage, if the segment exists and it size is not equal to
/// `kHyperLogLogRegisterBytesPerSegment`, it will be treated as a corruption.
class HllSegmentCache {
 public:
  struct SegmentEntry {
    /// The segment data, it's would always equal to `kHyperLogLogRegisterBytesPerSegment`.
    std::string data;
    bool dirty;
  };
  std::map<uint32_t, SegmentEntry> segments;

  /// Get the segment from cache or storage.
  ///
  /// If the segment in not in the cache and storage, it will be initialized with
  /// string(kHyperLogLogSegmentBytes, 0) and return OK.
  template <typename GetSegmentFn>
  rocksdb::Status Get(uint32_t segment_index, const GetSegmentFn &get_segment, SegmentEntry **entry) {
    auto iter = segments.find(segment_index);
    if (iter == segments.end()) {
      std::string segment_data;
      auto s = get_segment(segment_index, &segment_data);
      if (!s.ok()) {
        if (s.IsNotFound()) {
          iter = segments.emplace(segment_index, SegmentEntry{std::move(segment_data), false}).first;
          // Initialize the segment with 0
          iter->second.data.resize(kHyperLogLogSegmentBytes, 0);
          *entry = &iter->second;
          return rocksdb::Status::OK();
        }
        return s;
      }
      iter = segments.emplace(segment_index, SegmentEntry{std::move(segment_data), false}).first;
    }
    if (iter->second.data.size() != kHyperLogLogSegmentBytes) {
      return rocksdb::Status::Corruption("invalid segment size: expect=" + std::to_string(kHyperLogLogSegmentBytes) +
                                         ", actual=" + std::to_string(iter->second.data.size()));
    }
    *entry = &iter->second;
    return rocksdb::Status::OK();
  }
};

rocksdb::Status HyperLogLog::GetMetadata(engine::Context &ctx, const Slice &ns_key, HyperLogLogMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisHyperLogLog}, ns_key, metadata);
}

uint64_t HyperLogLog::HllHash(std::string_view element) {
  DCHECK(element.size() <= std::numeric_limits<int32_t>::max());
  return HllMurMurHash64A(element.data(), static_cast<int32_t>(element.size()), kHyperLogLogHashSeed);
}

/* the max 0 pattern counter of the subset the element belongs to is incremented if needed */
rocksdb::Status HyperLogLog::Add(engine::Context &ctx, const Slice &user_key,
                                 const std::vector<uint64_t> &element_hashes, uint64_t *ret) {
  *ret = 0;
  std::string ns_key = AppendNamespacePrefix(user_key);

  HyperLogLogMetadata metadata{};
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperLogLog);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  HllSegmentCache cache;
  for (uint64_t element_hash : element_hashes) {
    DenseHllResult dense_hll_result = ExtractDenseHllResult(element_hash);
    uint32_t segment_index = dense_hll_result.register_index / kHyperLogLogSegmentRegisters;
    uint32_t register_index_in_segment = dense_hll_result.register_index % kHyperLogLogSegmentRegisters;
    HllSegmentCache::SegmentEntry *entry{nullptr};
    s = cache.Get(
        segment_index,
        [this, &ns_key, &metadata, &ctx](uint32_t segment_index, std::string *segment) -> rocksdb::Status {
          std::string sub_key =
              InternalKey(ns_key, std::to_string(segment_index), metadata.version, storage_->IsSlotIdEncoded())
                  .Encode();
          return storage_->Get(ctx, ctx.GetReadOptions(), sub_key, segment);
        },
        &entry);
    if (!s.ok()) return s;
    DCHECK(entry != nullptr);
    DCHECK_EQ(kHyperLogLogSegmentBytes, entry->data.size());
    auto *segment_data = reinterpret_cast<uint8_t *>(entry->data.data());
    uint8_t old_count = HllDenseGetRegister(segment_data, register_index_in_segment);
    if (dense_hll_result.hll_trailing_zero > old_count) {
      HllDenseSetRegister(segment_data, register_index_in_segment, dense_hll_result.hll_trailing_zero);
      entry->dirty = true;
      *ret = 1;
    }
  }
  // Nothing changed, no need to flush the segments
  if (*ret == 0) {
    return rocksdb::Status::OK();
  }

  // Flush dirty segments
  // Release memory after batch is written
  for (auto &[segment_index, entry] : cache.segments) {
    if (entry.dirty) {
      std::string sub_key =
          InternalKey(ns_key, std::to_string(segment_index), metadata.version, storage_->IsSlotIdEncoded()).Encode();
      s = batch->Put(sub_key, entry.data);
      if (!s.ok()) return s;
      entry.data.clear();
    }
  }
  cache.segments.clear();
  // Update metadata
  {
    metadata.encode_type = HyperLogLogMetadata::EncodeType::DENSE;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status HyperLogLog::Count(engine::Context &ctx, const Slice &user_key, uint64_t *ret) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  *ret = 0;
  std::vector<rocksdb::PinnableSlice> registers;
  auto s = getRegisters(ctx, ns_key, &registers);
  if (!s.ok()) {
    return s;
  }
  DCHECK_EQ(kHyperLogLogSegmentCount, registers.size());
  std::vector<nonstd::span<const uint8_t>> register_segments = TransformToSpan(registers);
  *ret = HllDenseEstimate(register_segments);
  return rocksdb::Status::OK();
}

rocksdb::Status HyperLogLog::mergeUserKeys(engine::Context &ctx, const std::vector<Slice> &user_keys,
                                           std::vector<std::string> *register_segments) {
  DCHECK_GE(user_keys.size(), static_cast<size_t>(1));

  std::string first_ns_key = AppendNamespacePrefix(user_keys[0]);
  rocksdb::Status s = getRegisters(ctx, first_ns_key, register_segments);
  if (!s.ok()) return s;
  // The set of keys that have been seen so far
  std::unordered_set<std::string_view> seend_user_keys;
  seend_user_keys.emplace(user_keys[0].ToStringView());

  for (size_t idx = 1; idx < user_keys.size(); idx++) {
    rocksdb::Slice source_user_key = user_keys[idx];
    if (!seend_user_keys.emplace(source_user_key.ToStringView()).second) {
      // Skip duplicate keys
      continue;
    }
    std::string source_key = AppendNamespacePrefix(source_user_key);
    std::vector<rocksdb::PinnableSlice> source_registers;
    s = getRegisters(ctx, source_key, &source_registers);
    if (!s.ok()) return s;
    DCHECK_EQ(kHyperLogLogSegmentCount, source_registers.size());
    DCHECK_EQ(kHyperLogLogSegmentCount, register_segments->size());
    std::vector<nonstd::span<const uint8_t>> source_register_span = TransformToSpan(source_registers);
    HllMerge(register_segments, source_register_span);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status HyperLogLog::CountMultiple(engine::Context &ctx, const std::vector<Slice> &user_key, uint64_t *ret) {
  DCHECK_GT(user_key.size(), static_cast<size_t>(1));
  std::vector<std::string> register_segments;
  auto s = mergeUserKeys(ctx, user_key, &register_segments);
  if (!s.ok()) return s;
  std::vector<nonstd::span<const uint8_t>> register_segment_span = TransformToSpan(register_segments);
  *ret = HllDenseEstimate(register_segment_span);
  return rocksdb::Status::OK();
}

rocksdb::Status HyperLogLog::Merge(engine::Context &ctx, const Slice &dest_user_key,
                                   const std::vector<Slice> &source_user_keys) {
  if (source_user_keys.empty()) {
    return rocksdb::Status::OK();
  }

  std::string dest_key = AppendNamespacePrefix(dest_user_key);
  std::vector<std::string> registers;
  HyperLogLogMetadata metadata;

  rocksdb::Status s = GetMetadata(ctx, dest_user_key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  {
    std::vector<Slice> all_user_keys;
    all_user_keys.reserve(source_user_keys.size() + 1);
    all_user_keys.push_back(dest_user_key);
    for (const auto &source_user_key : source_user_keys) {
      all_user_keys.push_back(source_user_key);
    }
    s = mergeUserKeys(ctx, all_user_keys, &registers);
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisHyperLogLog);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;
  for (uint32_t i = 0; i < kHyperLogLogSegmentCount; i++) {
    if (registers[i].empty()) {
      continue;
    }
    std::string sub_key =
        InternalKey(dest_key, std::to_string(i), metadata.version, storage_->IsSlotIdEncoded()).Encode();
    s = batch->Put(sub_key, registers[i]);
    if (!s.ok()) return s;
    // Release memory after batch is written
    registers[i].clear();
  }
  // Metadata
  {
    metadata.encode_type = HyperLogLogMetadata::EncodeType::DENSE;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, dest_key, bytes);
    if (!s.ok()) return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status HyperLogLog::getRegisters(engine::Context &ctx, const Slice &ns_key,
                                          std::vector<rocksdb::PinnableSlice> *register_segments) {
  HyperLogLogMetadata metadata;
  rocksdb::Status s = GetMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      // return empty registers with the right size.
      register_segments->resize(kHyperLogLogSegmentCount);
      return rocksdb::Status::OK();
    }
    return s;
  }

  // Multi get all segments
  std::vector<std::string> sub_segment_keys;
  sub_segment_keys.reserve(kHyperLogLogSegmentCount);
  for (uint32_t i = 0; i < kHyperLogLogSegmentCount; i++) {
    std::string sub_key =
        InternalKey(ns_key, std::to_string(i), metadata.version, storage_->IsSlotIdEncoded()).Encode();
    sub_segment_keys.push_back(std::move(sub_key));
  }
  std::vector<rocksdb::Slice> sub_segment_slices;
  sub_segment_slices.reserve(kHyperLogLogSegmentCount);
  for (const auto &sub_key : sub_segment_keys) {
    sub_segment_slices.emplace_back(sub_key);
  }
  register_segments->resize(kHyperLogLogSegmentCount);
  std::vector<rocksdb::Status> statuses(kHyperLogLogSegmentCount);
  storage_->MultiGet(ctx, ctx.DefaultMultiGetOptions(), storage_->GetDB()->DefaultColumnFamily(),
                     kHyperLogLogSegmentCount, sub_segment_slices.data(), register_segments->data(), statuses.data());
  for (size_t i = 0; i < kHyperLogLogSegmentCount; i++) {
    if (!statuses[i].ok() && !statuses[i].IsNotFound()) {
      register_segments->at(i).clear();
      return statuses[i];
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status HyperLogLog::getRegisters(engine::Context &ctx, const Slice &ns_key,
                                          std::vector<std::string> *register_segments) {
  std::vector<rocksdb::PinnableSlice> pinnable_slices;
  rocksdb::Status s = getRegisters(ctx, ns_key, &pinnable_slices);
  if (!s.ok()) return s;
  register_segments->reserve(kHyperLogLogSegmentCount);
  for (auto &pinnable_slice : pinnable_slices) {
    register_segments->push_back(pinnable_slice.ToString());
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
