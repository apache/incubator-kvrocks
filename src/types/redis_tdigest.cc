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

#include "redis_tdigest.h"

#include <fmt/format.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <range/v3/algorithm/minmax.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <vector>

#include "db_util.h"
#include "encoding.h"
#include "status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/tdigest.h"

namespace redis {

// It should be replaced by a iteration of the rocksdb iterator
class DummyCentroids {
 public:
  DummyCentroids(const TDigestMetadata& meta_data, std::vector<Centroid> centroids)
      : meta_data_(meta_data), centroids_(std::move(centroids)) {}
  class Iterator {
   public:
    Iterator(std::vector<Centroid>::const_iterator&& iter, const std::vector<Centroid>& centroids)
        : iter_(iter), centroids_(centroids) {}
    std::unique_ptr<Iterator> Clone() const {
      if (iter_ != centroids_.cend()) {
        return std::make_unique<Iterator>(std::next(centroids_.cbegin(), std::distance(centroids_.cbegin(), iter_)),
                                          centroids_);
      }
      return std::make_unique<Iterator>(centroids_.cend(), centroids_);
    }
    bool Next() {
      if (Valid()) {
        std::advance(iter_, 1);
      }
      return iter_ != centroids_.cend();
    }
    bool Prev() {
      if (Valid()) {
        if (iter_ != centroids_.cbegin()) {
          std::advance(iter_, -1);
        }
      }
      return Valid();
    }
    bool Valid() const { return iter_ != centroids_.cend(); }
    StatusOr<Centroid> GetCentroid() const {
      if (iter_ == centroids_.cend()) {
        return {::Status::NotOK, "invalid iterator during decoding tdigest centroid"};
      }
      return *iter_;
    }

   private:
    std::vector<Centroid>::const_iterator iter_;
    const std::vector<Centroid>& centroids_;
  };

  std::unique_ptr<Iterator> Begin() { return std::make_unique<Iterator>(centroids_.cbegin(), centroids_); }
  std::unique_ptr<Iterator> End() { return std::make_unique<Iterator>(centroids_.cend(), centroids_); }
  double TotalWeight() const { return static_cast<double>(meta_data_.total_weight); }
  double Min() const { return meta_data_.minimum; }
  double Max() const { return meta_data_.maximum; }
  uint64_t Size() const { return meta_data_.merged_nodes; }

 private:
  const TDigestMetadata& meta_data_;
  std::vector<Centroid> centroids_;
};

uint64_t constexpr kMaxBufferSize = 1 * 1024;  // 1k doubles

std::optional<rocksdb::Status> TDigest::Create(engine::Context& ctx, const Slice& digest_name,
                                               const TDigestCreateOptions& options) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  auto capacity = options.compression * 6 + 10;
  capacity = ((capacity < kMaxBufferSize) ? capacity : kMaxBufferSize);
  TDigestMetadata metadata(options.compression, capacity);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  auto status = GetMetaData(ctx, ns_key, &metadata);
  if (status.ok()) {
    return {};
  }

  if (!status.IsNotFound()) {
    return status;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTDigest);
  status = batch->PutLogData(log_data.Encode());
  if (!status.ok()) {
    return status;
  }

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  status = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!status.ok()) {
    return status;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}
rocksdb::Status TDigest::Add(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& inputs) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  TDigestMetadata metadata;
  auto status = GetMetaData(ctx, ns_key, &metadata);
  if (!status.ok()) {
    return status;
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTDigest);
  status = batch->PutLogData(log_data.Encode());
  if (!status.ok()) {
    return status;
  }

  metadata.total_observations += inputs.size();
  metadata.total_weight += inputs.size();

  if (metadata.unmerged_nodes + inputs.size() <= metadata.capacity) {
    status = appendBuffer(ctx, batch, ns_key, inputs, &metadata);
    if (!status.ok()) {
      return status;
    }
    metadata.unmerged_nodes += inputs.size();
  } else {
    status = mergeCurrentBuffer(ctx, ns_key, batch, &metadata, &inputs);
    if (!status.ok()) {
      return status;
    }
  }

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  status = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
  if (!status.ok()) {
    return status;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TDigest::Quantile(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& qs,
                                  TDigestQuantitleResult* result) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  TDigestMetadata metadata;
  {
    LockGuard guard(storage_->GetLockManager(), ns_key);

    auto status = GetMetaData(ctx, ns_key, &metadata);
    if (!status.ok()) {
      return status;
    }

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisTDigest);
    status = batch->PutLogData(log_data.Encode());
    if (!status.ok()) {
      return status;
    }

    status = mergeCurrentBuffer(ctx, ns_key, batch, &metadata);
    if (!status.ok()) {
      return status;
    }

    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    status = batch->Put(metadata_cf_handle_, ns_key, metadata_bytes);
    if (!status.ok()) {
      return status;
    }

    status = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!status.ok()) {
      return status;
    }

    ctx.RefreshLatestSnapshot();
  }

  std::vector<Centroid> centroids;
  if (auto status = dumpCentroidsAndBuffer(ctx, ns_key, metadata, &centroids); !status.ok()) {
    return status;
  }

  auto dump_centroids = DummyCentroids(metadata, centroids);

  for (auto q : qs) {
    auto s = TDigestQuantile(dump_centroids, q, Lerp);
    if (!s) {
      return rocksdb::Status::InvalidArgument(s.Msg());
    }
    result->quantiles.push_back(*s);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::GetMetaData(engine::Context& context, const Slice& ns_key, TDigestMetadata* metadata) {
  return Database::GetMetadata(context, {kRedisTDigest}, ns_key, metadata);
}

rocksdb::Status TDigest::mergeCurrentBuffer(engine::Context& ctx, const std::string& ns_key,
                                            ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                            TDigestMetadata* metadata, const std::vector<double>* additional_buffer) {
  std::vector<Centroid> centroids;
  std::vector<double> buffer;
  centroids.reserve(metadata->merged_nodes);
  buffer.reserve(metadata->unmerged_nodes + (additional_buffer == nullptr ? 0 : additional_buffer->size()));
  auto status = dumpCentroidsAndBuffer(ctx, ns_key, *metadata, &centroids, &buffer, &batch);
  if (!status.ok()) {
    return status;
  }

  if (additional_buffer != nullptr) {
    std::copy(additional_buffer->cbegin(), additional_buffer->cend(), std::back_inserter(buffer));
  }

  auto merged_centroids = TDigestMerge(buffer, {
                                                   .centroids = centroids,
                                                   .delta = metadata->compression,
                                                   .min = metadata->minimum,
                                                   .max = metadata->maximum,
                                                   .total_weight = static_cast<double>(metadata->merged_weight),
                                               });

  if (!merged_centroids.IsOK()) {
    return rocksdb::Status::InvalidArgument(merged_centroids.Msg());
  }

  status = applyNewCentroids(batch, ns_key, *metadata, merged_centroids->centroids);
  if (!status.ok()) {
    return status;
  }

  metadata->merge_times++;
  metadata->merged_nodes = merged_centroids->centroids.size();
  metadata->unmerged_nodes = 0;
  metadata->minimum = merged_centroids->min;
  metadata->maximum = merged_centroids->max;
  metadata->merged_weight = static_cast<uint64_t>(merged_centroids->total_weight);

  return status;
}

std::string TDigest::internalBufferKey(const std::string& ns_key, const TDigestMetadata& metadata) const {
  std::string sub_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(SegmentType::kBuffer));
  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TDigest::internalKeyFromCentroid(const std::string& ns_key, const TDigestMetadata& metadata,
                                             const Centroid& centroid) const {
  std::string sub_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(SegmentType::kCentroids));
  PutDouble(&sub_key, centroid.mean);  // It uses EncodeDoubleToUInt64 and keeps original order of double
  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TDigest::internalValueFromCentroid(const Centroid& centroid) {
  std::string value;
  PutDouble(&value, centroid.weight);
  return value;
}

rocksdb::Status TDigest::decodeCentroidFromKeyValue(const rocksdb::Slice& key, const rocksdb::Slice& value,
                                                    Centroid* centroid) const {
  InternalKey ikey(key, storage_->IsSlotIdEncoded());
  auto subkey = ikey.GetSubKey();
  auto type_flg = static_cast<uint8_t>(SegmentType::kGuardFlag);
  GetFixed8(&subkey, &type_flg);
  if (static_cast<SegmentType>(type_flg) != SegmentType::kCentroids) {
    return rocksdb::Status::Corruption(fmt::format("corrupted tdigest centroid key type: {}", type_flg));
  }
  GetDouble(&subkey, &centroid->mean);
  GetDouble(&const_cast<rocksdb::Slice&>(value), &centroid->weight);
  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::appendBuffer(engine::Context& ctx, ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                      const std::string& ns_key, const std::vector<double>& inputs,
                                      TDigestMetadata* metadata) {
  // must guard by lock
  auto buffer_key = internalBufferKey(ns_key, *metadata);
  std::string buffer_value;
  auto s = storage_->Get(ctx, ctx.GetReadOptions(), cf_handle_, buffer_key, &buffer_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  for (auto item : inputs) {
    PutDouble(&buffer_value, item);
  }

  if (s = batch->Put(cf_handle_, buffer_key, buffer_value); !s.ok()) {
    return s;
  }

  return s;
}

rocksdb::Status TDigest::dumpCentroidsAndBuffer(engine::Context& ctx, const std::string& ns_key,
                                                const TDigestMetadata& metadata, std::vector<Centroid>* centroids,
                                                std::vector<double>* buffer,
                                                ObserverOrUniquePtr<rocksdb::WriteBatchBase>* clean_after_dump_batch) {
  if (buffer != nullptr) {
    buffer->clear();
    buffer->reserve(metadata.unmerged_nodes);
    auto buffer_key = internalBufferKey(ns_key, metadata);
    std::string buffer_value;
    auto status = storage_->Get(ctx, ctx.GetReadOptions(), cf_handle_, buffer_key, &buffer_value);
    if (!status.ok() && !status.IsNotFound()) {
      return status;
    }

    if (status.ok()) {
      rocksdb::Slice buffer_slice = buffer_value;
      for (uint64_t i = 0; i < metadata.unmerged_nodes; ++i) {
        double tmp_value = std::numeric_limits<double>::quiet_NaN();
        if (!GetDouble(&buffer_slice, &tmp_value)) {
          return rocksdb::Status::Corruption(
              fmt::format("metadata has {} records, but get {} failed", metadata.unmerged_nodes, i));
        }
        buffer->emplace_back(tmp_value);
      }
    }

    if (clean_after_dump_batch != nullptr) {
      if (auto s = (*clean_after_dump_batch)->Delete(cf_handle_, buffer_key); !s.ok()) {
        return s;
      }
    }
  }

  centroids->clear();
  centroids->reserve(metadata.merged_nodes);

  auto start_key = internalSegmentGuardPrefixKey(metadata, ns_key, SegmentType::kCentroids);
  auto guard_key = internalSegmentGuardPrefixKey(metadata, ns_key, SegmentType::kGuardFlag);

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(guard_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(start_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options, cf_handle_);
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Centroid centroid;
    if (auto status = decodeCentroidFromKeyValue(iter->key(), iter->value(), &centroid); !status.ok()) {
      return status;
    }
    centroids->emplace_back(centroid);
  }

  if (clean_after_dump_batch != nullptr) {
    if (auto s = (*clean_after_dump_batch)->DeleteRange(cf_handle_, start_key, guard_key); !s.ok()) {
      return s;
    }
  }

  if (centroids->size() != metadata.merged_nodes) {
    return rocksdb::Status::Corruption(
        fmt::format("metadata has {} merged nodes, but got {}", metadata.merged_nodes, centroids->size()));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::applyNewCentroids(ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                           const std::string& ns_key, const TDigestMetadata& metadata,
                                           const std::vector<Centroid>& centroids) {
  for (const auto& c : centroids) {
    auto centroid_key = internalKeyFromCentroid(ns_key, metadata, c);
    auto centroid_payload = internalValueFromCentroid(c);
    if (auto s = batch->Put(cf_handle_, centroid_key, centroid_payload); !s.ok()) {
      return s;
    }
  }

  return rocksdb::Status::OK();
}

std::string TDigest::internalSegmentGuardPrefixKey(const TDigestMetadata& metadata, const std::string& ns_key,
                                                   SegmentType seg) {
  std::string prefix_key;
  PutFixed8(&prefix_key, static_cast<uint8_t>(seg));
  return InternalKey(ns_key, prefix_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}
}  // namespace redis