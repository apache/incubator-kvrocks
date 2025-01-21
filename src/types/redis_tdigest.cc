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

#include <algorithm>
#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <range/v3/range/conversion.hpp>
#include <range/v3/view/join.hpp>
#include <range/v3/view/transform.hpp>
#include <type_traits>
#include <vector>

#include "db_util.h"
#include "encoding.h"
#include "fmt/format.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/tdigest.h"

namespace {
// Helper function to convert rocksdb::Slice to hex string
std::string SliceToHex(const rocksdb::Slice& slice) {
  std::ostringstream oss;
  for (size_t i = 0; i < slice.size(); ++i) {
    oss << std::hex << std::setw(2) << std::setfill('0') << (int)(unsigned char)slice[i];
  }
  return oss.str();
}
}  // namespace

namespace redis {
namespace {
// a numerically stable lerp is unbelievably complex
// but we are *approximating* the quantile, so let's keep it simple
// reference:
// https://github.com/apache/arrow/blob/27bbd593625122a4a25d9471c8aaf5df54a6dcf9/cpp/src/arrow/util/tdigest.cc#L38
double Lerp(double a, double b, double t) { return a + t * (b - a); }
}  // namespace

// class TDSample {
//   public:
//   struct Iterator {
//     Iterator* Clone() const;
//     bool Next();
//     bool Valid() const;
//     StatusOr<Centroid> Centroid() const;
//   };

//   Iterator* Begin();
//   Iterator* End();
//   double TotalWeight();
//   double Min() const;
//   double Max() const;
// };

// using GetIteratorFunc = std::function<util::UniqueIterator(const rocksdb::Slice& key)>;
// using DecodeCentroidFunc = std::function<StatusOr<Centroid>(const rocksdb::Slice& key, const rocksdb::Slice& value)>;

template <typename GetIteratorFunc, typename DecodeCentroidFunc,
          typename _RocksDBIterator = std::result_of_t<GetIteratorFunc(const rocksdb::Slice&)>>
struct DumpCentroids {
  using Status = rocksdb::Status;
  explicit DumpCentroids(GetIteratorFunc get_iter_func, DecodeCentroidFunc decode_centroid_func,
                         const TDigestMetadata* meta_data)
      : get_iterator_func(get_iter_func), decode_centroid_func(decode_centroid_func), meta_data(meta_data) {}
  struct Iterator {
    Iterator(GetIteratorFunc get_iterator_func, DecodeCentroidFunc decode_centroid_func, const rocksdb::Slice& key)
        : get_iterator(get_iterator_func), decode_centroid_func(decode_centroid_func), iter(get_iterator(key)) {}
    GetIteratorFunc get_iterator;
    DecodeCentroidFunc decode_centroid_func;
    // util::UniqueIterator iter;
    _RocksDBIterator iter;
    std::unique_ptr<Iterator> Clone() const {
      return std::make_unique<Iterator>(get_iterator, decode_centroid_func, iter->key());
    }
    bool Next() const {
      if (iter->Valid()) {
        iter->Next();
      }
      return iter->Valid();
    }

    bool Prev() const {
      iter->Prev();
      return iter->Valid();
    }

    bool Valid() const { return iter->Valid(); }
    StatusOr<Centroid> GetCentroid() const {
      if (!iter->Valid()) {
        return {::Status::NotOK, "invalid iterator during decoding tdigest centroid"};
        // return Status::InvalidArgument("invalid iterator during decoding tdigest centroid");
      }
      return decode_centroid_func(iter->key(), iter->value());
    }
  };

  std::unique_ptr<Iterator> Begin() {
    auto iter = get_iterator_func({});
    return std::make_unique<Iterator>(get_iterator_func, decode_centroid_func, iter->key());
  }

  std::unique_ptr<Iterator> End() {
    auto iter = get_iterator_func({});
    iter->SeekToLast();
    return std::make_unique<Iterator>(get_iterator_func, decode_centroid_func, iter->key());
  }

  double TotalWeight() const { return meta_data->total_weight; }

  double Min() const { return meta_data->minimum; }
  double Max() const { return meta_data->maximum; }

  uint64_t Size() const { return meta_data->merged_nodes; }

  GetIteratorFunc get_iterator_func;
  DecodeCentroidFunc decode_centroid_func;
  const TDigestMetadata* const meta_data;
};

// It should be replaced by a iteration of the rocksdb iterator
class DummyCentroids {
 public:
  DummyCentroids(const TDigestMetadata& meta_data, const std::vector<Centroid>& centroids)
      : meta_data_(meta_data), centroids_(centroids) {}
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

  if (metadata.merge_times == 0 && metadata.merged_nodes == 0 && !inputs.empty()) {
    metadata.minimum = inputs.front();
    metadata.maximum = inputs.back();
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
// rocksdb::Status TDigest::Cdf(engine::Context& context, const Slice& digest_name, const std::vector<double>& numbers,
//                              TDigestCDFResult* result) {}
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

  // since we take a snapshot and read only, we don't need to lock again

  // auto start_key = internalSegmentGuardPrefixKey(ns_key, SegmentType::kCentroids);
  // auto guard_key = internalSegmentGuardPrefixKey(ns_key, SegmentType::kGuardFlag);

  // rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  // rocksdb::Slice upper_bound(guard_key);
  // read_options.iterate_upper_bound = &upper_bound;
  // rocksdb::Slice lower_bound(start_key);
  // read_options.iterate_lower_bound = &lower_bound;

  // auto get_iterator_func = [this, &ctx, &read_options](const rocksdb::Slice& key = {}) {
  //   auto iter = util::UniqueIterator(ctx, read_options, cf_handle_);
  //   if (key.empty()) {
  //     iter->SeekToFirst();
  //   } else {
  //     iter->Seek(key);
  //   }
  //   return iter;
  // };

  // auto decode_centroid_func = [this](const rocksdb::Slice& key, const rocksdb::Slice& value) -> StatusOr<Centroid> {
  //   Centroid centroid;
  //   auto s = decodeCentroidFromKeyValue(key, value, &centroid);
  //   if (!s.ok()) {
  //     return Status(::Status::InvalidArgument, s.ToString());
  //   }
  //   return std::move(centroid);
  // };

  // auto dump_centroids = DumpCentroids{get_iterator_func, decode_centroid_func, &metadata};

  std::vector<Centroid> centroids;
  if (auto status = dumpCentroidsAndBuffer(ctx, ns_key, metadata, &centroids); !status.ok()) {
    return status;
  }

  auto dump_centroids = DummyCentroids(metadata, centroids);

  LOG(INFO) << "metadata min: " << metadata.minimum << ", max: " << metadata.maximum
            << ", total weight: " << metadata.total_weight << ", merged nodes: " << metadata.merged_nodes
            << ", unmerged nodes: " << metadata.unmerged_nodes;

  for (auto q : qs) {
    auto s = TDigestQuantile(dump_centroids, q, Lerp);
    if (!s) {
      return rocksdb::Status::InvalidArgument(s.Msg());
    }
    result->quantiles.push_back(*s);
  }

  return rocksdb::Status::OK();
}
// rocksdb::Status TDigest::Info(engine::Context& context, const Slice& digest_name, TDigestInfoResult* result) {}
// rocksdb::Status TDigest::Merge(engine::Context& context, const Slice& dest_digest_name,
//                                const std::vector<Slice>& sources, const TDigestMergeOptions& options) {}

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
  auto status = dumpCentroidsAndBuffer(ctx, ns_key, *metadata, &centroids, &buffer);
  if (!status.ok()) {
    return status;
  }

  if (additional_buffer == nullptr || additional_buffer->empty()) {
    return rocksdb::Status::OK();
  }

  if (additional_buffer != nullptr) {
    std::copy(additional_buffer->cbegin(), additional_buffer->cend(), std::back_inserter(buffer));
  }

  auto merged_centroids = TDigestMerge(buffer, {centroids, metadata->compression});

  if (!merged_centroids.IsOK()) {
    return rocksdb::Status::InvalidArgument(merged_centroids.Msg());
  }

  LOG(INFO) << "merged centroids size: " << merged_centroids->centroids.size() << ", data: "
            << (merged_centroids->centroids | ranges::views::transform([](const auto& c) {
                  return fmt::format("mean: {}, weight: {}", c.mean, c.weight);
                }) |
                ranges::views::join(' ') | ranges::to<std::string>());

  status = applyNewCentroidsAndCleanBuffer(batch, ns_key, *metadata, merged_centroids->centroids);
  if (!status.ok()) {
    return status;
  }

  metadata->merge_times++;
  metadata->merged_nodes = merged_centroids->centroids.size();
  metadata->unmerged_nodes = 0;

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
  // LOG(INFO) << "decode subkey: " << SliceToHex(subkey);
  auto type_flg = static_cast<uint8_t>(SegmentType::kGuardFlag);
  GetFixed8(&subkey, &type_flg);
  if (static_cast<SegmentType>(type_flg) != SegmentType::kCentroids) {
    // LOG(WARNING) << "corrupted tdigest centroid type flag: " << type_flg << ",  key: " << SliceToHex(key);
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

  if (s.IsNotFound() && metadata->merge_times == 0 && !inputs.empty()) {
    metadata->minimum = inputs.front();
    metadata->maximum = inputs.front();
  }

  for (auto item : inputs) {
    if (item > metadata->maximum) {
      metadata->maximum = item;
    }
    if (item < metadata->minimum) {
      metadata->minimum = item;
    }
    PutDouble(&buffer_value, item);
  }

  if (s = batch->Put(cf_handle_, buffer_key, buffer_value); !s.ok()) {
    return s;
  }

  return s;
}

rocksdb::Status TDigest::dumpCentroidsAndBuffer(engine::Context& ctx, const std::string& ns_key,
                                                const TDigestMetadata& metadata, std::vector<Centroid>* centroids,
                                                std::vector<double>* buffer) {
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

  // LOG(INFO) << "scan lower bound: " << SliceToHex(lower_bound) << ", upper bound: " << SliceToHex(upper_bound);

  auto iter = util::UniqueIterator(ctx, read_options, cf_handle_);
  uint64_t total_count = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ++total_count;
    Centroid centroid;
    if (auto status = decodeCentroidFromKeyValue(iter->key(), iter->value(), &centroid); !status.ok()) {
      return status;
    }
    centroids->emplace_back(centroid);
  }
  LOG(INFO) << "read centroids total count: " << total_count;

  if (total_count != metadata.merged_nodes) {
    return rocksdb::Status::Corruption(
        fmt::format("metadata has {} merged nodes, but got {}", metadata.merged_nodes, total_count));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TDigest::applyNewCentroidsAndCleanBuffer(ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                                         const std::string& ns_key, const TDigestMetadata& metadata,
                                                         const std::vector<Centroid>& centroids) {
  auto buffer_key = internalBufferKey(ns_key, metadata);
  if (auto s = batch->Delete(cf_handle_, buffer_key); !s.ok() && !s.IsNotFound()) {
    return s;
  }

  for (const auto& c : centroids) {
    auto centroid_key = internalKeyFromCentroid(ns_key, metadata, c);
    auto centroid_payload = internalValueFromCentroid(c);
    // LOG(INFO) << "store centroid with key: " << SliceToHex(centroid_key) << ", value: " <<
    // SliceToHex(centroid_payload);
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