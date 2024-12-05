#include "redis_tdigest.h"

#include <algorithm>
#include <functional>
#include <iterator>
#include <memory>
#include <type_traits>

#include "db_util.h"
#include "encoding.h"
#include "fmt/format.h"
#include "rocksdb/db.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/tdigest.h"

namespace redis {

using rocksdb::Status;
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
//     StatusOr<Centroid> Centriod() const;
//   };

//   Iterator* Begin();
//   Iterator* End();
//   double TotalWeight();
//   double Min() const;
//   double Max() const;
// };

template <typename GetIteratorFunc, typename _RocksDBIterator = std::result_of_t<GetIteratorFunc()>>
struct DumpCentroids {
  explicit DumpCentroids(GetIteratorFunc get_iter_func, const TDigestMetadata* meta_data)
      : get_iterator_func(get_iter_func), meta_data(meta_data) {}
  struct Iterator {
    Iterator(GetIteratorFunc get_iterator_func, const rocksdb::Slice& key)
        : get_iterator(get_iterator_func), iter(get_iterator(key)) {}
    GetIteratorFunc get_iterator;
    // util::UniqueIterator iter;
    _RocksDBIterator iter;
    std::unique_ptr<Iterator> Clone() const { return std::make_unique<Iterator>(get_iterator, iter->key()); }
    bool Next() const {
      if (iter->Valid()) {
        iter->Next();
      }
      return iter->Valid();
    }

    bool Valid() const { return iter->Valid(); }
    StatusOr<Centroid> Centroid() const {
      if (!iter->Valid()) {
        return Status::InvalidArgument("invalid iterator during decoding tdigest centroid");
      }
      return DecodeCentroid(iter->key(), iter->value());
    }
  };

  std::unique_ptr<Iterator> Begin() {
    auto iter = GetIteratorFunc();
    return std::make_unique<Iterator>(get_iterator_func, iter);
  }

  std::unique_ptr<Iterator> End() {
    auto iter = GetIteratorFunc();
    iter->SeekToLast();
    return std::make_unique<Iterator>(get_iterator_func, iter);
  }

  double TotalWeight() const { return meta_data->total_weight; }

  double Min() const { return meta_data->minimum; }
  double Max() const { return meta_data->maximum; }

  GetIteratorFunc get_iterator_func;
  const TDigestMetadata* const meta_data;
};

uint64_t constexpr kMaxBufferSize = 1 * 1024;  // 1k doubles

std::optional<Status> TDigest::Create(engine::Context& context, const Slice& digest_name,
                                      const TDigestCreateOptions& options) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  auto capacity = options.compression * 6 + 10;
  capacity = ((capacity < kMaxBufferSize) ? capacity : kMaxBufferSize);
  TDigestMetadata metadata(options.compression, capacity);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  auto status = GetMetaData(context, ns_key, &metadata);
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
  status = batch->Put(cf_handle_, ns_key, metadata_bytes);
  if (!status.ok()) {
    return status;
  }

  return storage_->Write(context, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}
rocksdb::Status TDigest::Add(engine::Context& context, const Slice& digest_name, const std::vector<double>& inputs) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  TDigestMetadata metadata;
  auto status = GetMetaData(context, ns_key, &metadata);
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

  if (metadata.unmerged_nodes + inputs.size() <= metadata.capcacity) {
    status = appendBuffer(batch, ns_key, metadata, inputs);
    if (!status.ok()) {
      return status;
    }
    metadata.unmerged_nodes += inputs.size();
  } else {
    status = mergeCurrentBuffer(ns_key, batch, &metadata, &inputs);
    if (!status.ok()) {
      return status;
    }
  }

  std::string metadata_bytes;
  metadata.Encode(&metadata_bytes);
  status = batch->Put(cf_handle_, ns_key, metadata_bytes);
  if (!status.ok()) {
    return status;
  }

  return storage_->Write(context, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}
// rocksdb::Status TDigest::Cdf(engine::Context& context, const Slice& digest_name, const std::vector<double>& numbers,
//                              TDigestCDFResult* result) {}
rocksdb::Status TDigest::Quantile(engine::Context& context, const Slice& digest_name, const std::vector<double>& qs,
                                  TDigestQuantitleResult* result) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  TDigestMetadata metadata;
  {
    LockGuard guard(storage_->GetLockManager(), ns_key);

    auto status = GetMetaData(context, ns_key, &metadata);
    if (!status.ok()) {
      return status;
    }

    auto batch = storage_->GetWriteBatchBase();
    WriteBatchLogData log_data(kRedisTDigest);
    status = batch->PutLogData(log_data.Encode());
    if (!status.ok()) {
      return status;
    }

    status = mergeCurrentBuffer(ns_key, batch, &metadata);
    if (!status.ok()) {
      return status;
    }

    std::string metadata_bytes;
    metadata.Encode(&metadata_bytes);
    status = batch->Put(cf_handle_, ns_key, metadata_bytes);
    if (!status.ok()) {
      return status;
    }

    status = storage_->Write(context, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    if (!status.ok()) {
      return status;
    }

    context.RefreshLatestSnapshot();
  }

  // since we take a snapshot and read only, we don't need to lock again

  auto start_key = internalSegmentGuardPrefixKey(SegmentType::kCentroids, digest_name);
  auto guard_key = internalSegmentGuardPrefixKey(SegmentType::kGuardFlag, digest_name);

  rocksdb::ReadOptions read_options = context.DefaultScanOptions();
  rocksdb::Slice upper_bound(guard_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(start_key);
  read_options.iterate_lower_bound = &lower_bound;

  auto get_iterator_func = [this, &context, &read_options](const rocksdb::Slice& key = {}) {
    auto iter = util::UniqueIterator(context, read_options, cf_handle_);
    if (key.empty()) {
      iter->SeekToFirst();
    } else {
      iter->Seek(key);
    }
    return iter;
  };

  auto dump_centroids = DumpCentroids<decltype(get_iterator_func)>{get_iterator_func, &metadata};

  for (auto q : qs) {
    auto s = TDigestQuantile(dump_centroids, q, Lerp);
    if (!s) {
      return rocksdb::Status::InvalidArgument(s.Msg());
    }
    result->quantiles.push_back(*s);
  }

  return Status::OK();
}
// rocksdb::Status TDigest::Info(engine::Context& context, const Slice& digest_name, TDigestInfoResult* result) {}
// rocksdb::Status TDigest::Merge(engine::Context& context, const Slice& dest_digest_name,
//                                const std::vector<Slice>& sources, const TDigestMergeOptions& options) {}

rocksdb::Status TDigest::GetMetaData(engine::Context& context, const Slice& ns_key, TDigestMetadata* metadata) {
  return Database::GetMetadata(context, {kRedisTDigest}, ns_key, metadata);
}

rocksdb::Status TDigest::mergeCurrentBuffer(const std::string& ns_key,
                                            ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                            TDigestMetadata* metadata, const std::vector<double>* additional_buffer) {
  std::vector<Centroid> centroids;
  std::vector<double> buffer;
  centroids.reserve(metadata->merged_nodes);
  buffer.reserve(metadata->unmerged_nodes + (additional_buffer == nullptr ? 0 : additional_buffer->size()));
  auto status = dumpCentroidsAndBuffer(ns_key, *metadata, &centroids, &buffer);
  if (!status.ok()) {
    return status;
  }

  if (additional_buffer != nullptr) {
    std::copy(additional_buffer->begin(), additional_buffer->end(), std::back_inserter(buffer));
  }

  auto merged_centroids = TDigestMerge(buffer, {centroids});

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
  std::string sub_key = ns_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(SegmentType::kBuffer));
  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TDigest::internalKeyFromCentroid(const std::string& ns_key, const TDigestMetadata& metadata,
                                             const Centroid& centroid) const {
  std::string sub_key = ns_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(SegmentType::kCentroids));
  PutDouble(&sub_key, centroid.mean);  // It uses EncodeDoubleToUInt64 and keeps origonal order of double
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
                                      const std::string& ns_key, const TDigestMetadata& metadata,
                                      const std::vector<double>& inputs) {
  // TODO: implement it
  // must guard by lock
  auto buffer_key = internalBufferKey(ns_key, metadata);
  std::string buffer_value;
  auto s = storage_->Get(ctx, ctx.GetReadOptions(), cf_handle_, buffer_key, &buffer_value);
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }

  for (auto item : inputs) {
    PutDouble(&buffer_value, item);
  }
   // TODO: serialize and store

  return rocksdb::Status::NotSupported();
}

rocksdb::Status TDigest::dumpCentroidsAndBuffer(const std::string& ns_key, const TDigestMetadata& metadata,
                                                std::vector<Centroid>* centroids, std::vector<double>* buffer) {
  // TODO: implement it
  return rocksdb::Status::NotSupported();
}

rocksdb::Status TDigest::applyNewCentroidsAndCleanBuffer(ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                                         const std::string& ns_key, const TDigestMetadata& metadata,
                                                         const std::vector<Centroid>& centroids) {
  auto buffer_key = internalBufferKey(ns_key, metadata);
  if (auto s = batch->Delete(buffer_key); !s.ok() && !s.IsNotFound()) {
    return s;
  }

  for (const auto& c : centroids) {
    auto centroid_key = internalKeyFromCentroid(ns_key, metadata, c);
    auto centroid_payload = internalValueFromCentroid(c);
    if (auto s = batch->Put(centroid_key, centroid_payload); !s.ok()) {
      return s;
    }
  }

  return rocksdb::Status::OK();
}
}  // namespace redis