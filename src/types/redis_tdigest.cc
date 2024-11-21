#include "redis_tdigest.h"

#include <functional>
#include <iterator>
#include <memory>

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

StatusOr<Centroid> DecodeCentroid(const rocksdb::Slice& data) {
  // TODO: implement it
  return nullptr;
}

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

template <typename GetIteratorFunc>
struct DumpCentroids {
  struct Iterator {
    Iterator(GetIteratorFunc get_iterator_func, const rocksdb::Slice& key)
        : get_iterator(get_iterator_func), iter(get_iterator(key)) {}
    GetIteratorFunc get_iterator;
    rocksdb::Iterator* iter;
    std::unique_ptr<Iterator> Clone() const { return std::make_unique<Iterator>(get_iterator, iter->key()); }
    bool Next() const {
      if (iter->Valid()) {
        iter->Next();
      }
      return iter->Valid();
    }

    bool Valid() const {
      return iter->Valid();
    }
    StatusOr<Centroid> Centroid() const {
      if (!iter->Valid()) {
        return Status::InvalidArgument("invalid iterator during decoding tdigest centroid");
      }
      return DecodeCentroid(iter->value());
    }
  };

  std::unique_ptr<Iterator> Begin() {
    return GetIteratorFunc(lower_bound);
  }

  const rocksdb::Slice upper_bound;
  const rocksdb::Slice lower_bound;
};

uint64_t constexpr kMaxBufferSize = 1 * 1024;  // 1k doubles

std::optional<Status> TDigest::Create(engine::Context& context, const Slice& digest_name,
                                      const TDigestCreateOptions& options) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  TDigestMetadata metadata;
  auto status = GetMetaData(context, ns_key, &metadata);
  if (status.ok()) {
    return {};
  }

  if (!status.IsNotFound()) {
    return status;
  }

  metadata.compression = options.compression;
  auto capactity = options.compression * 6 + 10;
  metadata.capcacity = (capactity < kMaxBufferSize) ? capactity : kMaxBufferSize;

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
    status = appendBuffer(context, batch, ns_key, inputs);
    if (!status.ok()) {
      return status;
    }
    metadata.unmerged_nodes += inputs.size();
  } else {
    std::vector<Centroid> centroids;
    std::vector<double> buffer;
    centroids.reserve(metadata.merged_nodes);
    buffer.reserve(metadata.unmerged_nodes + inputs.size());
    status = dumpCentroidsAndBuffer(context, ns_key, &centroids, &buffer);
    if (!status.ok()) {
      return status;
    }

    std::copy(inputs.begin(), inputs.end(), std::back_inserter(buffer));

    auto merged_centroids = TDigestMerge(buffer, {centroids});

    status = applyNewCentroidsAndCleanBuffer(context, batch, ns_key, merged_centroids->centroids);
    if (!status.ok()) {
      return status;
    }

    metadata.merge_times++;
    metadata.merged_nodes = merged_centroids->centroids.size();
    metadata.unmerged_nodes = 0;
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
// rocksdb::Status TDigest::Quantile(engine::Context& context, const Slice& digest_name,
//                                   const std::vector<double>& numbers, TDigestQuantitleResult* result) {}
// rocksdb::Status TDigest::Info(engine::Context& context, const Slice& digest_name, TDigestInfoResult* result) {}
// rocksdb::Status TDigest::Merge(engine::Context& context, const Slice& dest_digest_name,
//                                const std::vector<Slice>& sources, const TDigestMergeOptions& options) {}

rocksdb::Status TDigest::GetMetaData(engine::Context& context, const Slice& ns_key, TDigestMetadata* metadata) {
  return Database::GetMetadata(context, {kRedisTDigest}, ns_key, metadata);
}

rocksdb::Status TDigest::Quantile(engine::Context& context, const Slice& digest_name,
                                  const std::vector<double>& numbers, TDigestQuantitleResult* result) {}

}  // namespace redis