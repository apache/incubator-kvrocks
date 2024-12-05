#pragma once

#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "tdigest.h"

namespace redis {
// struct Centroid {
//   double mean;
//   double weight = 1.0;

//   Centroid& operator+(const Centroid& _) {
//     // TODO: implement this
//     return *this;
//   }
// };

struct CentroidWithKey {
  Centroid centroid;
  rocksdb::Slice key;
};

struct TDigestCreateOptions {
  uint64_t compression;
};

struct TDigestMergeOptions {};

struct TDigestCDFResult {};

struct TDigestQuantitleResult {
  std::vector<double> quantiles;
};

struct TDigestInfoResult {};

class TDigest : public SubKeyScanner {
 public:
  using Slice = rocksdb::Slice;
  explicit TDigest(engine::Storage* storage, const std::string& ns)
      : SubKeyScanner(storage, ns), cf_handle_(storage->GetCFHandle(ColumnFamilyID::TDigest)) {}
  std::optional<rocksdb::Status> Create(engine::Context& context, const Slice& digest_name,
                                        const TDigestCreateOptions& options);
  rocksdb::Status Add(engine::Context& context, const Slice& digest_name, const std::vector<double>& inputs);
  // rocksdb::Status Cdf(engine::Context& context, const Slice& digest_name, const std::vector<double>& numbers,
  //                     TDigestCDFResult* result);
  rocksdb::Status Quantile(engine::Context& context, const Slice& digest_name, const std::vector<double>& qs,
                           TDigestQuantitleResult* result);
  // rocksdb::Status Info(engine::Context& context, const Slice& digest_name, TDigestInfoResult* result);
  // rocksdb::Status Merge(engine::Context& context, const Slice& dest_digest_name, const std::vector<Slice>& sources,
  //                       const TDigestMergeOptions& options);

  rocksdb::Status GetMetaData(engine::Context& context, const Slice& digest_name, TDigestMetadata* metadata);

 private:
  enum class SegmentType : uint8_t { kBuffer = 0, kCentroids = 1, kGuardFlag = 0xFF };

  rocksdb::ColumnFamilyHandle* cf_handle_;

  rocksdb::Status appendBuffer(engine::Context &ctx, ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                               const std::string& ns_key, const TDigestMetadata& metadata, const std::vector<double>& inputs);

  rocksdb::Status dumpCentroidsAndBuffer( const std::string& ns_key, const TDigestMetadata& metadata,
                                         std::vector<Centroid>* centroids, std::vector<double>* buffer);
  rocksdb::Status applyNewCentroidsAndCleanBuffer(
                                                  ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                                  const std::string& ns_key, const TDigestMetadata& metadata, const std::vector<Centroid>& centroids);

  std::string internalSegmentGuardPrefixKey(SegmentType seg, const Slice& digest_name) const;

  rocksdb::Status mergeCurrentBuffer(const std::string& ns_key,
                                      ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch, TDigestMetadata* metadata,
                                      const std::vector<double>* additional_buffer = nullptr);
  std::string internalBufferKey(const std::string& ns_key,  const TDigestMetadata& metadata) const;
  std::string internalKeyFromCentroid(const std::string& ns_key, const TDigestMetadata& metadata, const Centroid& centroid) const;
  static std::string internalValueFromCentroid(const Centroid& centroid);
  rocksdb::Status decodeCentroidFromKeyValue(const rocksdb::Slice& key, const rocksdb::Slice& value, Centroid* centroid) const;
};

}  // namespace redis