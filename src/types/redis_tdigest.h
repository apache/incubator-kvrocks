#pragma once

#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"

namespace redis {
struct Centroid {
  double mean;
  double weight = 1.0;

  Centroid& operator+(const Centroid& _) {
    // TODO: implement this
    return *this;
  }
};

struct CentroidWithKey {
  Centroid centroid;
  rocksdb::Slice key;
};

struct TDigestCreateOptions {};

struct TDigestMergeOptions {};

struct TDigestCDFResult {};

struct TDigestQuantitleResult {};

struct TDigestInfoResult {};

class TDigest : public SubKeyScanner {
 public:
  using Slice = rocksdb::Slice;
  explicit TDigest(engine::Storage* storage, const std::string& ns)
      : SubKeyScanner(storage, ns), cf_handle_(storage->GetCFHandle(ColumnFamilyID::TDigest)) {}
  rocksdb::Status Create(engine::Context& context, const Slice& digest_name, const TDigestCreateOptions& options);
  // rocksdb::Status Add(engine::Context& context, const Slice& digest_name, const std::vector<Centroid>& centroids);
  // rocksdb::Status Cdf(engine::Context& context, const Slice& digest_name, const std::vector<double>& numbers,
  //                     TDigestCDFResult* result);
  // rocksdb::Status Quantile(engine::Context& context, const Slice& digest_name, const std::vector<double>& numbers,
  //                          TDigestQuantitleResult* result);
  // rocksdb::Status Info(engine::Context& context, const Slice& digest_name, TDigestInfoResult* result);
  // rocksdb::Status Merge(engine::Context& context, const Slice& dest_digest_name, const std::vector<Slice>& sources,
  //                       const TDigestMergeOptions& options);

  rocksdb::Status GetMetaData(engine::Context& context, const Slice& digest_name, TDigestMetadata* metadata);
 private:
  rocksdb::ColumnFamilyHandle* cf_handle_;
};

}  // namespace redis