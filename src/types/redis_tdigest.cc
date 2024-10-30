#include "redis_tdigest.h"

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {
rocksdb::Status TDigest::Create(engine::Context& context, const Slice& digest_name,
                                const TDigestCreateOptions& options) {
  // TODO: implement it
  return {};
}
// rocksdb::Status TDigest::Add(engine::Context& context, const Slice& digest_name,
//                              const std::vector<Centroid>& centroids) {}
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

}  // namespace redis