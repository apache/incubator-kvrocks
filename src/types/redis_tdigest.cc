#include "redis_tdigest.h"

#include "rocksdb/status.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {
using rocksdb::Status;

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