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

#pragma once

#include "bloom_filter.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

const uint32_t kBFDefaultInitCapacity = 100;
const double kBFDefaultErrorRate = 0.01;
const uint16_t kBFDefaultExpansion = 2;

enum class BloomInfoType {
  kAll,
  kCapacity,
  kSize,
  kFilters,
  kItems,
  kExpansion,
};

enum class BloomFilterAddResult {
  kOk,
  kExist,
  kFull,
};

struct BloomFilterInsertOptions {
  double error_rate = kBFDefaultErrorRate;
  uint32_t capacity = kBFDefaultInitCapacity;
  uint16_t expansion = kBFDefaultExpansion;
  bool auto_create = true;
};

struct BloomFilterInfo {
  uint32_t capacity;
  uint32_t bloom_bytes;
  uint16_t n_filters;
  uint64_t size;
  uint16_t expansion;
};

class BloomChain : public Database {
 public:
  BloomChain(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity, double error_rate,
                          uint16_t expansion);
  rocksdb::Status Add(engine::Context &ctx, const Slice &user_key, const std::string &item, BloomFilterAddResult *ret);
  rocksdb::Status MAdd(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                       std::vector<BloomFilterAddResult> *rets);
  rocksdb::Status InsertCommon(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                               const BloomFilterInsertOptions &insert_options, std::vector<BloomFilterAddResult> *rets);
  rocksdb::Status Exists(engine::Context &ctx, const Slice &user_key, const std::string &item, bool *exist);
  rocksdb::Status MExists(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                          std::vector<bool> *exists);
  rocksdb::Status Info(engine::Context &ctx, const Slice &user_key, BloomFilterInfo *info);

 private:
  rocksdb::Status getBloomChainMetadata(engine::Context &ctx, const Slice &ns_key, BloomChainMetadata *metadata);
  std::string getBFKey(const Slice &ns_key, const BloomChainMetadata &metadata, uint16_t filters_index);
  void getBFKeyList(const Slice &ns_key, const BloomChainMetadata &metadata, std::vector<std::string> *bf_key_list);
  rocksdb::Status getBFDataList(engine::Context &ctx, const std::vector<std::string> &bf_key_list,
                                std::vector<rocksdb::PinnableSlice> *bf_data_list);
  static void getItemHashList(const std::vector<std::string> &items, std::vector<uint64_t> *item_hash_list);

  rocksdb::Status createBloomChain(engine::Context &ctx, const Slice &ns_key, double error_rate, uint32_t capacity,
                                   uint16_t expansion, BloomChainMetadata *metadata);
  rocksdb::Status createBloomFilterInBatch(const Slice &ns_key, BloomChainMetadata *metadata,
                                           ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch, std::string *bf_data);

  /// bf_data: [in/out] The content string of bloomfilter.
  static void bloomAdd(uint64_t item_hash, std::string &bf_data);

  static bool bloomCheck(uint64_t item_hash, std::string_view &bf_data);
};
}  // namespace redis
