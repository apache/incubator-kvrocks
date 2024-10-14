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

#include "redis_bloom_chain.h"

#include "types/bloom_filter.h"

namespace redis {

rocksdb::Status BloomChain::getBloomChainMetadata(engine::Context &ctx, const Slice &ns_key,
                                                  BloomChainMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisBloomFilter}, ns_key, metadata);
}

std::string BloomChain::getBFKey(const Slice &ns_key, const BloomChainMetadata &metadata, uint16_t filters_index) {
  std::string sub_key;
  PutFixed16(&sub_key, filters_index);
  std::string bf_key = InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  return bf_key;
}

void BloomChain::getBFKeyList(const Slice &ns_key, const BloomChainMetadata &metadata,
                              std::vector<std::string> *bf_key_list) {
  bf_key_list->reserve(metadata.n_filters);
  for (uint16_t i = 0; i < metadata.n_filters; ++i) {
    std::string bf_key = getBFKey(ns_key, metadata, i);
    bf_key_list->push_back(std::move(bf_key));
  }
}

rocksdb::Status BloomChain::getBFDataList(engine::Context &ctx, const std::vector<std::string> &bf_key_list,
                                          std::vector<rocksdb::PinnableSlice> *bf_data_list) {
  bf_data_list->reserve(bf_key_list.size());
  for (const auto &bf_key : bf_key_list) {
    rocksdb::PinnableSlice pin_value;
    rocksdb::Status s = storage_->Get(ctx, ctx.GetReadOptions(), bf_key, &pin_value);
    if (!s.ok()) return s;
    bf_data_list->push_back(std::move(pin_value));
  }
  return rocksdb::Status::OK();
}

void BloomChain::getItemHashList(const std::vector<std::string> &items, std::vector<uint64_t> *item_hash_list) {
  item_hash_list->reserve(items.size());
  for (const auto &item : items) {
    item_hash_list->push_back(BlockSplitBloomFilter::Hash(item.data(), item.size()));
  }
}

rocksdb::Status BloomChain::createBloomChain(engine::Context &ctx, const Slice &ns_key, double error_rate,
                                             uint32_t capacity, uint16_t expansion, BloomChainMetadata *metadata) {
  metadata->n_filters = 1;
  metadata->expansion = expansion;
  metadata->size = 0;

  metadata->error_rate = error_rate;
  metadata->base_capacity = capacity;
  metadata->bloom_bytes = BlockSplitBloomFilter::OptimalNumOfBytes(capacity, error_rate);

  auto [block_split_bloom_filter, _] = CreateBlockSplitBloomFilter(metadata->bloom_bytes);

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"createBloomChain"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  std::string bloom_chain_meta_bytes;
  metadata->Encode(&bloom_chain_meta_bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bloom_chain_meta_bytes);
  if (!s.ok()) return s;

  std::string bf_key = getBFKey(ns_key, *metadata, metadata->n_filters - 1);
  s = batch->Put(bf_key, block_split_bloom_filter.GetData());
  if (!s.ok()) return s;

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BloomChain::createBloomFilterInBatch(const Slice &ns_key, BloomChainMetadata *metadata,
                                                     ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                     std::string *bf_data) {
  uint32_t bloom_filter_bytes = BlockSplitBloomFilter::OptimalNumOfBytes(
      static_cast<uint32_t>(metadata->base_capacity * pow(metadata->expansion, metadata->n_filters)),
      metadata->error_rate);
  metadata->n_filters += 1;
  metadata->bloom_bytes += bloom_filter_bytes;

  std::tie(std::ignore, *bf_data) = CreateBlockSplitBloomFilter(bloom_filter_bytes);

  std::string bloom_chain_meta_bytes;
  metadata->Encode(&bloom_chain_meta_bytes);
  auto s = batch->Put(metadata_cf_handle_, ns_key, bloom_chain_meta_bytes);
  if (!s.ok()) return s;
  return rocksdb::Status::OK();
}

void BloomChain::bloomAdd(uint64_t item_hash, std::string &bf_data) {
  BlockSplitBloomFilter block_split_bloom_filter(bf_data);
  block_split_bloom_filter.InsertHash(item_hash);
}

bool BloomChain::bloomCheck(uint64_t item_hash, std::string_view &bf_data) {
  const BlockSplitBloomFilter block_split_bloom_filter(
      nonstd::span<char>(const_cast<char *>(bf_data.data()), bf_data.size()));
  return block_split_bloom_filter.FindHash(item_hash);
}

rocksdb::Status BloomChain::Reserve(engine::Context &ctx, const Slice &user_key, uint32_t capacity, double error_rate,
                                    uint16_t expansion) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  LockGuard guard(storage_->GetLockManager(), ns_key);
  BloomChainMetadata bloom_chain_metadata;
  rocksdb::Status s = getBloomChainMetadata(ctx, ns_key, &bloom_chain_metadata);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (!s.IsNotFound()) {
    return rocksdb::Status::InvalidArgument("the key already exists");
  }

  return createBloomChain(ctx, ns_key, error_rate, capacity, expansion, &bloom_chain_metadata);
}

rocksdb::Status BloomChain::Add(engine::Context &ctx, const Slice &user_key, const std::string &item,
                                BloomFilterAddResult *ret) {
  std::vector<BloomFilterAddResult> tmp{BloomFilterAddResult::kOk};
  rocksdb::Status s = MAdd(ctx, user_key, {item}, &tmp);
  *ret = tmp[0];
  return s;
}

rocksdb::Status BloomChain::MAdd(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                                 std::vector<BloomFilterAddResult> *rets) {
  BloomFilterInsertOptions insert_options;
  return InsertCommon(ctx, user_key, items, insert_options, rets);
}

rocksdb::Status BloomChain::InsertCommon(engine::Context &ctx, const Slice &user_key,
                                         const std::vector<std::string> &items,
                                         const BloomFilterInsertOptions &insert_options,
                                         std::vector<BloomFilterAddResult> *rets) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  LockGuard guard(storage_->GetLockManager(), ns_key);

  BloomChainMetadata metadata;
  rocksdb::Status s = getBloomChainMetadata(ctx, ns_key, &metadata);

  if (s.IsNotFound() && insert_options.auto_create) {
    s = createBloomChain(ctx, ns_key, insert_options.error_rate, insert_options.capacity, insert_options.expansion,
                         &metadata);
  }
  if (!s.ok()) return s;

  std::vector<std::string> bf_key_list;
  getBFKeyList(ns_key, metadata, &bf_key_list);

  std::vector<rocksdb::PinnableSlice> bf_data_list;
  s = getBFDataList(ctx, bf_key_list, &bf_data_list);
  if (!s.ok()) return s;

  std::vector<uint64_t> item_hash_list;
  getItemHashList(items, &item_hash_list);

  uint64_t origin_size = metadata.size;
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisBloomFilter, {"insert"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  for (size_t i = 0; i < items.size(); ++i) {
    // check
    bool exist = false;
    // TODO: to test which direction for searching is better
    for (int ii = static_cast<int>(bf_data_list.size()) - 1; ii >= 0; --ii) {
      std::string_view data = bf_data_list[ii].ToStringView();
      exist = bloomCheck(item_hash_list[i], data);
      if (exist) break;
    }

    // insert
    if (exist) {
      (*rets)[i] = BloomFilterAddResult::kExist;
    } else {
      if (metadata.size + 1 > metadata.GetCapacity()) {
        if (metadata.IsScaling()) {
          s = batch->Put(bf_key_list.back(), bf_data_list.back().ToStringView());
          if (!s.ok()) return s;
          std::string bf_data;
          s = createBloomFilterInBatch(ns_key, &metadata, batch, &bf_data);
          if (!s.ok()) return s;
          rocksdb::PinnableSlice pin_slice;
          *pin_slice.GetSelf() = std::move(bf_data);
          pin_slice.PinSelf();
          bf_data_list.push_back(std::move(pin_slice));
          bf_key_list.push_back(getBFKey(ns_key, metadata, metadata.n_filters - 1));
        } else {
          (*rets)[i] = BloomFilterAddResult::kFull;
          continue;
        }
      }
      std::string data = bf_data_list.back().ToString();
      bloomAdd(item_hash_list[i], data);
      *bf_data_list.back().GetSelf() = std::move(data);
      bf_data_list.back().PinSelf();
      (*rets)[i] = BloomFilterAddResult::kOk;
      metadata.size += 1;
    }
  }

  if (metadata.size != origin_size) {
    std::string bloom_chain_metadata_bytes;
    metadata.Encode(&bloom_chain_metadata_bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bloom_chain_metadata_bytes);
    if (!s.ok()) return s;
    s = batch->Put(bf_key_list.back(), bf_data_list.back().ToStringView());
    if (!s.ok()) return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BloomChain::Exists(engine::Context &ctx, const Slice &user_key, const std::string &item, bool *exist) {
  std::vector<bool> tmp{false};
  rocksdb::Status s = MExists(ctx, user_key, {item}, &tmp);
  *exist = tmp[0];
  return s;
}

rocksdb::Status BloomChain::MExists(engine::Context &ctx, const Slice &user_key, const std::vector<std::string> &items,
                                    std::vector<bool> *exists) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  BloomChainMetadata metadata;
  rocksdb::Status s = getBloomChainMetadata(ctx, ns_key, &metadata);
  if (s.IsNotFound()) {
    std::fill(exists->begin(), exists->end(), false);
    return rocksdb::Status::OK();
  }
  if (!s.ok()) return s;

  std::vector<std::string> bf_key_list;
  getBFKeyList(ns_key, metadata, &bf_key_list);

  std::vector<rocksdb::PinnableSlice> bf_data_list;
  s = getBFDataList(ctx, bf_key_list, &bf_data_list);
  if (!s.ok()) return s;

  std::vector<uint64_t> item_hash_list;
  getItemHashList(items, &item_hash_list);

  for (size_t i = 0; i < items.size(); ++i) {
    // check
    // TODO: to test which direction for searching is better
    for (int ii = static_cast<int>(bf_data_list.size()) - 1; ii >= 0; --ii) {
      std::string_view data = bf_data_list[ii].ToStringView();
      (*exists)[i] = bloomCheck(item_hash_list[i], data);
      if ((*exists)[i]) break;
    }
  }

  return rocksdb::Status::OK();
}

rocksdb::Status BloomChain::Info(engine::Context &ctx, const Slice &user_key, BloomFilterInfo *info) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  BloomChainMetadata metadata;
  rocksdb::Status s = getBloomChainMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  info->capacity = metadata.GetCapacity();
  info->bloom_bytes = metadata.bloom_bytes;
  info->n_filters = metadata.n_filters;
  info->size = metadata.size;
  info->expansion = metadata.expansion;

  return rocksdb::Status::OK();
}

}  // namespace redis
