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

#include <random>
#include <string>
#include <vector>

#include "search/indexer.h"
#include "search/search_encoding.h"
#include "search/value.h"
#include "storage/storage.h"

namespace redis {

struct HnswIndex;

struct HnswNode {
  using NodeKey = std::string;
  NodeKey key;
  uint16_t level{};
  std::vector<NodeKey> neighbours;

  HnswNode(NodeKey key, uint16_t level);

  StatusOr<HnswNodeFieldMetadata> DecodeMetadata(engine::Context& ctx, const SearchKey& search_key) const;
  Status PutMetadata(HnswNodeFieldMetadata* node_meta, const SearchKey& search_key, engine::Storage* storage,
                     rocksdb::WriteBatchBase* batch) const;
  void DecodeNeighbours(engine::Context& ctx, const SearchKey& search_key);

  // For testing purpose
  Status AddNeighbour(engine::Context& ctx, const NodeKey& neighbour_key, const SearchKey& search_key,
                      rocksdb::WriteBatchBase* batch) const;
  Status RemoveNeighbour(engine::Context& ctx, const NodeKey& neighbour_key, const SearchKey& search_key,
                         rocksdb::WriteBatchBase* batch) const;
  friend struct HnswIndex;
};

struct VectorItem {
  using NodeKey = HnswNode::NodeKey;

  NodeKey key;
  kqir::NumericArray vector;
  const HnswVectorFieldMetadata* metadata;

  VectorItem() : metadata(nullptr) {}

  static Status Create(NodeKey key, const kqir::NumericArray& vector, const HnswVectorFieldMetadata* metadata,
                       VectorItem* out);
  static Status Create(NodeKey key, kqir::NumericArray&& vector, const HnswVectorFieldMetadata* metadata,
                       VectorItem* out);

  bool operator==(const VectorItem& other) const;
  bool operator<(const VectorItem& other) const;

 private:
  VectorItem(NodeKey&& key, const kqir::NumericArray& vector, const HnswVectorFieldMetadata* metadata);
  VectorItem(NodeKey&& key, kqir::NumericArray&& vector, const HnswVectorFieldMetadata* metadata);
};

StatusOr<double> ComputeSimilarity(const VectorItem& left, const VectorItem& right);

using VectorItemWithDistance = std::pair<double, VectorItem>;
using KeyWithDistance = std::pair<double, std::string>;

// TODO(Beihao): Add DB context to improve consistency and isolation - see #2332
struct HnswIndex {
  using NodeKey = HnswNode::NodeKey;

  SearchKey search_key;
  HnswVectorFieldMetadata* metadata;
  engine::Storage* storage = nullptr;

  std::mt19937 generator;
  double m_level_normalization_factor;

  HnswIndex(const SearchKey& search_key, HnswVectorFieldMetadata* vector, engine::Storage* storage,
            std::random_device::result_type seed = std::random_device()());

  static StatusOr<std::vector<VectorItem>> DecodeNodesToVectorItems(engine::Context& ctx,
                                                                    const std::vector<NodeKey>& node_key,
                                                                    uint16_t level, const SearchKey& search_key,
                                                                    const HnswVectorFieldMetadata* metadata);
  uint16_t RandomizeLayer();
  StatusOr<NodeKey> DefaultEntryPoint(engine::Context& ctx, uint16_t level) const;
  Status AddEdge(const NodeKey& node_key1, const NodeKey& node_key2, uint16_t layer,
                 ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const;
  Status RemoveEdge(const NodeKey& node_key1, const NodeKey& node_key2, uint16_t layer,
                    ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const;

  StatusOr<std::vector<VectorItem>> SelectNeighbors(const VectorItem& vec, const std::vector<VectorItem>& vectors,
                                                    uint16_t layer) const;
  StatusOr<std::vector<VectorItemWithDistance>> SearchLayerInternal(engine::Context& ctx, uint16_t level,
                                                                    const VectorItem& target_vector,
                                                                    uint32_t ef_runtime,
                                                                    const std::vector<NodeKey>& entry_points) const;
  StatusOr<std::vector<VectorItem>> SearchLayer(engine::Context& ctx, uint16_t level, const VectorItem& target_vector,
                                                uint32_t ef_runtime, const std::vector<NodeKey>& entry_points) const;
  Status InsertVectorEntryInternal(engine::Context& ctx, std::string_view key, const kqir::NumericArray& vector,
                                   ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch, uint16_t layer) const;
  Status InsertVectorEntry(engine::Context& ctx, std::string_view key, const kqir::NumericArray& vector,
                           ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch);
  Status DeleteVectorEntry(engine::Context& ctx, std::string_view key,
                           ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const;
  StatusOr<std::vector<KeyWithDistance>> KnnSearch(engine::Context& ctx, const kqir::NumericArray& query_vector,
                                                   uint32_t k) const;
  StatusOr<std::vector<KeyWithDistance>> ExpandSearchScope(engine::Context& ctx, const kqir::NumericArray& query_vector,
                                                           std::vector<redis::KeyWithDistance>&& initial_keys,
                                                           std::unordered_set<std::string>& visited) const;
};

}  // namespace redis