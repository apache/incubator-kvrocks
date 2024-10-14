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

#include "hnsw_indexer.h"

#include <fmt/core.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <queue>
#include <random>
#include <unordered_set>
#include <vector>

#include "db_util.h"

namespace redis {

HnswNode::HnswNode(NodeKey key, uint16_t level) : key(std::move(key)), level(level) {}

StatusOr<HnswNodeFieldMetadata> HnswNode::DecodeMetadata(engine::Context& ctx, const SearchKey& search_key) const {
  auto node_index_key = search_key.ConstructHnswNode(level, key);
  rocksdb::PinnableSlice value;
  auto s = ctx.storage->Get(ctx, ctx.GetReadOptions(), ctx.storage->GetCFHandle(ColumnFamilyID::Search), node_index_key,
                            &value);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  HnswNodeFieldMetadata metadata;
  s = metadata.Decode(&value);
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return metadata;
}

Status HnswNode::PutMetadata(HnswNodeFieldMetadata* node_meta, const SearchKey& search_key, engine::Storage* storage,
                             rocksdb::WriteBatchBase* batch) const {
  std::string updated_metadata;
  node_meta->Encode(&updated_metadata);
  auto s = batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), search_key.ConstructHnswNode(level, key),
                      updated_metadata);
  if (!s.ok()) {
    return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

void HnswNode::DecodeNeighbours(engine::Context& ctx, const SearchKey& search_key) {
  neighbours.clear();
  auto edge_prefix = search_key.ConstructHnswEdgeWithSingleEnd(level, key);
  util::UniqueIterator iter(ctx, ctx.DefaultScanOptions(), ColumnFamilyID::Search);
  for (iter->Seek(edge_prefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(edge_prefix)) {
      break;
    }
    auto neighbour_edge = iter->key();
    neighbour_edge.remove_prefix(edge_prefix.size());
    Slice neighbour;
    GetSizedString(&neighbour_edge, &neighbour);
    neighbours.push_back(neighbour.ToString());
  }
}

Status HnswNode::AddNeighbour(engine::Context& ctx, const NodeKey& neighbour_key, const SearchKey& search_key,
                              rocksdb::WriteBatchBase* batch) const {
  auto edge_index_key = search_key.ConstructHnswEdge(level, key, neighbour_key);
  auto rocket_s = batch->Put(ctx.storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key, Slice());
  if (!rocket_s.ok()) {
    return {Status::NotOK, rocket_s.ToString()};
  }
  HnswNodeFieldMetadata node_metadata = GET_OR_RET(DecodeMetadata(ctx, search_key));
  node_metadata.num_neighbours++;
  return PutMetadata(&node_metadata, search_key, ctx.storage, batch);
}

Status HnswNode::RemoveNeighbour(engine::Context& ctx, const NodeKey& neighbour_key, const SearchKey& search_key,
                                 rocksdb::WriteBatchBase* batch) const {
  auto edge_index_key = search_key.ConstructHnswEdge(level, key, neighbour_key);
  auto s = batch->Delete(ctx.storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete edge, {}", s.ToString())};
  }

  HnswNodeFieldMetadata node_metadata = GET_OR_RET(DecodeMetadata(ctx, search_key));
  node_metadata.num_neighbours--;
  return PutMetadata(&node_metadata, search_key, ctx.storage, batch);
}

Status VectorItem::Create(NodeKey key, const kqir::NumericArray& vector, const HnswVectorFieldMetadata* metadata,
                          VectorItem* out) {
  if (metadata->dim != vector.size()) {
    return {Status::InvalidArgument, "VectorItem's metadata dimension must be consistent with the vector itself."};
  }

  *out = VectorItem(std::move(key), vector, metadata);
  return Status::OK();
}

Status VectorItem::Create(NodeKey key, kqir::NumericArray&& vector, const HnswVectorFieldMetadata* metadata,
                          VectorItem* out) {
  if (metadata->dim != vector.size()) {
    return {Status::InvalidArgument, "VectorItem's metadata dimension must be consistent with the vector itself."};
  }

  *out = VectorItem(std::move(key), std::move(vector), metadata);
  return Status::OK();
}

bool VectorItem::operator==(const VectorItem& other) const { return key == other.key; }

bool VectorItem::operator<(const VectorItem& other) const { return key < other.key; }

VectorItem::VectorItem(NodeKey&& key, const kqir::NumericArray& vector, const HnswVectorFieldMetadata* metadata)
    : key(std::move(key)), vector(vector), metadata(metadata) {}

VectorItem::VectorItem(NodeKey&& key, kqir::NumericArray&& vector, const HnswVectorFieldMetadata* metadata)
    : key(std::move(key)), vector(std::move(vector)), metadata(metadata) {}

StatusOr<double> ComputeSimilarity(const VectorItem& left, const VectorItem& right) {
  if (left.metadata->distance_metric != right.metadata->distance_metric || left.metadata->dim != right.metadata->dim)
    return {Status::InvalidArgument, "Vectors must be of the same metric and dimension to compute distance."};

  auto metric = left.metadata->distance_metric;
  auto dim = left.metadata->dim;

  switch (metric) {
    case DistanceMetric::L2: {
      double dist = 0.0;
      for (auto i = 0; i < dim; i++) {
        double diff = left.vector[i] - right.vector[i];
        dist += diff * diff;
      }
      return std::sqrt(dist);
    }
    case DistanceMetric::IP: {
      double dist = 0.0;
      for (auto i = 0; i < dim; i++) {
        dist += left.vector[i] * right.vector[i];
      }
      return -dist;
    }
    case DistanceMetric::COSINE: {
      double dist = 0.0;
      double norm_left = 0.0;
      double norm_right = 0.0;
      for (auto i = 0; i < dim; i++) {
        dist += left.vector[i] * right.vector[i];
        norm_left += left.vector[i] * left.vector[i];
        norm_right += right.vector[i] * right.vector[i];
      }
      auto similarity = dist / std::sqrt(norm_left * norm_right);
      return 1.0 - similarity;
    }
    default:
      __builtin_unreachable();
  }
}

HnswIndex::HnswIndex(const SearchKey& search_key, HnswVectorFieldMetadata* vector, engine::Storage* storage,
                     std::random_device::result_type seed)
    : search_key(search_key),
      metadata(vector),
      storage(storage),
      generator(std::mt19937(seed)),
      m_level_normalization_factor(1.0 / std::log(metadata->m)) {}

uint16_t HnswIndex::RandomizeLayer() {
  std::uniform_real_distribution<double> level_dist(0.0, 1.0);
  double r = level_dist(generator);
  double log_val = -std::log(r);
  double layer_val = log_val * m_level_normalization_factor;
  return static_cast<uint16_t>(std::floor(layer_val));
}

StatusOr<HnswIndex::NodeKey> HnswIndex::DefaultEntryPoint(engine::Context& ctx, uint16_t level) const {
  auto prefix = search_key.ConstructHnswLevelNodePrefix(level);
  util::UniqueIterator it(ctx, ctx.DefaultScanOptions(), ColumnFamilyID::Search);
  it->Seek(prefix);

  Slice node_key;
  Slice node_key_dst;
  if (it->Valid() && it->key().starts_with(prefix)) {
    std::string node_key_str = it->key().ToString().substr(prefix.size());
    node_key = Slice(node_key_str);
    if (!GetSizedString(&node_key, &node_key_dst)) {
      return {Status::NotOK, fmt::format("fail to decode the default node key layer {}", level)};
    }
    return node_key_dst.ToString();
  }
  return {Status::NotFound, fmt::format("No node found in layer {}", level)};
}

StatusOr<std::vector<VectorItem>> HnswIndex::DecodeNodesToVectorItems(engine::Context& ctx,
                                                                      const std::vector<NodeKey>& node_keys,
                                                                      uint16_t level, const SearchKey& search_key,
                                                                      const HnswVectorFieldMetadata* metadata) {
  std::vector<VectorItem> vector_items;
  vector_items.reserve(node_keys.size());

  for (const auto& neighbour_key : node_keys) {
    HnswNode neighbour_node(neighbour_key, level);
    auto neighbour_metadata_status = neighbour_node.DecodeMetadata(ctx, search_key);
    if (!neighbour_metadata_status.IsOK()) {
      continue;  // Skip this neighbour if metadata can't be decoded
    }
    auto neighbour_metadata = neighbour_metadata_status.GetValue();
    VectorItem item;
    GET_OR_RET(VectorItem::Create(neighbour_key, std::move(neighbour_metadata.vector), metadata, &item));
    vector_items.emplace_back(std::move(item));
  }
  return vector_items;
}

Status HnswIndex::AddEdge(const NodeKey& node_key1, const NodeKey& node_key2, uint16_t layer,
                          ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const {
  auto edge_index_key1 = search_key.ConstructHnswEdge(layer, node_key1, node_key2);
  auto s = batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key1, Slice());
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to add edge, {}", s.ToString())};
  }

  auto edge_index_key2 = search_key.ConstructHnswEdge(layer, node_key2, node_key1);
  s = batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key2, Slice());
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to add edge, {}", s.ToString())};
  }
  return Status::OK();
}

Status HnswIndex::RemoveEdge(const NodeKey& node_key1, const NodeKey& node_key2, uint16_t layer,
                             ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const {
  auto edge_index_key1 = search_key.ConstructHnswEdge(layer, node_key1, node_key2);
  auto s = batch->Delete(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key1);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete edge, {}", s.ToString())};
  }

  auto edge_index_key2 = search_key.ConstructHnswEdge(layer, node_key2, node_key1);
  s = batch->Delete(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key2);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete edge, {}", s.ToString())};
  }
  return Status::OK();
}

StatusOr<std::vector<VectorItem>> HnswIndex::SelectNeighbors(const VectorItem& vec,
                                                             const std::vector<VectorItem>& vertors,
                                                             uint16_t layer) const {
  std::vector<std::pair<double, VectorItem>> distances;
  distances.reserve(vertors.size());
  for (const auto& candidate : vertors) {
    auto dist = GET_OR_RET(ComputeSimilarity(vec, candidate));
    distances.emplace_back(dist, candidate);
  }

  std::sort(distances.begin(), distances.end());
  std::vector<VectorItem> selected_vs;

  selected_vs.reserve(vertors.size());
  uint16_t m_max = layer != 0 ? metadata->m : 2 * metadata->m;
  for (auto i = 0; i < std::min(m_max, (uint16_t)distances.size()); i++) {
    selected_vs.push_back(distances[i].second);
  }
  return selected_vs;
}

StatusOr<std::vector<VectorItemWithDistance>> HnswIndex::SearchLayerInternal(
    engine::Context& ctx, uint16_t level, const VectorItem& target_vector, uint32_t ef_runtime,
    const std::vector<NodeKey>& entry_points) const {
  std::vector<VectorItemWithDistance> result;
  std::unordered_set<NodeKey> visited;
  std::priority_queue<VectorItemWithDistance, std::vector<VectorItemWithDistance>, std::greater<>> explore_heap;
  std::priority_queue<VectorItemWithDistance> result_heap;

  for (const auto& entry_point_key : entry_points) {
    HnswNode entry_node = HnswNode(entry_point_key, level);
    auto entry_node_metadata = GET_OR_RET(entry_node.DecodeMetadata(ctx, search_key));

    VectorItem entry_point_vector;
    GET_OR_RET(
        VectorItem::Create(entry_point_key, std::move(entry_node_metadata.vector), metadata, &entry_point_vector));
    auto dist = GET_OR_RET(ComputeSimilarity(target_vector, entry_point_vector));

    explore_heap.push(std::make_pair(dist, entry_point_vector));
    result_heap.push(std::make_pair(dist, std::move(entry_point_vector)));
    visited.insert(entry_point_key);
  }

  while (!explore_heap.empty()) {
    auto [dist, current_vector] = explore_heap.top();
    explore_heap.pop();
    if (dist > result_heap.top().first) {
      break;
    }

    auto current_node = HnswNode(current_vector.key, level);
    current_node.DecodeNeighbours(ctx, search_key);

    for (const auto& neighbour_key : current_node.neighbours) {
      if (visited.find(neighbour_key) != visited.end()) {
        continue;
      }
      visited.insert(neighbour_key);

      auto neighbour_node = HnswNode(neighbour_key, level);
      auto neighbour_node_metadata = GET_OR_RET(neighbour_node.DecodeMetadata(ctx, search_key));

      VectorItem neighbour_node_vector;
      GET_OR_RET(VectorItem::Create(neighbour_key, std::move(neighbour_node_metadata.vector), metadata,
                                    &neighbour_node_vector));

      auto dist = GET_OR_RET(ComputeSimilarity(target_vector, neighbour_node_vector));
      explore_heap.push(std::make_pair(dist, neighbour_node_vector));
      result_heap.push(std::make_pair(dist, neighbour_node_vector));
      while (result_heap.size() > ef_runtime) {
        result_heap.pop();
      }
    }
  }

  result.resize(result_heap.size());
  auto idx = result_heap.size() - 1;
  while (!result_heap.empty()) {
    result[idx] = result_heap.top();
    result_heap.pop();
    idx--;
  }
  return result;
}

StatusOr<std::vector<VectorItem>> HnswIndex::SearchLayer(engine::Context& ctx, uint16_t level,
                                                         const VectorItem& target_vector, uint32_t ef_runtime,
                                                         const std::vector<NodeKey>& entry_points) const {
  std::vector<VectorItem> result;
  auto result_with_distance = GET_OR_RET(SearchLayerInternal(ctx, level, target_vector, ef_runtime, entry_points));
  for (auto& [_, vector_item] : result_with_distance) {
    result.push_back(std::move(vector_item));
  }
  return result;
}

Status HnswIndex::InsertVectorEntryInternal(engine::Context& ctx, std::string_view key,
                                            const kqir::NumericArray& vector,
                                            ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                            uint16_t target_level) const {
  auto cf_handle = storage->GetCFHandle(ColumnFamilyID::Search);
  VectorItem inserted_vector_item;
  GET_OR_RET(VectorItem::Create(std::string(key), vector, metadata, &inserted_vector_item));
  std::vector<VectorItem> nearest_vec_items;

  if (metadata->num_levels != 0) {
    auto level = metadata->num_levels - 1;

    auto default_entry_node = GET_OR_RET(DefaultEntryPoint(ctx, level));
    std::vector<NodeKey> entry_points{default_entry_node};

    for (; level > target_level; level--) {
      nearest_vec_items = GET_OR_RET(SearchLayer(ctx, level, inserted_vector_item, metadata->ef_runtime, entry_points));
      entry_points = {nearest_vec_items[0].key};
    }

    for (; level >= 0; level--) {
      nearest_vec_items =
          GET_OR_RET(SearchLayer(ctx, level, inserted_vector_item, metadata->ef_construction, entry_points));
      auto candidate_vec_items = GET_OR_RET(SelectNeighbors(inserted_vector_item, nearest_vec_items, level));
      auto node = HnswNode(std::string(key), level);
      auto m_max = level == 0 ? 2 * metadata->m : metadata->m;

      std::unordered_set<NodeKey> connected_edges_set;
      std::unordered_map<NodeKey, std::unordered_set<NodeKey>> deleted_edges_map;

      // Check if candidate node has room for more outgoing edges
      auto has_room_for_more_edges = [&](uint16_t candidate_node_num_neighbours) {
        return candidate_node_num_neighbours < m_max;
      };

      // Check if candidate node has room after some other nodes' are pruned in current batch
      auto has_room_after_deletions = [&](const HnswNode& candidate_node, uint16_t candidate_node_num_neighbours) {
        auto it = deleted_edges_map.find(candidate_node.key);
        if (it != deleted_edges_map.end()) {
          auto num_deleted_edges = static_cast<uint16_t>(it->second.size());
          return (candidate_node_num_neighbours - num_deleted_edges) < m_max;
        }
        return false;
      };

      for (const auto& candidate_vec : candidate_vec_items) {
        auto candidate_node = HnswNode(candidate_vec.key, level);
        auto candidate_node_metadata = GET_OR_RET(candidate_node.DecodeMetadata(ctx, search_key));
        uint16_t candidate_node_num_neighbours = candidate_node_metadata.num_neighbours;

        if (has_room_for_more_edges(candidate_node_num_neighbours) ||
            has_room_after_deletions(candidate_node, candidate_node_num_neighbours)) {
          GET_OR_RET(AddEdge(inserted_vector_item.key, candidate_node.key, level, batch));
          connected_edges_set.insert(candidate_node.key);
          continue;
        }

        // Re-evaluate the neighbours for the candidate node
        candidate_node.DecodeNeighbours(ctx, search_key);
        auto candidate_node_neighbour_vec_items =
            GET_OR_RET(DecodeNodesToVectorItems(ctx, candidate_node.neighbours, level, search_key, metadata));
        candidate_node_neighbour_vec_items.push_back(inserted_vector_item);
        auto sorted_neighbours_by_distance =
            GET_OR_RET(SelectNeighbors(candidate_vec, candidate_node_neighbour_vec_items, level));

        bool inserted_node_is_selected =
            std::find(sorted_neighbours_by_distance.begin(), sorted_neighbours_by_distance.end(),
                      inserted_vector_item) != sorted_neighbours_by_distance.end();

        if (inserted_node_is_selected) {
          // Add the edge between candidate and inserted node
          GET_OR_RET(AddEdge(inserted_vector_item.key, candidate_node.key, level, batch));
          connected_edges_set.insert(candidate_node.key);

          auto find_deleted_item = [&](const std::vector<VectorItem>& candidate_neighbours,
                                       const std::vector<VectorItem>& selected_neighbours) -> VectorItem {
            auto it =
                std::find_if(candidate_neighbours.begin(), candidate_neighbours.end(), [&](const VectorItem& item) {
                  return std::find(selected_neighbours.begin(), selected_neighbours.end(), item) ==
                         selected_neighbours.end();
                });
            return *it;
          };

          // Remove the edge for candidate and the pruned node
          auto deleted_node = find_deleted_item(candidate_node_neighbour_vec_items, sorted_neighbours_by_distance);
          GET_OR_RET(RemoveEdge(deleted_node.key, candidate_node.key, level, batch));
          deleted_edges_map[candidate_node.key].insert(deleted_node.key);
          deleted_edges_map[deleted_node.key].insert(candidate_node.key);
        }
      }

      // Update inserted node metadata
      HnswNodeFieldMetadata node_metadata(static_cast<uint16_t>(connected_edges_set.size()), vector);
      auto s = node.PutMetadata(&node_metadata, search_key, storage, batch.Get());
      if (!s.IsOK()) {
        return s;
      }

      // Update modified nodes metadata
      for (const auto& node_edges : deleted_edges_map) {
        auto& current_node_key = node_edges.first;
        auto current_node = HnswNode(current_node_key, level);
        auto current_node_metadata = GET_OR_RET(current_node.DecodeMetadata(ctx, search_key));
        auto new_num_neighbours = current_node_metadata.num_neighbours - node_edges.second.size();
        if (connected_edges_set.count(current_node_key) != 0) {
          new_num_neighbours++;
          connected_edges_set.erase(current_node_key);
        }
        current_node_metadata.num_neighbours = new_num_neighbours;
        s = current_node.PutMetadata(&current_node_metadata, search_key, storage, batch.Get());
        if (!s.IsOK()) {
          return s;
        }
      }

      for (const auto& current_node_key : connected_edges_set) {
        auto current_node = HnswNode(current_node_key, level);
        HnswNodeFieldMetadata current_node_metadata = GET_OR_RET(current_node.DecodeMetadata(ctx, search_key));
        current_node_metadata.num_neighbours++;
        s = current_node.PutMetadata(&current_node_metadata, search_key, storage, batch.Get());
        if (!s.IsOK()) {
          return s;
        }
      }

      entry_points.clear();
      for (const auto& new_entry_point : nearest_vec_items) {
        entry_points.push_back(new_entry_point.key);
      }
    }
  } else {
    auto node = HnswNode(std::string(key), 0);
    HnswNodeFieldMetadata node_metadata(0, vector);
    auto s = node.PutMetadata(&node_metadata, search_key, storage, batch.Get());
    if (!s.IsOK()) {
      return s;
    }
    metadata->num_levels = 1;
  }

  while (target_level > metadata->num_levels - 1) {
    auto node = HnswNode(std::string(key), metadata->num_levels);
    HnswNodeFieldMetadata node_metadata(0, vector);
    auto s = node.PutMetadata(&node_metadata, search_key, storage, batch.Get());
    if (!s.IsOK()) {
      return s;
    }
    metadata->num_levels++;
  }

  std::string encoded_index_metadata;
  metadata->Encode(&encoded_index_metadata);
  auto index_meta_key = search_key.ConstructFieldMeta();
  auto s = batch->Put(cf_handle, index_meta_key, encoded_index_metadata);
  if (!s.ok()) {
    return {Status::NotOK, s.ToString()};
  }

  return Status::OK();
}

Status HnswIndex::InsertVectorEntry(engine::Context& ctx, std::string_view key, const kqir::NumericArray& vector,
                                    ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  auto target_level = RandomizeLayer();
  return InsertVectorEntryInternal(ctx, key, vector, batch, target_level);
}

Status HnswIndex::DeleteVectorEntry(engine::Context& ctx, std::string_view key,
                                    ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const {
  std::string node_key(key);
  for (uint16_t level = 0; level < metadata->num_levels; level++) {
    auto node = HnswNode(node_key, level);
    auto node_metadata_status = node.DecodeMetadata(ctx, search_key);
    if (!node_metadata_status.IsOK()) {
      break;
    }

    auto node_metadata = std::move(node_metadata_status).GetValue();
    auto node_index_key = search_key.ConstructHnswNode(level, key);
    auto s = batch->Delete(storage->GetCFHandle(ColumnFamilyID::Search), node_index_key);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    node.DecodeNeighbours(ctx, search_key);
    for (const auto& neighbour_key : node.neighbours) {
      GET_OR_RET(RemoveEdge(node_key, neighbour_key, level, batch));
      auto neighbour_node = HnswNode(neighbour_key, level);
      HnswNodeFieldMetadata neighbour_node_metadata = GET_OR_RET(neighbour_node.DecodeMetadata(ctx, search_key));
      neighbour_node_metadata.num_neighbours--;
      auto s = neighbour_node.PutMetadata(&neighbour_node_metadata, search_key, storage, batch.Get());
      if (!s.IsOK()) {
        return s;
      }
    }
  }

  auto has_other_nodes_at_level = [&](uint16_t level, std::string_view skip_key) -> bool {
    auto prefix = search_key.ConstructHnswLevelNodePrefix(level);
    util::UniqueIterator it(ctx, ctx.DefaultScanOptions(), ColumnFamilyID::Search);
    it->Seek(prefix);

    Slice node_key;
    Slice node_key_dst;
    while (it->Valid() && it->key().starts_with(prefix)) {
      std::string node_key_str = it->key().ToString().substr(prefix.size());
      node_key = Slice(node_key_str);
      if (!GetSizedString(&node_key, &node_key_dst)) {
        continue;
      }
      if (node_key_dst.ToString() != skip_key) {
        return true;
      }
      it->Next();
    }
    return false;
  };

  while (metadata->num_levels > 0) {
    if (has_other_nodes_at_level(metadata->num_levels - 1, key)) {
      break;
    }
    metadata->num_levels--;
  }

  std::string encoded_index_metadata;
  metadata->Encode(&encoded_index_metadata);
  auto index_meta_key = search_key.ConstructFieldMeta();
  auto s = batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), index_meta_key, encoded_index_metadata);
  if (!s.ok()) {
    return {Status::NotOK, s.ToString()};
  }
  return Status::OK();
}

StatusOr<std::vector<KeyWithDistance>> HnswIndex::KnnSearch(engine::Context& ctx,
                                                            const kqir::NumericArray& query_vector, uint32_t k) const {
  VectorItem query_vector_item;
  GET_OR_RET(VectorItem::Create({}, query_vector, metadata, &query_vector_item));

  if (metadata->num_levels == 0) {
    return {Status::NotFound, fmt::format("No vector found in the HNSW index")};
  }

  auto level = metadata->num_levels - 1;
  auto default_entry_node = GET_OR_RET(DefaultEntryPoint(ctx, level));
  std::vector<NodeKey> entry_points{default_entry_node};
  std::vector<VectorItem> nearest_vec_items;

  for (; level > 0; level--) {
    nearest_vec_items = GET_OR_RET(SearchLayer(ctx, level, query_vector_item, metadata->ef_runtime, entry_points));
    entry_points = {nearest_vec_items[0].key};
  }

  uint32_t effective_ef = std::max(metadata->ef_runtime, k);  // Ensure ef_runtime is at least k
  auto nearest_vec_with_distance =
      GET_OR_RET(SearchLayerInternal(ctx, 0, query_vector_item, effective_ef, entry_points));

  uint32_t result_length = std::min(k, static_cast<uint32_t>(nearest_vec_with_distance.size()));
  std::vector<KeyWithDistance> nearest_neighbours;
  for (uint32_t result_idx = 0; result_idx < result_length; result_idx++) {
    nearest_neighbours.emplace_back(nearest_vec_with_distance[result_idx].first,
                                    std::move(nearest_vec_with_distance[result_idx].second.key));
  }
  return nearest_neighbours;
}

StatusOr<std::vector<KeyWithDistance>> HnswIndex::ExpandSearchScope(engine::Context& ctx,
                                                                    const kqir::NumericArray& query_vector,
                                                                    std::vector<redis::KeyWithDistance>&& initial_keys,
                                                                    std::unordered_set<std::string>& visited) const {
  constexpr uint16_t level = 0;
  VectorItem query_vector_item;
  GET_OR_RET(VectorItem::Create({}, query_vector, metadata, &query_vector_item));
  std::vector<KeyWithDistance> result;

  while (!initial_keys.empty()) {
    auto current_key = initial_keys.front().second;
    initial_keys.erase(initial_keys.begin());

    auto current_node = HnswNode(current_key, level);
    current_node.DecodeNeighbours(ctx, search_key);

    for (const auto& neighbour_key : current_node.neighbours) {
      if (visited.find(neighbour_key) != visited.end()) {
        continue;
      }
      visited.insert(neighbour_key);

      auto neighbour_node = HnswNode(neighbour_key, level);
      auto neighbour_node_metadata = GET_OR_RET(neighbour_node.DecodeMetadata(ctx, search_key));

      VectorItem neighbour_node_vector;
      GET_OR_RET(VectorItem::Create(neighbour_key, std::move(neighbour_node_metadata.vector), metadata,
                                    &neighbour_node_vector));

      auto dist = GET_OR_RET(ComputeSimilarity(query_vector_item, neighbour_node_vector));
      result.emplace_back(dist, neighbour_key);
    }
  }
  std::sort(result.begin(), result.end(),
            [](const KeyWithDistance& a, const KeyWithDistance& b) { return a.first < b.first; });

  return result;
}

}  // namespace redis
