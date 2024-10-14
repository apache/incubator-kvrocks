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

#include <encoding.h>
#include <gtest/gtest.h>
#include <test_base.h>

#include <iostream>
#include <memory>
#include <unordered_set>

#include "search/hnsw_indexer.h"
#include "search/indexer.h"
#include "search/search_encoding.h"
#include "storage/storage.h"

struct NodeTest : public TestBase {
  std::string ns = "hnsw_node_test_ns";
  std::string idx_name = "hnsw_node_test_idx";
  std::string key = "vector";
  redis::SearchKey search_key;

  NodeTest() : search_key(ns, idx_name, key) {}

  void TearDown() override {}
};

TEST_F(NodeTest, PutAndDecodeMetadata) {
  uint16_t layer = 0;
  redis::HnswNode node1("node1", layer);
  redis::HnswNode node2("node2", layer);
  redis::HnswNode node3("node3", layer);

  redis::HnswNodeFieldMetadata metadata1(0, {1, 2, 3});
  redis::HnswNodeFieldMetadata metadata2(0, {4, 5, 6});
  redis::HnswNodeFieldMetadata metadata3(0, {7, 8, 9});

  auto batch = storage_->GetWriteBatchBase();
  auto s = node1.PutMetadata(&metadata1, search_key, storage_.get(), batch.Get());
  ASSERT_TRUE(s.IsOK());
  s = node2.PutMetadata(&metadata2, search_key, storage_.get(), batch.Get());
  ASSERT_TRUE(s.IsOK());
  s = node3.PutMetadata(&metadata3, search_key, storage_.get(), batch.Get());
  ASSERT_TRUE(s.IsOK());
  engine::Context ctx(storage_.get());
  auto s1 = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s1.ok());

  auto decoded_metadata1 = node1.DecodeMetadata(ctx, search_key);
  ASSERT_TRUE(decoded_metadata1.IsOK());
  ASSERT_EQ(decoded_metadata1.GetValue().num_neighbours, 0);
  ASSERT_EQ(decoded_metadata1.GetValue().vector, std::vector<double>({1, 2, 3}));

  auto decoded_metadata2 = node2.DecodeMetadata(ctx, search_key);
  ASSERT_TRUE(decoded_metadata2.IsOK());
  ASSERT_EQ(decoded_metadata2.GetValue().num_neighbours, 0);
  ASSERT_EQ(decoded_metadata2.GetValue().vector, std::vector<double>({4, 5, 6}));

  auto decoded_metadata3 = node3.DecodeMetadata(ctx, search_key);
  ASSERT_TRUE(decoded_metadata3.IsOK());
  ASSERT_EQ(decoded_metadata3.GetValue().num_neighbours, 0);
  ASSERT_EQ(decoded_metadata3.GetValue().vector, std::vector<double>({7, 8, 9}));

  // Prepare edges between node1 and node2
  batch = storage_->GetWriteBatchBase();
  auto edge1 = search_key.ConstructHnswEdge(layer, "node1", "node2");
  auto edge2 = search_key.ConstructHnswEdge(layer, "node2", "node1");
  auto edge3 = search_key.ConstructHnswEdge(layer, "node2", "node3");
  auto edge4 = search_key.ConstructHnswEdge(layer, "node3", "node2");

  s1 = batch->Put(storage_->GetCFHandle(ColumnFamilyID::Search), edge1, Slice());
  ASSERT_TRUE(s1.ok());
  s1 = batch->Put(storage_->GetCFHandle(ColumnFamilyID::Search), edge2, Slice());
  ASSERT_TRUE(s1.ok());
  s1 = batch->Put(storage_->GetCFHandle(ColumnFamilyID::Search), edge3, Slice());
  ASSERT_TRUE(s1.ok());
  s1 = batch->Put(storage_->GetCFHandle(ColumnFamilyID::Search), edge4, Slice());
  ASSERT_TRUE(s1.ok());
  s1 = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s1.ok());

  node1.DecodeNeighbours(ctx, search_key);
  EXPECT_EQ(node1.neighbours.size(), 1);
  EXPECT_EQ(node1.neighbours[0], "node2");

  node2.DecodeNeighbours(ctx, search_key);
  EXPECT_EQ(node2.neighbours.size(), 2);
  std::unordered_set<std::string> expected_neighbours = {"node1", "node3"};
  std::unordered_set<std::string> actual_neighbours(node2.neighbours.begin(), node2.neighbours.end());
  EXPECT_EQ(actual_neighbours, expected_neighbours);

  node3.DecodeNeighbours(ctx, search_key);
  EXPECT_EQ(node3.neighbours.size(), 1);
  EXPECT_EQ(node3.neighbours[0], "node2");
}

TEST_F(NodeTest, ModifyNeighbours) {
  uint16_t layer = 1;
  redis::HnswNode node1("node1", layer);
  redis::HnswNode node2("node2", layer);
  redis::HnswNode node3("node3", layer);
  redis::HnswNode node4("node4", layer);

  redis::HnswNodeFieldMetadata metadata1(0, {1, 2, 3});
  redis::HnswNodeFieldMetadata metadata2(0, {4, 5, 6});
  redis::HnswNodeFieldMetadata metadata3(0, {7, 8, 9});
  redis::HnswNodeFieldMetadata metadata4(0, {10, 11, 12});

  // Add Nodes
  auto batch1 = storage_->GetWriteBatchBase();
  auto put_meta_data = node1.PutMetadata(&metadata1, search_key, storage_.get(), batch1.Get());
  ASSERT_TRUE(put_meta_data.IsOK());
  put_meta_data = node2.PutMetadata(&metadata2, search_key, storage_.get(), batch1.Get());
  ASSERT_TRUE(put_meta_data.IsOK());
  put_meta_data = node3.PutMetadata(&metadata3, search_key, storage_.get(), batch1.Get());
  ASSERT_TRUE(put_meta_data.IsOK());
  put_meta_data = node4.PutMetadata(&metadata4, search_key, storage_.get(), batch1.Get());
  ASSERT_TRUE(put_meta_data.IsOK());
  engine::Context ctx(storage_.get());
  auto s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch1->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  // Add Edges
  auto batch2 = storage_->GetWriteBatchBase();
  auto s1 = node1.AddNeighbour(ctx, "node2", search_key, batch2.Get());
  ASSERT_TRUE(s1.IsOK());
  auto s2 = node2.AddNeighbour(ctx, "node1", search_key, batch2.Get());
  ASSERT_TRUE(s2.IsOK());
  auto s3 = node2.AddNeighbour(ctx, "node3", search_key, batch2.Get());
  ASSERT_TRUE(s3.IsOK());
  auto s4 = node3.AddNeighbour(ctx, "node2", search_key, batch2.Get());
  ASSERT_TRUE(s4.IsOK());
  s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch2->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  node1.DecodeNeighbours(ctx, search_key);
  EXPECT_EQ(node1.neighbours.size(), 1);
  EXPECT_EQ(node1.neighbours[0], "node2");

  node2.DecodeNeighbours(ctx, search_key);
  EXPECT_EQ(node2.neighbours.size(), 2);
  std::unordered_set<std::string> expected_neighbours = {"node1", "node3"};
  std::unordered_set<std::string> actual_neighbours(node2.neighbours.begin(), node2.neighbours.end());
  EXPECT_EQ(actual_neighbours, expected_neighbours);

  node3.DecodeNeighbours(ctx, search_key);
  EXPECT_EQ(node3.neighbours.size(), 1);
  EXPECT_EQ(node3.neighbours[0], "node2");

  // Remove Edges
  auto batch3 = storage_->GetWriteBatchBase();
  auto s5 = node2.RemoveNeighbour(ctx, "node3", search_key, batch3.Get());
  ASSERT_TRUE(s5.IsOK());

  s = storage_->Write(ctx, storage_->DefaultWriteOptions(), batch3->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  node2.DecodeNeighbours(ctx, search_key);
  EXPECT_EQ(node2.neighbours.size(), 1);
  EXPECT_EQ(node2.neighbours[0], "node1");
}
