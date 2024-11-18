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

#include <tsl/htrie_map.h>

#include <deque>
#include <map>
#include <utility>
#include <variant>

#include "commands/commander.h"
#include "config/config.h"
#include "index_info.h"
#include "indexer.h"
#include "search/search_encoding.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "types/redis_hash.h"
#include "types/redis_json.h"
#include "value.h"

namespace redis {

struct GlobalIndexer;

struct FieldValueRetriever {
  struct HashData {
    Hash hash;
    HashMetadata metadata;
    std::string_view key;

    HashData(Hash hash, HashMetadata metadata, std::string_view key)
        : hash(std::move(hash)), metadata(std::move(metadata)), key(key) {}
  };
  using JsonData = JsonValue;

  using Variant = std::variant<HashData, JsonData>;
  Variant db;

  static StatusOr<FieldValueRetriever> Create(IndexOnDataType type, std::string_view key, engine::Storage *storage,
                                              const std::string &ns);

  explicit FieldValueRetriever(Hash hash, HashMetadata metadata, std::string_view key)
      : db(std::in_place_type<HashData>, std::move(hash), std::move(metadata), key) {}

  explicit FieldValueRetriever(JsonValue json) : db(std::in_place_type<JsonData>, std::move(json)) {}

  StatusOr<kqir::Value> Retrieve(engine::Context &ctx, std::string_view field, const redis::IndexFieldMetadata *type);

  static StatusOr<kqir::Value> ParseFromJson(const jsoncons::json &value, const redis::IndexFieldMetadata *type);
  static StatusOr<kqir::Value> ParseFromHash(const std::string &value, const redis::IndexFieldMetadata *type);
};

struct IndexUpdater {
  using FieldValues = std::map<std::string, kqir::Value>;

  const kqir::IndexInfo *info = nullptr;
  GlobalIndexer *indexer = nullptr;
  std::mutex update_mutex;

  explicit IndexUpdater(const kqir::IndexInfo *info) : info(info) {}

  StatusOr<FieldValues> Record(engine::Context &ctx, std::string_view key) const;
  Status UpdateIndex(engine::Context &ctx, const std::string &field, std::string_view key, const kqir::Value &original,
                     const kqir::Value &current);
  Status Update(engine::Context &ctx, const FieldValues &original, std::string_view key);

  Status Build(engine::Context &ctx);

  Status UpdateTagIndex(engine::Context &ctx, std::string_view key, const kqir::Value &original,
                        const kqir::Value &current, const SearchKey &search_key, const TagFieldMetadata *tag) const;
  Status UpdateNumericIndex(engine::Context &ctx, std::string_view key, const kqir::Value &original,
                            const kqir::Value &current, const SearchKey &search_key,
                            const NumericFieldMetadata *num) const;
  Status UpdateHnswVectorIndex(engine::Context &ctx, std::string_view key, const kqir::Value &original,
                               const kqir::Value &current, const SearchKey &search_key,
                               HnswVectorFieldMetadata *vector);
};

struct GlobalIndexer {
  using FieldValues = IndexUpdater::FieldValues;
  struct RecordResult {
    IndexUpdater *updater;
    std::string key;
    FieldValues fields;
  };

  tsl::htrie_map<char, IndexUpdater *> prefix_map;
  std::vector<std::unique_ptr<IndexUpdater>> updater_list;

  engine::Storage *storage = nullptr;

  explicit GlobalIndexer(engine::Storage *storage) : storage(storage) {}

  void Add(std::unique_ptr<IndexUpdater> updater);
  void Remove(const kqir::IndexInfo *index);

  StatusOr<RecordResult> Record(engine::Context &ctx, std::string_view key, const std::string &ns);
  static Status Update(engine::Context &ctx, const RecordResult &original);
};

}  // namespace redis
