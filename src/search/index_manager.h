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

#include "search/index_info.h"
#include "search/indexer.h"
#include "search/ir.h"
#include "search/plan_executor.h"
#include "status.h"
#include "storage/storage.h"

namespace redis {

struct IndexManager {
  kqir::IndexMap index_map;
  GlobalIndexer *indexer;
  engine::Storage *storage;

  IndexManager(GlobalIndexer *indexer, engine::Storage *storage) : indexer(indexer), storage(storage) {}

  Status Load(engine::Context &ctx, const std::string &ns);
  Status Create(engine::Context &ctx, std::unique_ptr<kqir::IndexInfo> info);
  Status Drop(engine::Context &ctx, std::string_view index_name, const std::string &ns);

  StatusOr<std::unique_ptr<kqir::PlanOperator>> GeneratePlan(std::unique_ptr<kqir::Node> ir,
                                                             const std::string &ns) const;
  StatusOr<std::vector<kqir::ExecutorContext::RowType>> Search(std::unique_ptr<kqir::Node> ir,
                                                               const std::string &ns) const;

  StatusOr<std::unordered_set<std::string>> TagValues(engine::Context &ctx, std::string_view index_name,
                                                      std::string_view tag_field_name, const std::string &ns);
};
}  // namespace redis
