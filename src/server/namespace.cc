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

#include "namespace.h"

#include "jsoncons/json.hpp"

// Error messages
constexpr const char* kErrNamespaceExists = "the namespace already exists";
constexpr const char* kErrTokenExists = "the token already exists";
constexpr const char* kErrNamespaceNotFound = "the namespace was not found";
constexpr const char* kErrRequiredPassEmpty = "forbidden to add namespace when requirepass was empty";
constexpr const char* kErrClusterModeEnabled = "forbidden to add namespace when cluster mode was enabled";
constexpr const char* kErrDeleteDefaultNamespace = "forbidden to delete the default namespace";
constexpr const char* kErrAddDefaultNamespace = "forbidden to add the default namespace";
constexpr const char* kErrInvalidToken = "the token is duplicated with requirepass or masterauth";
constexpr const char* kErrCantModifyNamespace =
    "modify namespace requires the server is running with a configuration file or enabled namespace replication";

Status IsNamespaceLegal(const std::string& ns) {
  if (ns.size() > UINT8_MAX) {
    return {Status::NotOK, fmt::format("size exceed limit {}", UINT8_MAX)};
  }
  char last_char = ns.back();
  if (last_char == std::numeric_limits<char>::max()) {
    return {Status::NotOK, "namespace contain illegal letter"};
  }
  return Status::OK();
}

bool Namespace::IsAllowModify() const {
  auto config = storage_->GetConfig();

  return config->HasConfigFile() || config->repl_namespace_enabled;
}

Status Namespace::loadFromDB(std::map<std::string, std::string>* db_tokens) const {
  std::string value;
  engine::Context ctx(storage_);
  auto cf = storage_->GetCFHandle(ColumnFamilyID::Propagate);
  auto s = storage_->Get(ctx, ctx.GetReadOptions(), cf, kNamespaceDBKey, &value);
  if (!s.ok()) {
    if (s.IsNotFound()) return Status::OK();
    return {Status::NotOK, s.ToString()};
  }

  jsoncons::json j = jsoncons::json::parse(value);
  for (const auto& iter : j.object_range()) {
    db_tokens->insert({iter.key(), iter.value().as_string()});
  }
  return Status::OK();
}

Status Namespace::LoadAndRewrite() {
  auto config = storage_->GetConfig();
  // Namespace is NOT allowed in the cluster mode, so we don't need to rewrite here.
  if (config->cluster_enabled) return Status::OK();

  std::map<std::string, std::string> db_tokens;
  auto s = loadFromDB(&db_tokens);
  if (!s.IsOK()) return s;

  if (!db_tokens.empty() && !config->repl_namespace_enabled) {
    return {Status::NotOK, "cannot switch off repl_namespace_enabled when namespaces exist in db"};
  }

  std::unique_lock<std::shared_mutex> lock(tokens_mu_);
  // Load from the configuration file first
  tokens_ = config->load_tokens;
  // Merge the tokens from the database if the token is not in the configuration file
  for (const auto& iter : db_tokens) {
    if (tokens_.find(iter.first) == tokens_.end()) {
      tokens_[iter.first] = iter.second;
    }
  }

  // The following rewrite is to remove namespace/token pairs from the configuration if the namespace replication
  // is enabled. So we don't need to do that if no tokens are loaded or the namespace replication is disabled.
  if (config->load_tokens.empty() || !config->repl_namespace_enabled) return Status::OK();

  return Rewrite(tokens_);
}

StatusOr<std::string> Namespace::Get(const std::string& ns) {
  std::shared_lock lock(tokens_mu_);
  for (const auto& iter : tokens_) {
    if (iter.second == ns) {
      return iter.first;
    }
  }
  return {Status::NotFound};
}

StatusOr<std::string> Namespace::GetByToken(const std::string& token) {
  std::shared_lock lock(tokens_mu_);
  auto iter = tokens_.find(token);
  if (iter == tokens_.end()) {
    return {Status::NotFound};
  }
  return iter->second;
}

Status Namespace::Set(const std::string& ns, const std::string& token) {
  auto s = IsNamespaceLegal(ns);
  if (!s.IsOK()) return s;
  auto config = storage_->GetConfig();
  if (config->requirepass.empty()) {
    return {Status::NotOK, kErrRequiredPassEmpty};
  }
  if (config->cluster_enabled) {
    return {Status::NotOK, kErrClusterModeEnabled};
  }
  if (!IsAllowModify()) {
    return {Status::NotOK, kErrCantModifyNamespace};
  }
  if (ns == kDefaultNamespace) {
    return {Status::NotOK, kErrAddDefaultNamespace};
  }
  if (token == config->requirepass || token == config->masterauth) {
    return {Status::NotOK, kErrInvalidToken};
  }

  std::unique_lock lock(tokens_mu_);
  for (const auto& iter : tokens_) {
    if (iter.second == ns) {  // need to delete the old token first
      tokens_.erase(iter.first);
      break;
    }
  }
  tokens_[token] = ns;

  s = Rewrite(tokens_);
  if (!s.IsOK()) {
    tokens_.erase(token);
    return s;
  }
  return Status::OK();
}

Status Namespace::Add(const std::string& ns, const std::string& token) {
  {
    std::shared_lock lock(tokens_mu_);
    // duplicate namespace
    for (const auto& iter : tokens_) {
      if (iter.second == ns) {
        if (iter.first == token) return Status::OK();
        return {Status::NotOK, kErrNamespaceExists};
      }
    }
    // duplicate token
    if (tokens_.find(token) != tokens_.end()) {
      return {Status::NotOK, kErrTokenExists};
    }
  }

  // we don't need to lock the mutex here because the Set method will lock it
  return Set(ns, token);
}

Status Namespace::Del(const std::string& ns) {
  if (ns == kDefaultNamespace) {
    return {Status::NotOK, kErrDeleteDefaultNamespace};
  }
  if (!IsAllowModify()) {
    return {Status::NotOK, kErrCantModifyNamespace};
  }

  std::unique_lock lock(tokens_mu_);
  for (const auto& iter : tokens_) {
    if (iter.second == ns) {
      tokens_.erase(iter.first);
      auto s = Rewrite(tokens_);
      if (!s.IsOK()) {
        tokens_[iter.first] = iter.second;
        return s;
      }
      return Status::OK();
    }
  }
  return {Status::NotOK, kErrNamespaceNotFound};
}

Status Namespace::Rewrite(const std::map<std::string, std::string>& tokens) const {
  auto config = storage_->GetConfig();
  // Rewrite the configuration file only if it's running with the configuration file
  if (config->HasConfigFile()) {
    auto s = config->Rewrite(tokens);
    if (!s.IsOK()) {
      return s;
    }
  }

  // Don't propagate write to DB if its role is slave to prevent from
  // increasing the DB sequence number.
  if (config->IsSlave()) {
    return Status::OK();
  }

  // Don't need to write to db if repl_namespace_enabled is false
  if (!config->repl_namespace_enabled) {
    return Status::OK();
  }
  jsoncons::json json;
  for (const auto& iter : tokens) {
    json[iter.first] = iter.second;
  }
  engine::Context ctx(storage_);
  return storage_->WriteToPropagateCF(ctx, kNamespaceDBKey, json.to_string());
}
