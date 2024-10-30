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

#include "cluster/cluster_defs.h"
#include "cluster/slot_import.h"
#include "cluster/sync_migrate_context.h"
#include "commander.h"
#include "error_constants.h"
#include "status.h"

namespace redis {

class CommandCluster : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "nodes" || subcommand_ == "slots" || subcommand_ == "info"))
      return Status::OK();

    // CLUSTER RESET [HARD|SOFT]
    if (subcommand_ == "reset" && (args_.size() == 2 || args_.size() == 3)) {
      if (args_.size() == 3 && !util::EqualICase(args_[2], "hard") && !util::EqualICase(args_[2], "soft")) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
      return Status::OK();
    }

    if (subcommand_ == "keyslot" && args_.size() == 3) return Status::OK();

    if (subcommand_ == "import") {
      if (args.size() != 4) return {Status::RedisParseErr, errWrongNumOfArguments};

      Status s = CommandTable::ParseSlotRanges(args_[2], slot_ranges_);
      if (!s.IsOK()) {
        return s;
      }

      auto state = ParseInt<unsigned>(args[3], {kImportStart, kImportNone}, 10);
      if (!state) return {Status::NotOK, "Invalid import state"};

      state_ = static_cast<ImportStatus>(*state);
      return Status::OK();
    }

    if (subcommand_ == "replicas" && args_.size() == 3) return Status::OK();

    return {Status::RedisParseErr, "CLUSTER command, CLUSTER INFO|NODES|SLOTS|KEYSLOT|RESET|REPLICAS"};
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (!srv->GetConfig()->cluster_enabled) {
      return {Status::RedisExecErr, "Cluster mode is not enabled"};
    }

    if (!conn->IsAdmin()) {
      return {Status::RedisExecErr, errAdminPermissionRequired};
    }

    if (subcommand_ == "keyslot") {
      auto slot_id = GetSlotIdFromKey(args_[2]);
      *output = redis::Integer(slot_id);
    } else if (subcommand_ == "slots") {
      std::vector<SlotInfo> infos;
      Status s = srv->cluster->GetSlotsInfo(&infos);
      if (s.IsOK()) {
        output->append(redis::MultiLen(infos.size()));
        for (const auto &info : infos) {
          output->append(redis::MultiLen(info.nodes.size() + 2));
          output->append(redis::Integer(info.start));
          output->append(redis::Integer(info.end));
          for (const auto &n : info.nodes) {
            output->append(redis::MultiLen(3));
            output->append(redis::BulkString(n.host));
            output->append(redis::Integer(n.port));
            output->append(redis::BulkString(n.id));
          }
        }
      } else {
        return s;
      }
    } else if (subcommand_ == "nodes") {
      std::string nodes_desc;
      Status s = srv->cluster->GetClusterNodes(&nodes_desc);
      if (s.IsOK()) {
        *output = conn->VerbatimString("txt", nodes_desc);
      } else {
        return s;
      }
    } else if (subcommand_ == "info") {
      std::string cluster_info;
      Status s = srv->cluster->GetClusterInfo(&cluster_info);
      if (s.IsOK()) {
        *output = conn->VerbatimString("txt", cluster_info);
      } else {
        return s;
      }
    } else if (subcommand_ == "import") {
      // TODO: support multiple slot ranges
      Status s = srv->cluster->ImportSlotRange(conn, slot_ranges_[0], state_);
      if (s.IsOK()) {
        *output = redis::SimpleString("OK");
      } else {
        return s;
      }
    } else if (subcommand_ == "reset") {
      Status s = srv->cluster->Reset();
      if (s.IsOK()) {
        *output = redis::SimpleString("OK");
      } else {
        return s;
      }
    } else if (subcommand_ == "replicas") {
      auto node_id = args_[2];
      StatusOr<std::string> s = srv->cluster->GetReplicas(node_id);
      if (s.IsOK()) {
        *output = conn->VerbatimString("txt", s.GetValue());
      } else {
        return s;
      }
    } else {
      return {Status::RedisExecErr, "Invalid cluster command options"};
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  std::vector<SlotRange> slot_ranges_;
  ImportStatus state_ = kImportNone;
};

class CommandClusterX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);

    if (args.size() == 2 && (subcommand_ == "version" || subcommand_ == "myid")) return Status::OK();

    if (subcommand_ == "setnodeid" && args_.size() == 3 && args_[2].size() == kClusterNodeIdLen) return Status::OK();

    if (subcommand_ == "migrate") {
      if (args.size() < 4 || args.size() > 6) return {Status::RedisParseErr, errWrongNumOfArguments};

      Status s = CommandTable::ParseSlotRanges(args_[2], slot_ranges_);
      if (!s.IsOK()) {
        return s;
      }

      dst_node_id_ = args[3];

      if (args.size() >= 5) {
        auto sync_flag = util::ToLower(args[4]);
        if (sync_flag == "async") {
          sync_migrate_ = false;

          if (args.size() == 6) {
            return {Status::RedisParseErr, "Async migration does not support timeout"};
          }
        } else if (sync_flag == "sync") {
          sync_migrate_ = true;

          if (args.size() == 6) {
            auto parse_result = ParseInt<int>(args[5], 10);
            if (!parse_result) {
              return {Status::RedisParseErr, "timeout is not an integer or out of range"};
            }
            if (*parse_result < 0) {
              return {Status::RedisParseErr, errTimeoutIsNegative};
            }
            sync_migrate_timeout_ = *parse_result;
          }
        } else {
          return {Status::RedisParseErr, "Invalid sync flag"};
        }
      }
      return Status::OK();
    }

    if (subcommand_ == "setnodes" && args_.size() >= 4) {
      nodes_str_ = args_[2];

      auto parse_result = ParseInt<int64_t>(args[3], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "Invalid version"};
      }

      set_version_ = *parse_result;

      if (args_.size() == 4) return Status::OK();

      if (args_.size() == 5 && util::EqualICase(args_[4], "force")) {
        force_ = true;
        return Status::OK();
      }

      return {Status::RedisParseErr, "Invalid setnodes options"};
    }

    // CLUSTERX SETSLOT $SLOT_ID NODE $NODE_ID $VERSION
    if (subcommand_ == "setslot" && args_.size() == 6) {
      Status s = CommandTable::ParseSlotRanges(args_[2], slot_ranges_);
      if (!s.IsOK()) {
        return s;
      }

      if (!util::EqualICase(args_[3], "node")) {
        return {Status::RedisParseErr, "Invalid setslot options"};
      }

      if (args_[4].size() != kClusterNodeIdLen) {
        return {Status::RedisParseErr, "Invalid node id"};
      }

      auto parse_version = ParseInt<int64_t>(args[5], 10);
      if (!parse_version) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      if (*parse_version < 0) return {Status::RedisParseErr, "Invalid version"};

      set_version_ = *parse_version;

      return Status::OK();
    }

    return {Status::RedisParseErr, "CLUSTERX command, CLUSTERX VERSION|MYID|SETNODEID|SETNODES|SETSLOT|MIGRATE"};
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (!srv->GetConfig()->cluster_enabled) {
      return {Status::RedisExecErr, "Cluster mode is not enabled"};
    }

    if (!conn->IsAdmin()) {
      return {Status::RedisExecErr, errAdminPermissionRequired};
    }

    bool need_persist_nodes_info = false;
    if (subcommand_ == "setnodes") {
      Status s = srv->cluster->SetClusterNodes(nodes_str_, set_version_, force_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = redis::SimpleString("OK");
      } else {
        return s;
      }
    } else if (subcommand_ == "setnodeid") {
      Status s = srv->cluster->SetNodeId(args_[2]);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = redis::SimpleString("OK");
      } else {
        return s;
      }
    } else if (subcommand_ == "setslot") {
      Status s = srv->cluster->SetSlotRanges(slot_ranges_, args_[4], set_version_);
      if (s.IsOK()) {
        need_persist_nodes_info = true;
        *output = redis::SimpleString("OK");
      } else {
        return s;
      }
    } else if (subcommand_ == "version") {
      int64_t v = srv->cluster->GetVersion();
      *output = redis::BulkString(std::to_string(v));
    } else if (subcommand_ == "myid") {
      *output = redis::BulkString(srv->cluster->GetMyId());
    } else if (subcommand_ == "migrate") {
      if (sync_migrate_) {
        sync_migrate_ctx_ = std::make_unique<SyncMigrateContext>(srv, conn, sync_migrate_timeout_);
      }
      // TODO: support multiple slot ranges
      Status s = srv->cluster->MigrateSlotRange(slot_ranges_[0], dst_node_id_, sync_migrate_ctx_.get());
      if (s.IsOK()) {
        if (sync_migrate_) {
          return {Status::BlockingCmd};
        }
        *output = redis::SimpleString("OK");
      } else {
        return s;
      }
    } else {
      return {Status::RedisExecErr, "Invalid cluster command options"};
    }
    if (need_persist_nodes_info && srv->GetConfig()->persist_cluster_nodes_enabled) {
      return srv->cluster->DumpClusterNodes(srv->GetConfig()->NodesFilePath());
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
  std::string nodes_str_;
  std::string dst_node_id_;
  int64_t set_version_ = 0;
  std::vector<SlotRange> slot_ranges_;
  bool force_ = false;

  bool sync_migrate_ = false;
  int sync_migrate_timeout_ = 0;
  std::unique_ptr<SyncMigrateContext> sync_migrate_ctx_ = nullptr;
};

static uint64_t GenerateClusterFlag(uint64_t flags, const std::vector<std::string> &args) {
  if (args.size() >= 2 && Cluster::SubCommandIsExecExclusive(args[1])) {
    return flags | kCmdExclusive;
  }

  return flags;
}

class CommandReadOnly : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    *output = redis::SimpleString("OK");
    conn->EnableFlag(redis::Connection::kReadOnly);
    return Status::OK();
  }
};

class CommandReadWrite : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    *output = redis::SimpleString("OK");
    conn->DisableFlag(redis::Connection::kReadOnly);
    return Status::OK();
  }
};

class CommandAsking : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    conn->EnableFlag(redis::Connection::kAsking);
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(Cluster,
                        MakeCmdAttr<CommandCluster>("cluster", -2, "cluster no-script", NO_KEY, GenerateClusterFlag),
                        MakeCmdAttr<CommandClusterX>("clusterx", -2, "cluster no-script", NO_KEY, GenerateClusterFlag),
                        MakeCmdAttr<CommandReadOnly>("readonly", 1, "cluster no-multi", NO_KEY),
                        MakeCmdAttr<CommandReadWrite>("readwrite", 1, "cluster no-multi", NO_KEY),
                        MakeCmdAttr<CommandAsking>("asking", 1, "cluster", NO_KEY), )

}  // namespace redis
