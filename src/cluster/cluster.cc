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

#include "cluster.h"

#include <config/config_util.h>

#include <array>
#include <cstring>
#include <fstream>
#include <memory>
#include <vector>

#include "cluster/cluster_defs.h"
#include "commands/commander.h"
#include "common/io_util.h"
#include "fmt/format.h"
#include "parse_util.h"
#include "replication.h"
#include "server/server.h"
#include "string_util.h"
#include "time_util.h"

ClusterNode::ClusterNode(std::string id, std::string host, int port, int role, std::string master_id,
                         const std::bitset<kClusterSlots> &slots)
    : id(std::move(id)), host(std::move(host)), port(port), role(role), master_id(std::move(master_id)), slots(slots) {}

Cluster::Cluster(Server *srv, std::vector<std::string> binds, int port)
    : srv_(srv), binds_(std::move(binds)), port_(port) {
  for (auto &slots_node : slots_nodes_) {
    slots_node = nullptr;
  }
}

// We access cluster without lock, actually we guarantee data-safe by work threads
// ReadWriteLockGuard, CLUSTER command doesn't have 'exclusive' attribute, i.e.
// CLUSTER command can be executed concurrently, but some subcommand may change
// cluster data, so these commands should be executed exclusively, and ReadWriteLock
// also can guarantee accessing data is safe.
bool Cluster::SubCommandIsExecExclusive(const std::string &subcommand) {
  std::array subcommands = {"setnodes", "setnodeid", "setslot", "import", "reset"};

  return std::any_of(std::begin(subcommands), std::end(subcommands),
                     [&subcommand](const std::string &val) { return util::EqualICase(val, subcommand); });
}

Status Cluster::SetNodeId(const std::string &node_id) {
  if (node_id.size() != kClusterNodeIdLen) {
    return {Status::ClusterInvalidInfo, errInvalidNodeID};
  }

  myid_ = node_id;
  // Already has cluster topology
  if (version_ >= 0 && nodes_.find(node_id) != nodes_.end()) {
    myself_ = nodes_[myid_];
  } else {
    myself_ = nullptr;
  }

  // Set replication relationship
  if (myself_) return SetMasterSlaveRepl();

  return Status::OK();
}

// The reason why the new version MUST be +1 of current version is that,
// the command changes topology based on specific topology (also means specific
// version), we must guarantee current topology is exactly expected, otherwise,
// this update may make topology corrupt, so base topology version is very important.
// This is different with CLUSTERX SETNODES commands because it uses new version
// topology to cover current version, it allows kvrocks nodes lost some topology
// updates since of network failure, it is state instead of operation.
Status Cluster::SetSlotRanges(const std::vector<SlotRange> &slot_ranges, const std::string &node_id,
                              int64_t new_version) {
  if (new_version <= 0 || new_version != version_ + 1) {
    return {Status::NotOK, errInvalidClusterVersion};
  }

  if (node_id.size() != kClusterNodeIdLen) {
    return {Status::NotOK, errInvalidNodeID};
  }

  // Get the node which we want to assign slots into it
  std::shared_ptr<ClusterNode> to_assign_node = nodes_[node_id];
  if (to_assign_node == nullptr) {
    return {Status::NotOK, "No this node in the cluster"};
  }

  if (to_assign_node->role != kClusterMaster) {
    return {Status::NotOK, errNoMasterNode};
  }

  // Update version
  version_ = new_version;

  // Update topology
  //  1. Remove the slot from old node if existing
  //  2. Add the slot into to-assign node
  //  3. Update the map of slots to nodes.
  // remember: The atomicity of the process is based on
  // the transactionality of ClearKeysOfSlotRange().
  engine::Context ctx(srv_->storage);
  for (auto [s_start, s_end] : slot_ranges) {
    for (int slot = s_start; slot <= s_end; slot++) {
      std::shared_ptr<ClusterNode> old_node = slots_nodes_[slot];
      if (old_node != nullptr) {
        old_node->slots[slot] = false;
      }
      to_assign_node->slots[slot] = true;
      slots_nodes_[slot] = to_assign_node;

      // Clear data of migrated slot or record of imported slot
      if (old_node == myself_ && old_node != to_assign_node) {
        // If slot is migrated from this node
        if (migrated_slots_.count(slot) > 0) {
          auto s = srv_->slot_migrator->ClearKeysOfSlotRange(ctx, kDefaultNamespace, SlotRange::GetPoint(slot));
          if (!s.ok()) {
            LOG(ERROR) << "failed to clear data of migrated slot: " << s.ToString();
          }
          migrated_slots_.erase(slot);
        }
        // If slot is imported into this node
        if (imported_slots_.count(slot) > 0) {
          imported_slots_.erase(slot);
        }
      }
    }
  }

  return Status::OK();
}

// cluster setnodes $all_nodes_info $version $force
// one line of $all_nodes: $node_id $host $port $role $master_node_id $slot_range
Status Cluster::SetClusterNodes(const std::string &nodes_str, int64_t version, bool force) {
  if (version < 0) return {Status::NotOK, errInvalidClusterVersion};

  if (!force) {
    // Low version wants to reset current version
    if (version_ > version) {
      return {Status::NotOK, errInvalidClusterVersion};
    }

    // The same version, it is not needed to update
    if (version_ == version) return Status::OK();
  }

  ClusterNodes nodes;
  std::unordered_map<int, std::string> slots_nodes;
  Status s = parseClusterNodes(nodes_str, &nodes, &slots_nodes);
  if (!s.IsOK()) return s;

  // Update version and cluster topology
  version_ = version;
  nodes_ = nodes;
  size_ = 0;

  // Update slots to nodes
  for (const auto &[slot, node_id] : slots_nodes) {
    slots_nodes_[slot] = nodes_[node_id];
  }

  // Update replicas info and size
  for (const auto &[node_id, node] : nodes_) {
    if (node->role == kClusterSlave) {
      if (nodes_.find(node->master_id) != nodes_.end()) {
        nodes_[node->master_id]->replicas.push_back(node_id);
      }
    }
    if (node->role == kClusterMaster && node->slots.count() > 0) {
      size_++;
    }
  }

  if (myid_.empty() || force) {
    for (const auto &[node_id, node] : nodes_) {
      if (node->port == port_ && util::MatchListeningIP(binds_, node->host)) {
        myid_ = node_id;
        break;
      }
    }
  }

  myself_ = nullptr;
  if (!myid_.empty() && nodes_.find(myid_) != nodes_.end()) {
    myself_ = nodes_[myid_];
  }

  // Set replication relationship
  if (myself_) {
    s = SetMasterSlaveRepl();
    if (!s.IsOK()) {
      return s.Prefixed("failed to set master-replica replication");
    }
  }

  // Clear data of migrated slots
  if (!migrated_slots_.empty()) {
    engine::Context ctx(srv_->storage);
    for (const auto &[slot, _] : migrated_slots_) {
      if (slots_nodes_[slot] != myself_) {
        auto s = srv_->slot_migrator->ClearKeysOfSlotRange(ctx, kDefaultNamespace, SlotRange::GetPoint(slot));
        if (!s.ok()) {
          LOG(ERROR) << "failed to clear data of migrated slots: " << s.ToString();
        }
      }
    }
  }
  // Clear migrated and imported slot info
  migrated_slots_.clear();
  imported_slots_.clear();

  return Status::OK();
}

// Set replication relationship by cluster topology setting
Status Cluster::SetMasterSlaveRepl() {
  if (!srv_) return Status::OK();

  if (!myself_) return Status::OK();

  if (myself_->role == kClusterMaster) {
    // Master mode
    auto s = srv_->RemoveMaster();
    if (!s.IsOK()) {
      return s.Prefixed("failed to remove master");
    }
    LOG(INFO) << "MASTER MODE enabled by cluster topology setting";
  } else if (nodes_.find(myself_->master_id) != nodes_.end()) {
    // Replica mode and master node is existing
    std::shared_ptr<ClusterNode> master = nodes_[myself_->master_id];
    auto s = srv_->AddMaster(master->host, master->port, false);
    if (!s.IsOK()) {
      LOG(WARNING) << "SLAVE OF " << master->host << ":" << master->port
                   << " wasn't enabled by cluster topology setting, encounter error: " << s.Msg();
      return s.Prefixed("failed to add master");
    }
    LOG(INFO) << "SLAVE OF " << master->host << ":" << master->port << " enabled by cluster topology setting";
  }

  return Status::OK();
}

bool Cluster::IsNotMaster() { return myself_ == nullptr || myself_->role != kClusterMaster || srv_->IsSlave(); }

Status Cluster::SetSlotRangeMigrated(const SlotRange &slot_range, const std::string &ip_port) {
  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }

  // It is called by slot-migrating thread which is an asynchronous thread.
  // Therefore, it should be locked when a record is added to 'migrated_slots_'
  // which will be accessed when executing commands.
  auto exclusivity = srv_->WorkExclusivityGuard();
  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    migrated_slots_[slot] = ip_port;
  }
  return Status::OK();
}

Status Cluster::SetSlotRangeImported(const SlotRange &slot_range) {
  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }

  // It is called by command 'cluster import'. When executing the command, the
  // exclusive lock has been locked. Therefore, it can't be locked again.
  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    imported_slots_.insert(slot);
  }
  return Status::OK();
}

Status Cluster::MigrateSlotRange(const SlotRange &slot_range, const std::string &dst_node_id,
                                 SyncMigrateContext *blocking_ctx) {
  if (nodes_.find(dst_node_id) == nodes_.end()) {
    return {Status::NotOK, "Can't find the destination node id"};
  }

  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }

  if (!migrated_slots_.empty() &&
      slot_range.HasOverlap({migrated_slots_.begin()->first, migrated_slots_.rbegin()->first})) {
    return {Status::NotOK, "Can't migrate slot which has been migrated"};
  }

  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    if (slots_nodes_[slot] != myself_) {
      return {Status::NotOK, "Can't migrate slot which doesn't belong to me"};
    }
  }

  if (IsNotMaster()) {
    return {Status::NotOK, "Slave can't migrate slot"};
  }

  if (nodes_[dst_node_id]->role != kClusterMaster) {
    return {Status::NotOK, "Can't migrate slot to a slave"};
  }

  if (nodes_[dst_node_id] == myself_) {
    return {Status::NotOK, "Can't migrate slot to myself"};
  }

  const auto &dst = nodes_[dst_node_id];
  Status s =
      srv_->slot_migrator->PerformSlotRangeMigration(dst_node_id, dst->host, dst->port, slot_range, blocking_ctx);
  return s;
}

Status Cluster::ImportSlotRange(redis::Connection *conn, const SlotRange &slot_range, int state) {
  if (IsNotMaster()) {
    return {Status::NotOK, "Slave can't import slot"};
  }

  if (!slot_range.IsValid()) {
    return {Status::NotOK, errSlotRangeInvalid};
  }

  for (auto slot = slot_range.start; slot <= slot_range.end; slot++) {
    auto source_node = srv_->cluster->slots_nodes_[slot];
    if (source_node && source_node->id == myid_) {
      return {Status::NotOK, "Can't import slot which belongs to me"};
    }
  }

  Status s;
  switch (state) {
    case kImportStart:
      s = srv_->slot_import->Start(slot_range);
      if (!s.IsOK()) return s;

      // Set link importing
      conn->SetImporting();
      myself_->importing_slot_range = slot_range;
      // Set link error callback
      conn->close_cb = [object_ptr = srv_->slot_import.get(), slot_range]([[maybe_unused]] int fd) {
        auto s = object_ptr->StopForLinkError();
        if (!s.IsOK()) {
          LOG(ERROR) << fmt::format("[import] Failed to stop importing slot(s) {}: {}", slot_range.String(), s.Msg());
        }
      };  // Stop forbidding writing slot to accept write commands
      if (slot_range == srv_->slot_migrator->GetForbiddenSlotRange()) srv_->slot_migrator->ReleaseForbiddenSlotRange();
      LOG(INFO) << fmt::format("[import] Start importing slot(s) {}", slot_range.String());
      break;
    case kImportSuccess:
      s = srv_->slot_import->Success(slot_range);
      if (!s.IsOK()) return s;
      LOG(INFO) << fmt::format("[import] Mark the importing slot(s) {} as succeed", slot_range.String());
      break;
    case kImportFailed:
      s = srv_->slot_import->Fail(slot_range);
      if (!s.IsOK()) return s;
      LOG(INFO) << fmt::format("[import] Mark the importing slot(s) {} as failed", slot_range.String());
      break;
    default:
      return {Status::NotOK, errInvalidImportState};
  }

  return Status::OK();
}

Status Cluster::GetClusterInfo(std::string *cluster_infos) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  cluster_infos->clear();

  int ok_slot = 0;
  for (auto &slots_node : slots_nodes_) {
    if (slots_node != nullptr) ok_slot++;
  }

  *cluster_infos =
      "cluster_state:ok\r\n"
      "cluster_slots_assigned:" +
      std::to_string(ok_slot) +
      "\r\n"
      "cluster_slots_ok:" +
      std::to_string(ok_slot) +
      "\r\n"
      "cluster_slots_pfail:0\r\n"
      "cluster_slots_fail:0\r\n"
      "cluster_known_nodes:" +
      std::to_string(nodes_.size()) +
      "\r\n"
      "cluster_size:" +
      std::to_string(size_) +
      "\r\n"
      "cluster_current_epoch:" +
      std::to_string(version_) +
      "\r\n"
      "cluster_my_epoch:" +
      std::to_string(version_) + "\r\n";

  if (myself_ != nullptr && myself_->role == kClusterMaster && !srv_->IsSlave()) {
    // Get migrating status
    std::string migrate_infos;
    srv_->slot_migrator->GetMigrationInfo(&migrate_infos);
    *cluster_infos += migrate_infos;

    // Get importing status
    std::string import_infos;
    srv_->slot_import->GetImportInfo(&import_infos);
    *cluster_infos += import_infos;
  }

  return Status::OK();
}

// Format: 1) 1) start slot
//            2) end slot
//            3) 1) master IP
//               2) master port
//               3) node ID
//            4) 1) replica IP
//               2) replica port
//               3) node ID
//          ... continued until done
Status Cluster::GetSlotsInfo(std::vector<SlotInfo> *slots_infos) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  slots_infos->clear();

  int start = -1;
  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      slots_infos->emplace_back(genSlotNodeInfo(start, i - 1, n));
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  return Status::OK();
}

SlotInfo Cluster::genSlotNodeInfo(int start, int end, const std::shared_ptr<ClusterNode> &n) {
  std::vector<SlotInfo::NodeInfo> vn;
  vn.push_back({n->host, n->port, n->id});  // itself

  for (const auto &id : n->replicas) {  // replicas
    if (nodes_.find(id) == nodes_.end()) continue;
    vn.push_back({nodes_[id]->host, nodes_[id]->port, nodes_[id]->id});
  }

  return {start, end, vn};
}

// $node $host:$port@$cport $role $master_id/$- $ping_sent $ping_received
// $version $connected $slot_range
Status Cluster::GetClusterNodes(std::string *nodes_str) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  *nodes_str = genNodesDescription();
  return Status::OK();
}

StatusOr<std::string> Cluster::GetReplicas(const std::string &node_id) {
  if (version_ < 0) {
    return {Status::RedisClusterDown, errClusterNoInitialized};
  }

  auto item = nodes_.find(node_id);
  if (item == nodes_.end()) {
    return {Status::InvalidArgument, errInvalidNodeID};
  }

  auto node = item->second;
  if (node->role != kClusterMaster) {
    return {Status::InvalidArgument, errNoMasterNode};
  }

  auto now = util::GetTimeStampMS();
  std::string replicas_desc;
  for (const auto &replica_id : node->replicas) {
    auto n = nodes_.find(replica_id);
    if (n == nodes_.end()) {
      continue;
    }

    auto replica = n->second;

    std::string node_str;
    // ID, host, port
    node_str.append(
        fmt::format("{} {}:{}@{} ", replica_id, replica->host, replica->port, replica->port + kClusterPortIncr));

    // Flags
    node_str.append(fmt::format("slave {} ", node_id));

    // Ping sent, pong received, config epoch, link status
    node_str.append(fmt::format("{} {} {} connected", now - 1, now, version_));

    replicas_desc.append(node_str + "\n");
  }

  return replicas_desc;
}

std::string Cluster::getNodeIDBySlot(int slot) const {
  if (slot < 0 || slot >= kClusterSlots || !slots_nodes_[slot]) return "";
  return slots_nodes_[slot]->id;
}

std::string Cluster::genNodesDescription() {
  auto slots_infos = getClusterNodeSlots();

  auto now = util::GetTimeStampMS();
  std::string nodes_desc;
  for (const auto &[_, node] : nodes_) {
    std::string node_str;
    // ID, host, port
    node_str.append(node->id + " ");
    node_str.append(fmt::format("{}:{}@{} ", node->host, node->port, node->port + kClusterPortIncr));

    // Flags
    if (node->id == myid_) node_str.append("myself,");
    if (node->role == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + node->master_id + " ");
    }

    // Ping sent, pong received, config epoch, link status
    node_str.append(fmt::format("{} {} {} connected", now - 1, now, version_));

    if (node->role == kClusterMaster) {
      auto iter = slots_infos.find(node->id);
      if (iter != slots_infos.end() && !iter->second.empty()) {
        node_str.append(" " + iter->second);
      }
    }

    // Just for MYSELF node to show the importing/migrating slot
    if (node->id == myid_) {
      if (srv_->slot_migrator) {
        auto migrating_slot_range = srv_->slot_migrator->GetMigratingSlotRange();
        if (migrating_slot_range.IsValid()) {
          node_str.append(fmt::format(" [{}->-{}]", migrating_slot_range.String(), srv_->slot_migrator->GetDstNode()));
        }
      }
      if (srv_->slot_import) {
        auto importing_slot_range = srv_->slot_import->GetSlotRange();
        if (importing_slot_range.IsValid()) {
          node_str.append(
              fmt::format(" [{}-<-{}]", importing_slot_range.String(), getNodeIDBySlot(importing_slot_range.start)));
        }
      }
    }
    nodes_desc.append(node_str + "\n");
  }
  return nodes_desc;
}

std::map<std::string, std::string, std::less<>> Cluster::getClusterNodeSlots() const {
  int start = -1;
  // node id => slots info string
  std::map<std::string, std::string, std::less<>> slots_infos;

  std::shared_ptr<ClusterNode> n = nullptr;
  for (int i = 0; i <= kClusterSlots; i++) {
    // Find start node and slot id
    if (n == nullptr) {
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
      continue;
    }
    // Generate slots info when occur different node with start or end of slot
    if (i == kClusterSlots || n != slots_nodes_[i]) {
      if (start == i - 1) {
        slots_infos[n->id] += fmt::format("{} ", start);
      } else {
        slots_infos[n->id] += fmt::format("{}-{} ", start, i - 1);
      }
      if (i == kClusterSlots) break;
      n = slots_nodes_[i];
      start = i;
    }
  }

  for (auto &[_, info] : slots_infos) {
    if (info.size() > 0) info.pop_back();  // Remove last space
  }
  return slots_infos;
}

std::string Cluster::genNodesInfo() const {
  auto slots_infos = getClusterNodeSlots();

  std::string nodes_info;
  for (const auto &[_, node] : nodes_) {
    std::string node_str;
    node_str.append("node ");
    // ID
    node_str.append(node->id + " ");
    // Host + Port
    node_str.append(fmt::format("{} {} ", node->host, node->port));

    // Role
    if (node->role == kClusterMaster) {
      node_str.append("master - ");
    } else {
      node_str.append("slave " + node->master_id + " ");
    }

    // Slots
    if (node->role == kClusterMaster) {
      auto iter = slots_infos.find(node->id);
      if (iter != slots_infos.end() && !iter->second.empty()) {
        node_str.append(" " + iter->second);
      }
    }
    nodes_info.append(node_str + "\n");
  }
  return nodes_info;
}

Status Cluster::DumpClusterNodes(const std::string &file) {
  // Parse and validate the cluster nodes string before dumping into file
  std::string tmp_path = file + ".tmp";
  remove(tmp_path.data());
  std::ofstream output_file(tmp_path, std::ios::out);
  output_file << fmt::format("version {}\n", version_);
  output_file << fmt::format("id {}\n", myid_);
  output_file << genNodesInfo();
  output_file.close();
  if (rename(tmp_path.data(), file.data()) < 0) {
    return {Status::NotOK, fmt::format("rename file encounter error: {}", strerror(errno))};
  }
  return Status::OK();
}

Status Cluster::LoadClusterNodes(const std::string &file_path) {
  if (rocksdb::Env::Default()->FileExists(file_path).IsNotFound()) {
    LOG(INFO) << fmt::format("The cluster nodes file {} is not found. Use CLUSTERX subcommands to specify it.",
                             file_path);
    return Status::OK();
  }

  std::ifstream file;
  file.open(file_path);
  if (!file.is_open()) {
    return {Status::NotOK, fmt::format("error opening the file '{}': {}", file_path, strerror(errno))};
  }

  int64_t version = -1;
  std::string id, nodes_info;
  std::string line;
  while (file.good() && std::getline(file, line)) {
    auto parsed = ParseConfigLine(line);
    if (!parsed) return parsed.ToStatus().Prefixed("malformed line");
    if (parsed->first.empty() || parsed->second.empty()) continue;

    auto key = parsed->first;
    if (key == "version") {
      auto parse_result = ParseInt<int64_t>(parsed->second, 10);
      if (!parse_result) {
        return {Status::NotOK, errInvalidClusterVersion};
      }
      version = *parse_result;
    } else if (key == "id") {
      id = parsed->second;
      if (id.length() != kClusterNodeIdLen) {
        return {Status::NotOK, errInvalidNodeID};
      }
    } else if (key == "node") {
      nodes_info.append(parsed->second + "\n");
    } else {
      return {Status::NotOK, fmt::format("unknown key: {}", key)};
    }
  }

  myid_ = id;
  return SetClusterNodes(nodes_info, version, false);
}

Status Cluster::parseClusterNodes(const std::string &nodes_str, ClusterNodes *nodes,
                                  std::unordered_map<int, std::string> *slots_nodes) {
  std::vector<std::string> nodes_info = util::Split(nodes_str, "\n");
  if (nodes_info.empty()) {
    return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
  }

  nodes->clear();

  // Parse all nodes
  for (const auto &node_str : nodes_info) {
    std::vector<std::string> fields = util::Split(node_str, " ");
    if (fields.size() < 5) {
      return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
    }

    // 1) node id
    if (fields[0].size() != kClusterNodeIdLen) {
      return {Status::ClusterInvalidInfo, errInvalidNodeID};
    }

    std::string id = fields[0];

    // 2) host, TODO(@shooterit): check host is valid
    std::string host = fields[1];

    // 3) port
    auto parse_result = ParseInt<uint16_t>(fields[2], 10);
    if (!parse_result) {
      return {Status::ClusterInvalidInfo, "Invalid cluster node port"};
    }

    int port = *parse_result;

    // 4) role
    int role = 0;
    if (util::EqualICase(fields[3], "master")) {
      role = kClusterMaster;
    } else if (util::EqualICase(fields[3], "slave") || util::EqualICase(fields[3], "replica")) {
      role = kClusterSlave;
    } else {
      return {Status::ClusterInvalidInfo, "Invalid cluster node role"};
    }

    // 5) master id
    std::string master_id = fields[4];
    if ((role == kClusterMaster && master_id != "-") ||
        (role == kClusterSlave && master_id.size() != kClusterNodeIdLen)) {
      return {Status::ClusterInvalidInfo, errInvalidNodeID};
    }

    std::bitset<kClusterSlots> slots;
    if (role == kClusterSlave) {
      if (fields.size() != 5) {
        return {Status::ClusterInvalidInfo, errInvalidClusterNodeInfo};
      } else {
        // Create slave node
        (*nodes)[id] = std::make_shared<ClusterNode>(id, host, port, role, master_id, slots);
        continue;
      }
    }

    // 6) slot info
    auto valid_range = NumericRange<int>{0, kClusterSlots - 1};
    const std::regex node_id_regex(R"(\b[a-fA-F0-9]{40}\b)");
    for (unsigned i = 5; i < fields.size(); i++) {
      std::vector<std::string> ranges = util::Split(fields[i], "-");
      if (ranges.size() == 1) {
        if (std::regex_match(fields[i], node_id_regex)) {
          return {Status::ClusterInvalidInfo, "Invalid nodes definition: Missing newline between node entries."};
        }

        auto parse_start = ParseInt<int>(ranges[0], valid_range, 10);
        if (!parse_start) {
          return {Status::ClusterInvalidInfo, errSlotOutOfRange};
        }

        int start = *parse_start;
        slots.set(start, true);
        if (role == kClusterMaster) {
          if (slots_nodes->find(start) != slots_nodes->end()) {
            return {Status::ClusterInvalidInfo, errSlotOverlapped};
          } else {
            (*slots_nodes)[start] = id;
          }
        }
      } else if (ranges.size() == 2) {
        auto parse_start = ParseInt<int>(ranges[0], valid_range, 10);
        auto parse_stop = ParseInt<int>(ranges[1], valid_range, 10);
        if (!parse_start || !parse_stop || *parse_start >= *parse_stop) {
          return {Status::ClusterInvalidInfo, errSlotOutOfRange};
        }

        int start = *parse_start;
        int stop = *parse_stop;
        for (int j = start; j <= stop; j++) {
          slots.set(j, true);
          if (role == kClusterMaster) {
            if (slots_nodes->find(j) != slots_nodes->end()) {
              return {Status::ClusterInvalidInfo, errSlotOverlapped};
            } else {
              (*slots_nodes)[j] = id;
            }
          }
        }
      } else {
        return {Status::ClusterInvalidInfo, errSlotOutOfRange};
      }
    }

    // Create master node
    (*nodes)[id] = std::make_shared<ClusterNode>(id, host, port, role, master_id, slots);
  }

  return Status::OK();
}

bool Cluster::IsWriteForbiddenSlot(int slot) const {
  return srv_->slot_migrator->GetForbiddenSlotRange().Contains(slot);
}

Status Cluster::CanExecByMySelf(const redis::CommandAttributes *attributes, const std::vector<std::string> &cmd_tokens,
                                redis::Connection *conn, lua::ScriptRunCtx *script_run_ctx) {
  std::vector<int> keys_indexes;

  // No keys
  if (auto s = redis::CommandTable::GetKeysFromCommand(attributes, cmd_tokens, &keys_indexes); !s.IsOK())
    return Status::OK();

  if (keys_indexes.empty()) return Status::OK();

  int slot = -1;
  for (auto i : keys_indexes) {
    if (i >= static_cast<int>(cmd_tokens.size())) break;

    int cur_slot = GetSlotIdFromKey(cmd_tokens[i]);
    if (slot == -1) slot = cur_slot;
    if (slot != cur_slot) {
      return {Status::RedisCrossSlot, "Attempted to access keys that don't hash to the same slot"};
    }
  }
  if (slot == -1) return Status::OK();

  if (slots_nodes_[slot] == nullptr) {
    return {Status::RedisClusterDown, "Hash slot not served"};
  }

  bool cross_slot_ok = false;
  if (script_run_ctx) {
    if (script_run_ctx->current_slot != -1 && script_run_ctx->current_slot != slot) {
      if (getNodeIDBySlot(script_run_ctx->current_slot) != getNodeIDBySlot(slot)) {
        return {Status::RedisMoved, fmt::format("{} {}:{}", slot, slots_nodes_[slot]->host, slots_nodes_[slot]->port)};
      }
      if (!(script_run_ctx->flags & lua::ScriptFlagType::kScriptAllowCrossSlotKeys)) {
        return {Status::RedisCrossSlot, "Script attempted to access keys that do not hash to the same slot"};
      }
    }

    script_run_ctx->current_slot = slot;
    cross_slot_ok = true;
  }

  if (myself_ && myself_ == slots_nodes_[slot]) {
    // We use central controller to manage the topology of the cluster.
    // Server can't change the topology directly, so we record the migrated slots
    // to move the requests of the migrated slots to the destination node.
    if (migrated_slots_.count(slot) > 0) {  // I'm not serving the migrated slot
      return {Status::RedisMoved, fmt::format("{} {}", slot, migrated_slots_[slot])};
    }
    // To keep data consistency, slot will be forbidden write while sending the last incremental data.
    // During this phase, the requests of the migrating slot has to be rejected.
    if ((attributes->flags & redis::kCmdWrite) && IsWriteForbiddenSlot(slot)) {
      return {Status::RedisTryAgain, "Can't write to slot being migrated which is in write forbidden phase"};
    }

    return Status::OK();  // I'm serving this slot
  }

  if (myself_ && myself_->importing_slot_range.Contains(slot) &&
      (conn->IsImporting() || conn->IsFlagEnabled(redis::Connection::kAsking))) {
    // While data migrating, the topology of the destination node has not been changed.
    // The destination node has to serve the requests from the migrating slot,
    // although the slot is not belong to itself. Therefore, we record the importing slot
    // and mark the importing connection to accept the importing data.
    return Status::OK();  // I'm serving the importing connection or asking connection
  }

  if (myself_ && imported_slots_.count(slot)) {
    // After the slot is migrated, new requests of the migrated slot will be moved to
    // the destination server. Before the central controller change the topology, the destination
    // server should record the imported slots to accept new data of the imported slots.
    return Status::OK();  // I'm serving the imported slot
  }

  if (myself_ && myself_->role == kClusterSlave && !(attributes->flags & redis::kCmdWrite) &&
      nodes_.find(myself_->master_id) != nodes_.end() && nodes_[myself_->master_id] == slots_nodes_[slot] &&
      conn->IsFlagEnabled(redis::Connection::kReadOnly)) {
    return Status::OK();  // My master is serving this slot
  }

  if (!cross_slot_ok) {
    return {Status::RedisMoved, fmt::format("{} {}:{}", slot, slots_nodes_[slot]->host, slots_nodes_[slot]->port)};
  }

  return Status::OK();
}

// Only HARD mode is meaningful to the Kvrocks cluster,
// so it will force clearing all information after resetting.
Status Cluster::Reset() {
  if (srv_->slot_migrator && srv_->slot_migrator->GetMigratingSlotRange().IsValid()) {
    return {Status::NotOK, "Can't reset cluster while migrating slot"};
  }
  if (srv_->slot_import && srv_->slot_import->GetSlotRange().IsValid()) {
    return {Status::NotOK, "Can't reset cluster while importing slot"};
  }
  if (!srv_->storage->IsEmptyDB()) {
    return {Status::NotOK, "Can't reset cluster while database is not empty"};
  }
  if (srv_->IsSlave()) {
    auto s = srv_->RemoveMaster();
    if (!s.IsOK()) return s;
  }

  version_ = -1;
  size_ = 0;
  myid_.clear();
  myself_.reset();

  nodes_.clear();
  for (auto &n : slots_nodes_) {
    n = nullptr;
  }
  migrated_slots_.clear();
  imported_slots_.clear();

  // unlink the cluster nodes file if exists
  unlink(srv_->GetConfig()->NodesFilePath().data());
  return Status::OK();
}
