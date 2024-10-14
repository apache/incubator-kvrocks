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

#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>
#include <rocksdb/types.h>
#include <rocksdb/utilities/backup_engine.h>

#include <deque>
#include <initializer_list>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cluster/cluster_defs.h"
#include "error_constants.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "status.h"
#include "string_util.h"

class Server;

namespace engine {
struct Context;
}

namespace redis {

class Connection;
struct CommandAttributes;

enum CommandFlags : uint64_t {
  kCmdWrite = 1ULL << 0,           // "write" flag
  kCmdReadOnly = 1ULL << 1,        // "read-only" flag
  kCmdReplication = 1ULL << 2,     // "replication" flag
  kCmdPubSub = 1ULL << 3,          // "pub-sub" flag
  kCmdScript = 1ULL << 4,          // "script" flag
  kCmdLoading = 1ULL << 5,         // "ok-loading" flag
  kCmdMulti = 1ULL << 6,           // "multi" flag
  kCmdExclusive = 1ULL << 7,       // "exclusive" flag
  kCmdNoMulti = 1ULL << 8,         // "no-multi" flag
  kCmdNoScript = 1ULL << 9,        // "no-script" flag
  kCmdROScript = 1ULL << 10,       // "ro-script" flag for read-only script commands
  kCmdCluster = 1ULL << 11,        // "cluster" flag
  kCmdNoDBSizeCheck = 1ULL << 12,  // "no-dbsize-check" flag
  kCmdSlow = 1ULL << 13,           // "slow" flag
};

enum class CommandCategory : uint8_t {
  Unknown = 0,
  Bit,
  BloomFilter,
  Cluster,
  Function,
  Geo,
  Hash,
  HLL,
  JSON,
  Key,
  List,
  Pubsub,
  Replication,
  Script,
  Search,
  Server,
  Set,
  SortedInt,
  Stream,
  String,
  Txn,
  ZSet,
};

class Commander {
 public:
  void SetAttributes(const CommandAttributes *attributes) { attributes_ = attributes; }
  const CommandAttributes *GetAttributes() const { return attributes_; }
  void SetArgs(const std::vector<std::string> &args) { args_ = args; }
  virtual Status Parse() { return Parse(args_); }
  virtual Status Parse([[maybe_unused]] const std::vector<std::string> &args) { return Status::OK(); }
  virtual Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv,
                         [[maybe_unused]] Connection *conn, [[maybe_unused]] std::string *output) {
    return {Status::RedisExecErr, errNotImplemented};
  }

  virtual ~Commander() = default;

 protected:
  std::vector<std::string> args_;
  const CommandAttributes *attributes_ = nullptr;
};

class CommanderWithParseMove : Commander {
 public:
  Status Parse() override { return ParseMove(std::move(args_)); }
  virtual Status ParseMove([[maybe_unused]] std::vector<std::string> &&args) { return Status::OK(); }
};

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;

struct CommandKeyRange {
  // index of the first key in command tokens
  // 0 stands for no key, since the first index of command arguments is command name
  int first_key;

  // index of the last key in command tokens
  // in normal one-key commands, first key and last key index are both 1
  // -n stands for the n-th last index of the sequence, i.e. args.size() - n
  int last_key;

  // step length of key position
  // e.g. key step 2 means "key other key other ..." sequence
  int key_step;

  template <typename F>
  void ForEachKey(F &&f, const std::vector<std::string> &args) const {
    for (size_t i = first_key; last_key > 0 ? i <= size_t(last_key) : i <= args.size() + last_key; i += key_step) {
      std::forward<F>(f)(args[i]);
    }
  }
};

using CommandKeyRangeGen = std::function<CommandKeyRange(const std::vector<std::string> &)>;

using CommandKeyRangeVecGen = std::function<std::vector<CommandKeyRange>(const std::vector<std::string> &)>;

using AdditionalFlagGen = std::function<uint64_t(uint64_t, const std::vector<std::string> &)>;

struct CommandAttributes {
  // command name
  std::string name;

  // number of command arguments
  // positive number n means number of arguments is equal to n
  // negative number -n means number of arguments is equal to or large than n
  int arity;

  // category of this command, e.g. key, string, hash
  CommandCategory category;

  // bitmap of enum CommandFlags
  uint64_t flags;

  // additional flags regarding to dynamic command arguments
  AdditionalFlagGen flag_gen;

  // static determined key range
  CommandKeyRange key_range;

  // if key_range.first_key == -1, key_range_gen is used instead
  CommandKeyRangeGen key_range_gen;

  // if key_range.first_key == -2, key_range_vec_gen is used instead
  CommandKeyRangeVecGen key_range_vec_gen;

  // commander object generator
  CommanderFactory factory;

  auto GenerateFlags(const std::vector<std::string> &args) const {
    uint64_t res = flags;
    if (flag_gen) res = flag_gen(res, args);
    return res;
  }

  bool CheckArity(int cmd_size) const {
    return !((arity > 0 && cmd_size != arity) || (arity < 0 && cmd_size < -arity));
  }

  template <typename F>
  void ForEachKeyRange(F &&f, const std::vector<std::string> &args) const {
    if (key_range.first_key > 0) {
      std::forward<F>(f)(args, key_range);
    } else if (key_range.first_key == -1) {
      redis::CommandKeyRange range = key_range_gen(args);

      if (range.first_key > 0) {
        std::forward<F>(f)(args, range);
      }
    } else if (key_range.first_key == -2) {
      std::vector<redis::CommandKeyRange> vec_range = key_range_vec_gen(args);

      for (const auto &range : vec_range) {
        if (range.first_key > 0) {
          std::forward<F>(f)(args, range);
        }
      }
    }
  }
};

using CommandMap = std::map<std::string, const CommandAttributes *>;

inline uint64_t ParseCommandFlags(const std::string &description, const std::string &cmd_name) {
  uint64_t flags = 0;

  for (const auto &flag : util::Split(description, " ")) {
    if (flag == "write")
      flags |= kCmdWrite;
    else if (flag == "read-only")
      flags |= kCmdReadOnly;
    else if (flag == "replication")
      flags |= kCmdReplication;
    else if (flag == "pub-sub")
      flags |= kCmdPubSub;
    else if (flag == "ok-loading")
      flags |= kCmdLoading;
    else if (flag == "exclusive")
      flags |= kCmdExclusive;
    else if (flag == "multi")
      flags |= kCmdMulti;
    else if (flag == "no-multi")
      flags |= kCmdNoMulti;
    else if (flag == "no-script")
      flags |= kCmdNoScript;
    else if (flag == "ro-script")
      flags |= kCmdROScript;
    else if (flag == "cluster")
      flags |= kCmdCluster;
    else if (flag == "no-dbsize-check")
      flags |= kCmdNoDBSizeCheck;
    else if (flag == "slow")
      flags |= kCmdSlow;
    else {
      std::cout << fmt::format("Encountered non-existent flag '{}' in command {} in command attribute parsing", flag,
                               cmd_name)
                << std::endl;
      std::abort();
    }
  }

  return flags;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, int first_key, int last_key,
                 int key_step = 1, const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         CommandCategory::Unknown,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         {first_key, last_key, key_step},
                         {},
                         {},
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  if ((first_key > 0 && key_step <= 0) || (first_key > 0 && last_key >= 0 && last_key < first_key)) {
    std::cout << fmt::format("Encountered invalid key range in command {}", name) << std::endl;
    std::abort();
  }

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description, const CommandKeyRangeGen &gen,
                 const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         CommandCategory::Unknown,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         {-1, 0, 0},
                         gen,
                         {},
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  return attr;
}

template <typename T>
auto MakeCmdAttr(const std::string &name, int arity, const std::string &description,
                 const CommandKeyRangeVecGen &vec_gen, const AdditionalFlagGen &flag_gen = {}) {
  CommandAttributes attr{name,
                         arity,
                         CommandCategory::Unknown,
                         ParseCommandFlags(description, name),
                         flag_gen,
                         {-2, 0, 0},
                         {},
                         vec_gen,
                         []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new T()); }};

  return attr;
}

struct RegisterToCommandTable {
  RegisterToCommandTable(CommandCategory category, std::initializer_list<CommandAttributes> list);
};

struct CommandTable {
 public:
  CommandTable() = delete;

  static CommandMap *Get();
  static const CommandMap *GetOriginal();
  static void Reset();

  static void GetAllCommandsInfo(std::string *info);
  static void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names);
  static std::string GetCommandInfo(const CommandAttributes *command_attributes);
  static Status GetKeysFromCommand(const CommandAttributes *attributes, const std::vector<std::string> &cmd_tokens,
                                   std::vector<int> *keys_indexes);

  static size_t Size();
  static bool IsExists(const std::string &name);

  static Status ParseSlotRanges(const std::string &slots_str, std::vector<SlotRange> &slots);

 private:
  static inline std::deque<CommandAttributes> redis_command_table;

  // Original Command table before rename-command directive
  static inline CommandMap original_commands;

  // Command table after rename-command directive
  static inline CommandMap commands;

  friend struct RegisterToCommandTable;
};

#define KVROCKS_CONCAT(a, b) a##b                   // NOLINT
#define KVROCKS_CONCAT2(a, b) KVROCKS_CONCAT(a, b)  // NOLINT

// NOLINTNEXTLINE
#define REDIS_REGISTER_COMMANDS(cat, ...)                                                                   \
  static RegisterToCommandTable KVROCKS_CONCAT2(register_to_command_table_, __LINE__)(CommandCategory::cat, \
                                                                                      {__VA_ARGS__});

}  // namespace redis
