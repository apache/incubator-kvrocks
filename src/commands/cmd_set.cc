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

#include <cstdint>

#include "commander.h"
#include "commands/scan_base.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_set.h"

namespace redis {

class CommandSAdd : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> members;
    for (unsigned int i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.Add(ctx, args_[1], members, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSRem : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.Remove(ctx, args_[1], members, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSCard : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    uint64_t ret = 0;

    auto s = set_db.Card(ctx, args_[1], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSMembers : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> members;

    auto s = set_db.Members(ctx, args_[1], &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = conn->SetOfBulkStrings(members);
    return Status::OK();
  }
};

class CommandSIsMember : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    bool ret = false;

    auto s = set_db.IsMember(ctx, args_[1], args_[2], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret ? 1 : 0);
    return Status::OK();
  }
};

class CommandSMIsMember : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    std::vector<Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    std::vector<int> exists;

    auto s = set_db.MIsMember(ctx, args_[1], members, &exists);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      exists.resize(members.size(), 0);
    }

    output->append(redis::MultiLen(exists.size()));
    for (const auto &exist : exists) {
      output->append(redis::Integer(exist));
    }

    return Status::OK();
  }
};

class CommandSPop : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    if (args.size() == 3) {
      auto parse_result = ParseInt<int>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      if (*parse_result < 0) {
        return {Status::RedisParseErr, errValueMustBePositive};
      }

      count_ = *parse_result;
      with_count_ = true;
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> members;

    auto s = set_db.Take(ctx, args_[1], &members, count_, true);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (with_count_) {
      *output = conn->SetOfBulkStrings(members);
    } else {
      if (members.size() > 0) {
        *output = redis::BulkString(members.front());
      } else {
        *output = conn->NilString();
      }
    }
    return Status::OK();
  }

 private:
  int count_ = 1;
  bool with_count_ = false;
};

class CommandSRandMember : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    if (args.size() == 3) {
      auto parse_result = ParseInt<int>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> members;

    auto s = set_db.Take(ctx, args_[1], &members, count_, false);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = conn->SetOfBulkStrings(members);
    return Status::OK();
  }

 private:
  int count_ = 1;
};

class CommandSMove : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    bool ret = false;

    auto s = set_db.Move(ctx, args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret ? 1 : 0);
    return Status::OK();
  }
};

class CommandSDiff : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    std::vector<std::string> members;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.Diff(ctx, keys, &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = conn->SetOfBulkStrings(members);
    return Status::OK();
  }
};

class CommandSUnion : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    std::vector<std::string> members;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.Union(ctx, keys, &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = conn->SetOfBulkStrings(members);
    return Status::OK();
  }
};

class CommandSInter : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    std::vector<std::string> members;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.Inter(ctx, keys, &members);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = conn->SetOfBulkStrings(members);
    return Status::OK();
  }
};

/*
 * description:
 *    syntax:   `SINTERCARD numkeys key [key ...] [LIMIT limit]`
 *
 *    limit:    the valid limit is an non-negative integer.
 */
class CommandSInterCard : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_numkey = ParseInt<int>(args[1], 10);
    if (!parse_numkey) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*parse_numkey <= 0) {
      return {Status::RedisParseErr, errValueMustBePositive};
    }
    numkeys_ = *parse_numkey;

    // command: for example, SINTERCARD 2 key1 key2 LIMIT 1
    auto arg_sz = args.size();
    if (arg_sz == numkeys_ + 4 && util::ToLower(args[numkeys_ + 2]) == "limit") {
      auto parse_limit = ParseInt<int>(args[numkeys_ + 3], 10);
      if (!parse_limit) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      if (*parse_limit < 0) {
        return {Status::RedisParseErr, errLimitIsNegative};
      }
      limit_ = *parse_limit;
      return Commander::Parse(args);
    }

    if (arg_sz != numkeys_ + 2) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 2; i < numkeys_ + 2; i++) {
      keys.emplace_back(args_[i]);
    }

    redis::Set set_db(srv->storage, conn->GetNamespace());
    uint64_t ret = 0;

    auto s = set_db.InterCard(ctx, keys, limit_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

  static CommandKeyRange Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[1], 10);
    return {2, 1 + num_key, 1};
  }

 private:
  uint64_t numkeys_ = 0;
  uint64_t limit_ = 0;
};

class CommandSDiffStore : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.DiffStore(ctx, args_[1], keys, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSUnionStore : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.UnionStore(ctx, args_[1], keys, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSInterStore : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> keys;
    for (size_t i = 2; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Set set_db(srv->storage, conn->GetNamespace());

    auto s = set_db.InterStore(ctx, args_[1], keys, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSScan : public CommandSubkeyScanBase {
 public:
  CommandSScan() = default;
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Set set_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> members;
    auto key_name = srv->GetKeyNameFromCursor(cursor_, CursorType::kTypeSet);

    auto s = set_db.Scan(ctx, key_, key_name, limit_, prefix_, &members);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = CommandScanBase::GenerateOutput(srv, conn, members, CursorType::kTypeSet);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(Set, MakeCmdAttr<CommandSAdd>("sadd", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandSRem>("srem", -3, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandSCard>("scard", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSMembers>("smembers", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandSIsMember>("sismember", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSMIsMember>("smismember", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandSPop>("spop", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandSRandMember>("srandmember", -2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandSMove>("smove", 4, "write", 1, 2, 1),
                        MakeCmdAttr<CommandSDiff>("sdiff", -2, "read-only slow", 1, -1, 1),
                        MakeCmdAttr<CommandSUnion>("sunion", -2, "read-only slow", 1, -1, 1),
                        MakeCmdAttr<CommandSInter>("sinter", -2, "read-only slow", 1, -1, 1),
                        MakeCmdAttr<CommandSInterCard>("sintercard", -3, "read-only slow", CommandSInterCard::Range),
                        MakeCmdAttr<CommandSDiffStore>("sdiffstore", -3, "write slow", 1, -1, 1),
                        MakeCmdAttr<CommandSUnionStore>("sunionstore", -3, "write slow", 1, -1, 1),
                        MakeCmdAttr<CommandSInterStore>("sinterstore", -3, "write slow", 1, -1, 1),
                        MakeCmdAttr<CommandSScan>("sscan", -3, "read-only", 1, 1, 1), )

}  // namespace redis
