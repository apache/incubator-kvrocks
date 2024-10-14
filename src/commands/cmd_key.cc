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
#include "commands/ttl_util.h"
#include "error_constants.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "storage/redis_db.h"
#include "time_util.h"

namespace redis {

class CommandType : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    RedisType type = kRedisNone;

    auto s = redis.Type(ctx, args_[1], &type);
    if (s.ok()) {
      if (type >= RedisTypeNames.size()) return {Status::RedisExecErr, "Invalid type"};
      *output = redis::SimpleString(RedisTypeNames[type]);
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandMove : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    GET_OR_RET(ParseInt<int64_t>(args[2], 10));
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int count = 0;
    redis::Database redis(srv->storage, conn->GetNamespace());

    rocksdb::Status s = redis.Exists(ctx, {args_[1]}, &count);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = count ? redis::Integer(1) : redis::Integer(0);
    return Status::OK();
  }
};

class CommandMoveX : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::string &key = args_[1], &token = args_[2];

    redis::Database redis(srv->storage, conn->GetNamespace());

    std::string ns;
    AuthResult auth_result = srv->AuthenticateUser(token, &ns);
    switch (auth_result) {
      case AuthResult::NO_REQUIRE_PASS:
        return {Status::NotOK, "Forbidden to move key when requirepass is empty"};
      case AuthResult::INVALID_PASSWORD:
        return {Status::NotOK, "Invalid password"};
      case AuthResult::IS_USER:
      case AuthResult::IS_ADMIN:
        break;
    }

    Database::CopyResult res = Database::CopyResult::DONE;
    std::string ns_key = redis.AppendNamespacePrefix(key);
    std::string new_ns_key = ComposeNamespaceKey(ns, key, srv->storage->IsSlotIdEncoded());

    auto s = redis.Copy(ctx, ns_key, new_ns_key, true, true, &res);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    if (res == Database::CopyResult::DONE) {
      *output = redis::Integer(1);
    } else {
      *output = redis::Integer(0);
    }
    return Status::OK();
  }
};

class CommandObject : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    if (util::ToLower(args_[1]) == "dump") {
      redis::Database redis(srv->storage, conn->GetNamespace());
      std::vector<std::string> infos;

      auto s = redis.Dump(ctx, args_[2], &infos);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      output->append(redis::MultiLen(infos.size()));
      for (const auto &info : infos) {
        output->append(redis::BulkString(info));
      }
    } else {
      return {Status::RedisExecErr, "object subcommand must be dump"};
    }
    return Status::OK();
  }
};

class CommandTTL : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    int64_t ttl = 0;

    auto s = redis.TTL(ctx, args_[1], &ttl);
    if (s.ok()) {
      *output = redis::Integer(ttl > 0 ? ttl / 1000 : ttl);
      return Status::OK();
    }

    return {Status::RedisExecErr, s.ToString()};
  }
};

class CommandPTTL : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    int64_t ttl = 0;

    auto s = redis.TTL(ctx, args_[1], &ttl);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(ttl);
    return Status::OK();
  }
};

class CommandExists : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<rocksdb::Slice> keys;
    keys.reserve(args_.size() - 1);
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    int cnt = 0;
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.Exists(ctx, keys, &cnt);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    *output = redis::Integer(cnt);

    return Status::OK();
  }
};

class CommandExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    ttl_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10));
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.Expire(ctx, args_[1], ttl_ * 1000 + util::GetTimeStampMS());
    if (s.ok()) {
      *output = redis::Integer(1);
    } else {
      *output = redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  uint64_t ttl_ = 0;
};

class CommandPExpire : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    seconds_ = GET_OR_RET(ParseInt<int64_t>(args[2], 10)) + util::GetTimeStampMS();
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.Expire(ctx, args_[1], seconds_);
    if (s.ok()) {
      *output = redis::Integer(1);
    } else {
      *output = redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  uint64_t seconds_ = 0;
};

class CommandExpireAt : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    timestamp_ = *parse_result;

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.Expire(ctx, args_[1], timestamp_ * 1000);
    if (s.ok()) {
      *output = redis::Integer(1);
    } else {
      *output = redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  uint64_t timestamp_ = 0;
};

class CommandPExpireAt : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    timestamp_ = *parse_result;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.Expire(ctx, args_[1], timestamp_);
    if (s.ok()) {
      *output = redis::Integer(1);
    } else {
      *output = redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  uint64_t timestamp_ = 0;
};

class CommandExpireTime : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    uint64_t timestamp = 0;

    auto s = redis.GetExpireTime(ctx, args_[1], &timestamp);
    if (s.ok()) {
      *output = timestamp > 0 ? redis::Integer(timestamp / 1000) : redis::Integer(-1);
    } else if (s.IsNotFound()) {
      *output = redis::Integer(-2);
    } else {
      return {Status::RedisExecErr, s.ToString()};
    }
    return Status::OK();
  }
};

class CommandPExpireTime : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    uint64_t timestamp = 0;

    auto s = redis.GetExpireTime(ctx, args_[1], &timestamp);
    if (s.ok()) {
      *output = timestamp > 0 ? redis::Integer(timestamp) : redis::Integer(-1);
    } else if (s.IsNotFound()) {
      *output = redis::Integer(-2);
    } else {
      return {Status::RedisExecErr, s.ToString()};
    }
    return Status::OK();
  }
};

class CommandPersist : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ttl = 0;
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.TTL(ctx, args_[1], &ttl);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    if (ttl == -1 || ttl == -2) {
      *output = redis::Integer(0);
      return Status::OK();
    }

    s = redis.Expire(ctx, args_[1], 0);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(1);
    return Status::OK();
  }
};

class CommandDel : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<rocksdb::Slice> keys;
    keys.reserve(args_.size() - 1);
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }

    uint64_t cnt = 0;
    redis::Database redis(srv->storage, conn->GetNamespace());

    auto s = redis.MDel(ctx, keys, &cnt);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(cnt);
    return Status::OK();
  }
};

class CommandRename : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    Database::CopyResult res = Database::CopyResult::DONE;
    std::string ns_key = redis.AppendNamespacePrefix(args_[1]);
    std::string new_ns_key = redis.AppendNamespacePrefix(args_[2]);

    auto s = redis.Copy(ctx, ns_key, new_ns_key, false, true, &res);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    if (res == Database::CopyResult::KEY_NOT_EXIST) return {Status::RedisExecErr, "no such key"};
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandRenameNX : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    Database::CopyResult res = Database::CopyResult::DONE;
    std::string ns_key = redis.AppendNamespacePrefix(args_[1]);
    std::string new_ns_key = redis.AppendNamespacePrefix(args_[2]);

    auto s = redis.Copy(ctx, ns_key, new_ns_key, true, true, &res);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    switch (res) {
      case Database::CopyResult::KEY_NOT_EXIST:
        return {Status::RedisExecErr, "no such key"};
      case Database::CopyResult::DONE:
        *output = redis::Integer(1);
        break;
      case Database::CopyResult::KEY_ALREADY_EXIST:
        *output = redis::Integer(0);
        break;
    }
    return Status::OK();
  }
};

class CommandCopy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 3);
    while (parser.Good()) {
      if (parser.EatEqICase("db")) {
        auto db_num = GET_OR_RET(parser.TakeInt());
        // There's only one database in Kvrocks, so the DB must be 0 here.
        if (db_num != 0) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }
      } else if (parser.EatEqICase("replace")) {
        replace_ = true;
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());
    Database::CopyResult res = Database::CopyResult::DONE;
    std::string ns_key = redis.AppendNamespacePrefix(args_[1]);
    std::string new_ns_key = redis.AppendNamespacePrefix(args_[2]);

    auto s = redis.Copy(ctx, ns_key, new_ns_key, !replace_, false, &res);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};
    switch (res) {
      case Database::CopyResult::KEY_NOT_EXIST:
        return {Status::RedisExecErr, "no such key"};
      case Database::CopyResult::DONE:
        *output = redis::Integer(1);
        break;
      case Database::CopyResult::KEY_ALREADY_EXIST:
        *output = redis::Integer(0);
        break;
    }
    return Status::OK();
  }

 private:
  bool replace_ = false;
};

template <bool ReadOnly>
class CommandSort : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    while (parser.Good()) {
      if (parser.EatEqICase("BY")) {
        if (!sort_argument_.sortby.empty()) return {Status::InvalidArgument, "don't use multiple BY parameters"};
        sort_argument_.sortby = GET_OR_RET(parser.TakeStr());

        if (sort_argument_.sortby.find('*') == std::string::npos) {
          sort_argument_.dontsort = true;
        } else {
          /* TODO:
           * If BY is specified with a real pattern, we can't accept it in cluster mode,
           * unless we can make sure the keys formed by the pattern are in the same slot
           * as the key to sort.
           * If BY is specified with a real pattern, we can't accept
           * it if no full ACL key access is applied for this command. */
        }
      } else if (parser.EatEqICase("LIMIT")) {
        sort_argument_.offset = GET_OR_RET(parser.template TakeInt<int>());
        sort_argument_.count = GET_OR_RET(parser.template TakeInt<int>());
      } else if (parser.EatEqICase("GET")) {
        /* TODO:
         * If GET is specified with a real pattern, we can't accept it in cluster mode,
         * unless we can make sure the keys formed by the pattern are in the same slot
         * as the key to sort. */
        sort_argument_.getpatterns.push_back(GET_OR_RET(parser.TakeStr()));
      } else if (parser.EatEqICase("ASC")) {
        sort_argument_.desc = false;
      } else if (parser.EatEqICase("DESC")) {
        sort_argument_.desc = true;
      } else if (parser.EatEqICase("ALPHA")) {
        sort_argument_.alpha = true;
      } else if (parser.EatEqICase("STORE")) {
        if constexpr (ReadOnly) {
          return {Status::RedisParseErr, "SORT_RO is read-only and does not support the STORE parameter"};
        }
        sort_argument_.storekey = GET_OR_RET(parser.TakeStr());
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Database redis(srv->storage, conn->GetNamespace());

    RedisType type = kRedisNone;
    if (auto s = redis.Type(ctx, args_[1], &type); !s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (type != RedisType::kRedisList && type != RedisType::kRedisSet && type != RedisType::kRedisZSet) {
      return {Status::RedisWrongType, "Operation against a key holding the wrong kind of value"};
    }

    /* When sorting a set with no sort specified, we must sort the output
     * so the result is consistent across scripting and replication.
     *
     * The other types (list, sorted set) will retain their native order
     * even if no sort order is requested, so they remain stable across
     * scripting and replication.
     *
     * TODO: support CLIENT_SCRIPT flag, (!storekey_.empty() || c->flags & CLIENT_SCRIPT)) */
    if (sort_argument_.dontsort && type == RedisType::kRedisSet && (!sort_argument_.storekey.empty())) {
      /* Force ALPHA sorting */
      sort_argument_.dontsort = false;
      sort_argument_.alpha = true;
      sort_argument_.sortby = "";
    }

    std::vector<std::optional<std::string>> sorted_elems;
    Database::SortResult res = Database::SortResult::DONE;

    if (auto s = redis.Sort(ctx, type, args_[1], sort_argument_, &sorted_elems, &res); !s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    switch (res) {
      case Database::SortResult::UNKNOWN_TYPE:
        return {Status::RedisErrorNoPrefix, "Unknown Type"};
      case Database::SortResult::DOUBLE_CONVERT_ERROR:
        return {Status::RedisErrorNoPrefix, "One or more scores can't be converted into double"};
      case Database::SortResult::LIMIT_EXCEEDED:
        return {Status::RedisErrorNoPrefix,
                "The number of elements to be sorted exceeds SORT_LENGTH_LIMIT = " + std::to_string(SORT_LENGTH_LIMIT)};
      case Database::SortResult::DONE:
        if (sort_argument_.storekey.empty()) {
          std::vector<std::string> output_vec;
          output_vec.reserve(sorted_elems.size());
          for (const auto &elem : sorted_elems) {
            output_vec.emplace_back(elem.has_value() ? redis::BulkString(elem.value()) : conn->NilString());
          }
          *output = redis::Array(output_vec);
        } else {
          *output = Integer(sorted_elems.size());
        }
        break;
    }

    return Status::OK();
  }

 private:
  SortArgument sort_argument_;
};

REDIS_REGISTER_COMMANDS(Key, MakeCmdAttr<CommandTTL>("ttl", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandPTTL>("pttl", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandType>("type", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandMove>("move", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandMoveX>("movex", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandObject>("object", 3, "read-only", 2, 2, 1),
                        MakeCmdAttr<CommandExists>("exists", -2, "read-only", 1, -1, 1),
                        MakeCmdAttr<CommandPersist>("persist", 2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandExpire>("expire", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandPExpire>("pexpire", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandExpireAt>("expireat", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandPExpireAt>("pexpireat", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandExpireTime>("expiretime", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandPExpireTime>("pexpiretime", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandDel>("del", -2, "write no-dbsize-check", 1, -1, 1),
                        MakeCmdAttr<CommandDel>("unlink", -2, "write no-dbsize-check", 1, -1, 1),
                        MakeCmdAttr<CommandRename>("rename", 3, "write", 1, 2, 1),
                        MakeCmdAttr<CommandRenameNX>("renamenx", 3, "write", 1, 2, 1),
                        MakeCmdAttr<CommandCopy>("copy", -3, "write", 1, 2, 1),
                        MakeCmdAttr<CommandSort<false>>("sort", -2, "write slow", 1, 1, 1),
                        MakeCmdAttr<CommandSort<true>>("sort_ro", -2, "read-only slow", 1, 1, 1))

}  // namespace redis
