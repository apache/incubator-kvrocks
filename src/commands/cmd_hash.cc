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

#include "commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "scan_base.h"
#include "server/server.h"
#include "types/redis_hash.h"

namespace redis {

class CommandHGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(value);
    return Status::OK();
  }
};

class CommandHSetNX : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, true, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHStrlen : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(static_cast<int>(value.size()));
    return Status::OK();
  }
};

class CommandHDel : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.Delete(ctx, args_[1], fields, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHExists : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::string value;

    auto s = hash_db.Get(ctx, args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(1);
    return Status::OK();
  }
};

class CommandHLen : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t count = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.Size(ctx, args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::Integer(0) : redis::Integer(count);
    return Status::OK();
  }
};

class CommandHIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int64_t>(args[3], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    increment_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.IncrBy(ctx, args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_ = 0;
};

class CommandHIncrByFloat : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[3]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    increment_ = *increment;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    double ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.IncrByFloat(ctx, args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::BulkString(util::Float2String(ret));
    return Status::OK();
  }

 private:
  double increment_ = 0;
};

class CommandHMGet : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> fields;
    for (size_t i = 2; i < args_.size(); i++) {
      fields.emplace_back(args_[i]);
    }

    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MGet(ctx, args_[1], fields, &values, &statuses);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      values.resize(fields.size(), "");
      *output = conn->MultiBulkString(values);
    } else {
      *output = conn->MultiBulkString(values, statuses);
    }
    return Status::OK();
  }
};

class CommandHMSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    for (size_t i = 2; i < args_.size(); i += 2) {
      field_values_.emplace_back(args_[i], args_[i + 1]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::Hash hash_db(srv->storage, conn->GetNamespace());

    auto s = hash_db.MSet(ctx, args_[1], field_values_, false, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (GetAttributes()->name == "hset") {
      *output = redis::Integer(ret);
    } else {
      *output = redis::SimpleString("OK");
    }
    return Status::OK();
  }

 private:
  std::vector<FieldValue> field_values_;
};

class CommandHKeys : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values, HashFetchType::kOnlyKey);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> keys;
    keys.reserve(field_values.size());
    for (const auto &fv : field_values) {
      keys.emplace_back(fv.field);
    }
    *output = conn->MultiBulkString(keys);

    return Status::OK();
  }
};

class CommandHVals : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values, HashFetchType::kOnlyValue);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    values.reserve(field_values.size());
    for (const auto &p : field_values) {
      values.emplace_back(p.value);
    }
    *output = ArrayOfBulkStrings(values);

    return Status::OK();
  }
};

class CommandHGetAll : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.GetAll(ctx, args_[1], &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> kv_pairs;
    kv_pairs.reserve(field_values.size());
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = conn->MapOfBulkStrings(kv_pairs);

    return Status::OK();
  }
};

class CommandHRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 4);
    while (parser.Good()) {
      if (parser.EatEqICase("REV")) {
        spec_.reversed = true;
      } else if (parser.EatEqICase("LIMIT")) {
        spec_.offset = GET_OR_RET(parser.TakeInt());
        spec_.count = GET_OR_RET(parser.TakeInt());
      } else {
        return parser.InvalidSyntax();
      }
    }
    if (spec_.reversed) {
      return ParseRangeLexSpec(args[3], args[2], &spec_);
    } else {
      return ParseRangeLexSpec(args[2], args[3], &spec_);
    }
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    rocksdb::Status s = hash_db.RangeByLex(ctx, args_[1], spec_, &field_values);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    std::vector<std::string> kv_pairs;
    for (const auto &p : field_values) {
      kv_pairs.emplace_back(p.field);
      kv_pairs.emplace_back(p.value);
    }
    *output = ArrayOfBulkStrings(kv_pairs);

    return Status::OK();
  }

 private:
  RangeLexSpec spec_;
};

class CommandHScan : public CommandSubkeyScanBase {
 public:
  CommandHScan() = default;
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> fields;
    std::vector<std::string> values;
    auto key_name = srv->GetKeyNameFromCursor(cursor_, CursorType::kTypeHash);

    auto s = hash_db.Scan(ctx, key_, key_name, limit_, prefix_, &fields, &values);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    auto cursor = GetNextCursor(srv, fields, CursorType::kTypeHash);
    std::vector<std::string> entries;
    entries.reserve(2 * fields.size());
    for (size_t i = 0; i < fields.size(); i++) {
      entries.emplace_back(redis::BulkString(fields[i]));
      entries.emplace_back(redis::BulkString(values[i]));
    }
    *output = redis::Array({redis::BulkString(cursor), redis::Array(entries)});
    return Status::OK();
  }
};

class CommandHRandField : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() >= 3) {
      no_parameters_ = false;
      auto parse_result = ParseInt<int64_t>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      command_count_ = *parse_result;

      if (args.size() > 4 || (args.size() == 4 && !util::EqualICase(args[3], "withvalues"))) {
        return {Status::RedisParseErr, errInvalidSyntax};
      } else if (args.size() == 4) {
        withvalues_ = true;
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::Hash hash_db(srv->storage, conn->GetNamespace());
    std::vector<FieldValue> field_values;

    auto s = hash_db.RandField(ctx, args_[1], command_count_, &field_values,
                               withvalues_ ? HashFetchType::kAll : HashFetchType::kOnlyKey);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_entries;
    result_entries.reserve(field_values.size());
    for (const auto &p : field_values) {
      result_entries.emplace_back(p.field);
      if (withvalues_) result_entries.emplace_back(p.value);
    }

    if (no_parameters_)
      *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(result_entries[0]);
    else
      *output = ArrayOfBulkStrings(result_entries);
    return Status::OK();
  }

 private:
  bool withvalues_ = false;
  int64_t command_count_ = 1;
  bool no_parameters_ = true;
};

REDIS_REGISTER_COMMANDS(Hash, MakeCmdAttr<CommandHGet>("hget", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrBy>("hincrby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHIncrByFloat>("hincrbyfloat", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHSetNX>("hsetnx", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHDel>("hdel", -3, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandHStrlen>("hstrlen", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHExists>("hexists", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHLen>("hlen", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMGet>("hmget", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHMSet>("hmset", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandHKeys>("hkeys", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHVals>("hvals", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHGetAll>("hgetall", 2, "read-only slow", 1, 1, 1),
                        MakeCmdAttr<CommandHScan>("hscan", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRangeByLex>("hrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandHRandField>("hrandfield", -2, "read-only slow", 1, 1, 1), )

}  // namespace redis
