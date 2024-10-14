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
#include "commands/blocking_commander.h"
#include "commands/command_parser.h"
#include "error_constants.h"
#include "event_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "types/redis_list.h"

namespace redis {

class CommandPush : public Commander {
 public:
  CommandPush(bool create_if_missing, bool left) : left_(left), create_if_missing_(create_if_missing) {}

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> elems;
    for (size_t i = 2; i < args_.size(); i++) {
      elems.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    rocksdb::Status s;
    redis::List list_db(srv->storage, conn->GetNamespace());

    if (create_if_missing_) {
      s = list_db.Push(ctx, args_[1], elems, left_, &ret);
    } else {
      s = list_db.PushX(ctx, args_[1], elems, left_, &ret);
    }
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    srv->WakeupBlockingConns(args_[1], elems.size());

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool left_;
  bool create_if_missing_;
};

class CommandLPush : public CommandPush {
 public:
  CommandLPush() : CommandPush(true, true) {}
};

class CommandRPush : public CommandPush {
 public:
  CommandRPush() : CommandPush(true, false) {}
};

class CommandLPushX : public CommandPush {
 public:
  CommandLPushX() : CommandPush(false, true) {}
};

class CommandRPushX : public CommandPush {
 public:
  CommandRPushX() : CommandPush(false, false) {}
};

class CommandPop : public Commander {
 public:
  explicit CommandPop(bool left) : left_(left) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    if (args.size() == 2) {
      return Status::OK();
    }

    auto v = ParseInt<int32_t>(args[2], 10);
    if (!v) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*v < 0) {
      return {Status::RedisParseErr, errValueMustBePositive};
    }

    count_ = *v;
    with_count_ = true;

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());

    if (with_count_) {
      std::vector<std::string> elems;
      auto s = list_db.PopMulti(ctx, args_[1], left_, count_, &elems);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = conn->NilArray();
      } else {
        *output = conn->MultiBulkString(elems);
      }
    } else {
      std::string elem;
      auto s = list_db.Pop(ctx, args_[1], left_, &elem);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = conn->NilString();
      } else {
        *output = redis::BulkString(elem);
      }
    }

    return Status::OK();
  }

 private:
  bool left_;
  bool with_count_ = false;
  uint32_t count_ = 1;
};

class CommandLPop : public CommandPop {
 public:
  CommandLPop() : CommandPop(true) {}
};

class CommandRPop : public CommandPop {
 public:
  CommandRPop() : CommandPop(false) {}
};

class CommandLMPop : public Commander {
 public:
  // format: LMPOP #numkeys key0 [key1 ...] <LEFT | RIGHT> [COUNT count]
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);

    auto num_keys = GET_OR_RET(parser.TakeInt<uint32_t>());
    keys_.clear();
    keys_.reserve(num_keys);
    for (uint32_t i = 0; i < num_keys; ++i) {
      keys_.emplace_back(GET_OR_RET(parser.TakeStr()));
    }

    auto left_or_right = util::ToLower(GET_OR_RET(parser.TakeStr()));
    if (left_or_right == "left") {
      left_ = true;
    } else if (left_or_right == "right") {
      left_ = false;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    while (parser.Good()) {
      if (parser.EatEqICase("count") && count_ == static_cast<uint32_t>(-1)) {
        count_ = GET_OR_RET(parser.TakeInt<uint32_t>());
      } else {
        return parser.InvalidSyntax();
      }
    }
    if (count_ == static_cast<uint32_t>(-1)) {
      count_ = 1;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());

    std::vector<std::string> elems;
    std::string chosen_key;

    for (auto &key : keys_) {
      auto s = list_db.PopMulti(ctx, key, left_, count_, &elems);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }
      if (!elems.empty()) {
        chosen_key = key;
        break;
      }
    }

    if (elems.empty()) {
      *output = conn->NilString();
    } else {
      std::string elems_bulk = conn->MultiBulkString(elems);
      *output = redis::Array({redis::BulkString(chosen_key), std::move(elems_bulk)});
    }

    return Status::OK();
  }

  static const inline CommandKeyRangeGen keyRangeGen = [](const std::vector<std::string> &args) {
    CommandKeyRange range;
    range.first_key = 2;
    range.key_step = 1;
    // This parsing would always succeed as this cmd has been parsed before.
    auto num_key = *ParseInt<int32_t>(args[1], 10);
    range.last_key = range.first_key + num_key - 1;
    return range;
  };

 private:
  bool left_;
  uint32_t count_ = -1;
  std::vector<std::string> keys_;
};

class CommandBPop : public BlockingCommander {
 public:
  explicit CommandBPop(bool left) : left_(left) {}

  CommandBPop(const CommandBPop &) = delete;
  CommandBPop &operator=(const CommandBPop &) = delete;

  ~CommandBPop() override = default;

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseFloat(args[args.size() - 1]);
    if (!parse_result) {
      return {Status::RedisParseErr, errTimeoutIsNotFloat};
    }

    if (*parse_result < 0) {
      return {Status::RedisParseErr, "timeout should not be negative"};
    }

    timeout_ = static_cast<int64_t>(*parse_result * 1000 * 1000);

    keys_ = std::vector<std::string>(args.begin() + 1, args.end() - 1);
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    srv_ = srv;
    InitConnection(conn);

    auto s = TryPopFromList(ctx);
    if (s.ok() || !s.IsNotFound()) {
      return Status::OK();  // error has already output in TryPopFromList
    }

    return StartBlocking(timeout_, output);
  }

  void BlockKeys() override {
    for (const auto &key : keys_) {
      srv_->BlockOnKey(key, conn_);
    }
  }

  void UnblockKeys() override {
    for (const auto &key : keys_) {
      srv_->UnblockOnKey(key, conn_);
    }
  }

  rocksdb::Status TryPopFromList(engine::Context &ctx) {
    redis::List list_db(srv_->storage, conn_->GetNamespace());
    std::string elem;
    const std::string *last_key_ptr = nullptr;
    rocksdb::Status s;
    for (const auto &key : keys_) {
      last_key_ptr = &key;
      s = list_db.Pop(ctx, key, left_, &elem);
      if (s.ok() || !s.IsNotFound()) {
        break;
      }
    }

    if (s.ok()) {
      if (!last_key_ptr) {
        conn_->Reply(conn_->MultiBulkString({"", ""}));
      } else {
        conn_->GetServer()->UpdateWatchedKeysManually({*last_key_ptr});
        conn_->Reply(conn_->MultiBulkString({*last_key_ptr, std::move(elem)}));
      }
    } else if (!s.IsNotFound()) {
      conn_->Reply(redis::Error({Status::NotOK, s.ToString()}));
    }

    return s;
  }

  bool OnBlockingWrite() override {
    engine::Context ctx(srv_->storage);
    auto s = TryPopFromList(ctx);
    return !s.IsNotFound();
  }

  std::string NoopReply(const Connection *conn) override { return conn->NilString(); }

 private:
  bool left_ = false;
  int64_t timeout_ = 0;  // microseconds
  std::vector<std::string> keys_;
  Server *srv_ = nullptr;
};

class CommandBLPop : public CommandBPop {
 public:
  CommandBLPop() : CommandBPop(true) {}
};

class CommandBRPop : public CommandBPop {
 public:
  CommandBRPop() : CommandBPop(false) {}
};

class CommandBLMPop : public BlockingCommander {
 public:
  CommandBLMPop() = default;
  CommandBLMPop(const CommandBLMPop &) = delete;
  CommandBLMPop &operator=(const CommandBLMPop &) = delete;

  ~CommandBLMPop() override = default;

  // format: BLMPOP timeout #numkeys key0 [key1 ...] <LEFT | RIGHT> [COUNT count]
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);

    auto timeout = GET_OR_RET(parser.TakeFloat());
    timeout_ = static_cast<int64_t>(timeout * 1000 * 1000);

    auto num_keys = GET_OR_RET(parser.TakeInt<uint32_t>());
    keys_.clear();
    keys_.reserve(num_keys);
    for (uint32_t i = 0; i < num_keys; ++i) {
      keys_.emplace_back(GET_OR_RET(parser.TakeStr()));
    }

    auto left_or_right = util::ToLower(GET_OR_RET(parser.TakeStr()));
    if (left_or_right == "left") {
      left_ = true;
    } else if (left_or_right == "right") {
      left_ = false;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    while (parser.Good()) {
      if (parser.EatEqICase("count") && count_ == static_cast<uint32_t>(-1)) {
        count_ = GET_OR_RET(parser.TakeInt<uint32_t>());
      } else {
        return parser.InvalidSyntax();
      }
    }
    if (count_ == static_cast<uint32_t>(-1)) {
      count_ = 1;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    srv_ = srv;
    InitConnection(conn);

    auto s = ExecuteUnblocked(ctx);
    if (s.ok() || !s.IsNotFound()) {
      return Status::OK();  // error has already been output
    }

    return StartBlocking(timeout_, output);
  }

  rocksdb::Status ExecuteUnblocked(engine::Context &ctx) {
    redis::List list_db(srv_->storage, conn_->GetNamespace());
    std::vector<std::string> elems;
    std::string chosen_key;
    rocksdb::Status s;
    for (const auto &key : keys_) {
      s = list_db.PopMulti(ctx, key, left_, count_, &elems);
      if (s.ok() && !elems.empty()) {
        chosen_key = key;
        break;
      }
      if (!s.IsNotFound()) {
        break;
      }
    }

    if (s.ok()) {
      if (!elems.empty()) {
        conn_->GetServer()->UpdateWatchedKeysManually({chosen_key});
        std::string elems_bulk = conn_->MultiBulkString(elems);
        conn_->Reply(redis::Array({redis::BulkString(chosen_key), std::move(elems_bulk)}));
      }
    } else if (!s.IsNotFound()) {
      conn_->Reply(redis::Error({Status::NotOK, s.ToString()}));
    }

    return s;
  }

  void BlockKeys() override {
    for (const auto &key : keys_) {
      srv_->BlockOnKey(key, conn_);
    }
  }

  void UnblockKeys() override {
    for (const auto &key : keys_) {
      srv_->UnblockOnKey(key, conn_);
    }
  }

  bool OnBlockingWrite() override {
    engine::Context ctx(srv_->storage);
    auto s = ExecuteUnblocked(ctx);
    return !s.IsNotFound();
  }

  std::string NoopReply(const Connection *conn) override { return conn->NilString(); }

  static const inline CommandKeyRangeGen keyRangeGen = [](const std::vector<std::string> &args) {
    CommandKeyRange range;
    range.first_key = 3;
    range.key_step = 1;
    // This parsing would always succeed as this cmd has been parsed before.
    auto num_key = *ParseInt<int32_t>(args[2], 10);
    range.last_key = range.first_key + num_key - 1;
    return range;
  };

 private:
  bool left_;
  uint32_t count_ = -1;
  int64_t timeout_ = 0;  // microseconds
  std::vector<std::string> keys_;
  Server *srv_ = nullptr;
};

class CommandLRem : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    count_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::List list_db(srv->storage, conn->GetNamespace());

    auto s = list_db.Rem(ctx, args_[1], count_, args_[3], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int count_ = 0;
};

class CommandLInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if ((util::ToLower(args[2]) == "before")) {
      before_ = true;
    } else if ((util::ToLower(args[2]) == "after")) {
      before_ = false;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int ret = 0;
    redis::List list_db(srv->storage, conn->GetNamespace());

    auto s = list_db.Insert(ctx, args_[1], args_[3], args_[4], before_, &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool before_ = false;
};

class CommandLRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> elems;

    auto s = list_db.Range(ctx, args_[1], start_, stop_, &elems);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = ArrayOfBulkStrings(elems);
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = 0;
};

class CommandLLen : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());
    uint64_t count = 0;

    auto s = list_db.Size(ctx, args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(count);
    return Status::OK();
  }
};

class CommandLIndex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    index_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());
    std::string elem;

    auto s = list_db.Index(ctx, args_[1], index_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
    } else {
      *output = redis::BulkString(elem);
    }
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    index_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());

    auto s = list_db.Set(ctx, args_[1], index_, args_[3]);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, errNoSuchKey};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLTrim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());

    auto s = list_db.Trim(ctx, args_[1], start_, stop_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandRPopLPUSH : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());
    std::string elem;

    auto s = list_db.LMove(ctx, args_[1], args_[2], /*src_left=*/false, /*dst_left=*/true, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(elem);
    return Status::OK();
  }
};

class CommandLMove : public Commander {
 public:
  Status Parse([[maybe_unused]] const std::vector<std::string> &args) override {
    auto arg_val = util::ToLower(args_[3]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    src_left_ = arg_val == "left";
    arg_val = util::ToLower(args_[4]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    dst_left_ = arg_val == "left";
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());
    std::string elem;

    auto s = list_db.LMove(ctx, args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(elem);
    return Status::OK();
  }

 private:
  bool src_left_;
  bool dst_left_;
};

class CommandBLMove : public BlockingCommander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto arg_val = util::ToLower(args_[3]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    src_left_ = arg_val == "left";
    arg_val = util::ToLower(args_[4]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    dst_left_ = arg_val == "left";

    auto parse_result = ParseFloat(args[args.size() - 1]);
    if (!parse_result) {
      return {Status::RedisParseErr, errTimeoutIsNotFloat};
    }
    if (*parse_result < 0) {
      return {Status::RedisParseErr, errTimeoutIsNegative};
    }
    timeout_ = static_cast<int64_t>(*parse_result * 1000 * 1000);

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    srv_ = srv;
    InitConnection(conn);

    redis::List list_db(srv->storage, conn->GetNamespace());
    std::string elem;

    auto s = list_db.LMove(ctx, args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    if (!elem.empty()) {
      *output = redis::BulkString(elem);
      return Status::OK();
    }

    return StartBlocking(timeout_, output);
  }

  void BlockKeys() override { srv_->BlockOnKey(args_[1], conn_); }

  void UnblockKeys() override { srv_->UnblockOnKey(args_[1], conn_); }

  bool OnBlockingWrite() override {
    redis::List list_db(srv_->storage, conn_->GetNamespace());
    std::string elem;
    engine::Context ctx(srv_->storage);
    auto s = list_db.LMove(ctx, args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      conn_->Reply(redis::Error({Status::NotOK, s.ToString()}));
      return true;
    }

    bool empty = elem.empty();
    if (!empty) {
      conn_->Reply(redis::BulkString(elem));
    }

    return !empty;
  }

  std::string NoopReply(const Connection *conn) override { return conn->NilArray(); }

 private:
  bool src_left_;
  bool dst_left_;
  int64_t timeout_ = 0;  // microseconds
  Server *srv_ = nullptr;
};

class CommandLPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 9) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    CommandParser parser(args, 3);
    while (parser.Good()) {
      if (parser.EatEqICase("rank")) {
        spec_.rank = GET_OR_RET(parser.TakeInt());
        if (spec_.rank == 0) {
          return {Status::RedisParseErr,
                  "RANK can't be zero: use 1 to start from "
                  "the first match, 2 from the second ... "
                  "or use negative to start from the end of the list"};
        } else if (spec_.rank == LLONG_MIN) {
          // Negating LLONG_MIN will cause an overflow, and is effectively be the same as passing -1.
          return {Status::RedisParseErr, "rank would overflow"};
        }
      } else if (parser.EatEqICase("count")) {
        spec_.count = GET_OR_RET(parser.TakeInt());
        if (spec_.count < 0) {
          return {Status::RedisParseErr, "COUNT can't be negative"};
        }
      } else if (parser.EatEqICase("maxlen")) {
        spec_.max_len = GET_OR_RET(parser.TakeInt());
        if (spec_.max_len < 0) {
          return {Status::RedisParseErr, "MAXLEN can't be negative"};
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::List list_db(srv->storage, conn->GetNamespace());
    std::vector<int64_t> indexes;

    auto s = list_db.Pos(ctx, args_[1], args_[2], spec_, &indexes);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    // We return nil or a single value if `COUNT` option is not given.
    if (!spec_.count.has_value()) {
      if (s.IsNotFound() || indexes.empty()) {
        *output = conn->NilString();
      } else {
        assert(indexes.size() == 1);
        *output = redis::Integer(indexes[0]);
      }
    }
    // Otherwise we return an array.
    else {
      std::vector<std::string> values;
      values.reserve(indexes.size());
      for (const auto &index : indexes) {
        values.emplace_back(std::to_string(index));
      }
      *output = ArrayOfBulkStrings(values);
    }
    return Status::OK();
  }

 private:
  PosSpec spec_;
};

REDIS_REGISTER_COMMANDS(List, MakeCmdAttr<CommandBLPop>("blpop", -3, "write no-script", 1, -2, 1),
                        MakeCmdAttr<CommandBRPop>("brpop", -3, "write no-script", 1, -2, 1),
                        MakeCmdAttr<CommandBLMPop>("blmpop", -5, "write no-script", CommandBLMPop::keyRangeGen),
                        MakeCmdAttr<CommandLIndex>("lindex", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLInsert>("linsert", 5, "write slow", 1, 1, 1),
                        MakeCmdAttr<CommandLLen>("llen", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLMove>("lmove", 5, "write", 1, 2, 1),
                        MakeCmdAttr<CommandBLMove>("blmove", 6, "write", 1, 2, 1),
                        MakeCmdAttr<CommandLPop>("lpop", -2, "write", 1, 1, 1),  //
                        MakeCmdAttr<CommandLPos>("lpos", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLPush>("lpush", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLPushX>("lpushx", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLRange>("lrange", 4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLRem>("lrem", 4, "write no-dbsize-check slow", 1, 1, 1),
                        MakeCmdAttr<CommandLSet>("lset", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLTrim>("ltrim", 4, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandLMPop>("lmpop", -4, "write", CommandLMPop::keyRangeGen),
                        MakeCmdAttr<CommandRPop>("rpop", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandRPopLPUSH>("rpoplpush", 3, "write", 1, 2, 1),
                        MakeCmdAttr<CommandRPush>("rpush", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandRPushX>("rpushx", -3, "write", 1, 1, 1), )

}  // namespace redis
