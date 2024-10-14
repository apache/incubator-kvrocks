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

#include "command_parser.h"
#include "commander.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_bloom_chain.h"

namespace {

constexpr const char *errBadErrorRate = "Bad error rate";
constexpr const char *errBadCapacity = "Bad capacity";
constexpr const char *errBadExpansion = "Bad expansion";
constexpr const char *errInvalidErrorRate = "error rate should be between 0 and 1";
constexpr const char *errInvalidCapacity = "capacity should be larger than 0";
constexpr const char *errInvalidExpansion = "expansion should be greater or equal to 1";
constexpr const char *errNonscalingButExpand = "nonscaling filters cannot expand";
constexpr const char *errFilterFull = "nonscaling filter is full";
}  // namespace

namespace redis {

class CommandBFReserve : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_error_rate = ParseFloat<double>(args[2]);
    if (!parse_error_rate) {
      return {Status::RedisParseErr, errBadErrorRate};
    }
    error_rate_ = *parse_error_rate;
    if (error_rate_ >= 1 || error_rate_ <= 0) {
      return {Status::RedisParseErr, errInvalidErrorRate};
    }

    auto parse_capacity = ParseInt<uint32_t>(args[3], 10);
    if (!parse_capacity) {
      return {Status::RedisParseErr, errBadCapacity};
    }
    capacity_ = *parse_capacity;
    if (capacity_ <= 0) {
      return {Status::RedisParseErr, errInvalidCapacity};
    }

    CommandParser parser(args, 4);
    bool is_nonscaling = false;
    bool has_expansion = false;
    while (parser.Good()) {
      if (parser.EatEqICase("nonscaling")) {
        is_nonscaling = true;
        expansion_ = 0;
      } else if (parser.EatEqICase("expansion")) {
        has_expansion = true;
        auto parse_expansion = parser.TakeInt<uint16_t>();
        if (!parse_expansion.IsOK()) {
          return {Status::RedisParseErr, errBadExpansion};
        }
        expansion_ = parse_expansion.GetValue();
        if (expansion_ < 1) {
          return {Status::RedisParseErr, errInvalidExpansion};
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    if (is_nonscaling && has_expansion) {
      return {Status::RedisParseErr, errNonscalingButExpand};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloomfilter_db(srv->storage, conn->GetNamespace());

    auto s = bloomfilter_db.Reserve(ctx, args_[1], capacity_, error_rate_, expansion_);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  double error_rate_;
  uint32_t capacity_;
  uint16_t expansion_ = kBFDefaultExpansion;
};

class CommandBFAdd : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(srv->storage, conn->GetNamespace());
    BloomFilterAddResult ret = BloomFilterAddResult::kOk;

    auto s = bloom_db.Add(ctx, args_[1], args_[2], &ret);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    switch (ret) {
      case BloomFilterAddResult::kOk:
        *output = redis::Integer(1);
        break;
      case BloomFilterAddResult::kExist:
        *output = redis::Integer(0);
        break;
      case BloomFilterAddResult::kFull:
        *output = redis::Error({Status::NotOK, errFilterFull});
        break;
    }
    return Status::OK();
  }
};

class CommandBFMAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    items_.reserve(args_.size() - 2);
    for (size_t i = 2; i < args_.size(); ++i) {
      items_.emplace_back(args_[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(srv->storage, conn->GetNamespace());
    std::vector<BloomFilterAddResult> rets(items_.size(), BloomFilterAddResult::kOk);

    auto s = bloom_db.MAdd(ctx, args_[1], items_, &rets);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(items_.size());
    for (size_t i = 0; i < items_.size(); ++i) {
      switch (rets[i]) {
        case BloomFilterAddResult::kOk:
          *output += redis::Integer(1);
          break;
        case BloomFilterAddResult::kExist:
          *output += redis::Integer(0);
          break;
        case BloomFilterAddResult::kFull:
          *output += redis::Error({Status::NotOK, errFilterFull});
          break;
      }
    }
    return Status::OK();
  }

 private:
  std::vector<std::string> items_;
};

class CommandBFInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    bool is_nonscaling = false;
    bool has_expansion = false;
    bool has_items = false;
    while (parser.Good()) {
      if (parser.EatEqICase("capacity")) {
        auto parse_capacity = parser.TakeInt<uint32_t>();
        if (!parse_capacity.IsOK()) {
          return {Status::RedisParseErr, errBadCapacity};
        }
        insert_options_.capacity = parse_capacity.GetValue();
        if (insert_options_.capacity <= 0) {
          return {Status::RedisParseErr, errInvalidCapacity};
        }
      } else if (parser.EatEqICase("error")) {
        auto parse_error_rate = parser.TakeFloat<double>();
        if (!parse_error_rate.IsOK()) {
          return {Status::RedisParseErr, errBadErrorRate};
        }
        insert_options_.error_rate = parse_error_rate.GetValue();
        if (insert_options_.error_rate >= 1 || insert_options_.error_rate <= 0) {
          return {Status::RedisParseErr, errInvalidErrorRate};
        }
      } else if (parser.EatEqICase("nocreate")) {
        insert_options_.auto_create = false;
      } else if (parser.EatEqICase("nonscaling")) {
        is_nonscaling = true;
        insert_options_.expansion = 0;
      } else if (parser.EatEqICase("expansion")) {
        has_expansion = true;
        auto parse_expansion = parser.TakeInt<uint16_t>();
        if (!parse_expansion.IsOK()) {
          return {Status::RedisParseErr, errBadExpansion};
        }
        insert_options_.expansion = parse_expansion.GetValue();
        if (insert_options_.expansion < 1) {
          return {Status::RedisParseErr, errInvalidExpansion};
        }
      } else if (parser.EatEqICase("items")) {
        has_items = true;
        break;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    if (is_nonscaling && has_expansion) {
      return {Status::RedisParseErr, errNonscalingButExpand};
    }

    if (not has_items) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    while (parser.Good()) {
      items_.emplace_back(GET_OR_RET(parser.TakeStr()));
    }

    if (items_.size() == 0) {
      return {Status::RedisParseErr, "num of items should be greater than 0"};
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(srv->storage, conn->GetNamespace());
    std::vector<BloomFilterAddResult> rets(items_.size(), BloomFilterAddResult::kOk);

    auto s = bloom_db.InsertCommon(ctx, args_[1], items_, insert_options_, &rets);
    if (s.IsNotFound()) return {Status::RedisExecErr, "key is not found"};
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(items_.size());
    for (size_t i = 0; i < items_.size(); ++i) {
      switch (rets[i]) {
        case BloomFilterAddResult::kOk:
          *output += redis::Integer(1);
          break;
        case BloomFilterAddResult::kExist:
          *output += redis::Integer(0);
          break;
        case BloomFilterAddResult::kFull:
          *output += redis::Error({Status::NotOK, errFilterFull});
          break;
      }
    }
    return Status::OK();
  }

 private:
  std::vector<std::string> items_;
  BloomFilterInsertOptions insert_options_;
};

class CommandBFExists : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(srv->storage, conn->GetNamespace());
    bool exist = false;

    auto s = bloom_db.Exists(ctx, args_[1], args_[2], &exist);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::Integer(exist ? 1 : 0);
    return Status::OK();
  }
};

class CommandBFMExists : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    items_.reserve(args_.size() - 2);
    for (size_t i = 2; i < args_.size(); ++i) {
      items_.emplace_back(args_[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(srv->storage, conn->GetNamespace());
    std::vector<bool> exists(items_.size(), false);

    auto s = bloom_db.MExists(ctx, args_[1], items_, &exists);
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    *output = redis::MultiLen(items_.size());
    for (size_t i = 0; i < items_.size(); ++i) {
      *output += Integer(exists[i] ? 1 : 0);
    }
    return Status::OK();
  }

 private:
  std::vector<std::string> items_;
};

class CommandBFInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }
    CommandParser parser(args, 2);
    if (parser.Good()) {
      if (parser.EatEqICase("capacity")) {
        type_ = BloomInfoType::kCapacity;
      } else if (parser.EatEqICase("size")) {
        type_ = BloomInfoType::kSize;
      } else if (parser.EatEqICase("filters")) {
        type_ = BloomInfoType::kFilters;
      } else if (parser.EatEqICase("items")) {
        type_ = BloomInfoType::kItems;
      } else if (parser.EatEqICase("expansion")) {
        type_ = BloomInfoType::kExpansion;
      } else {
        return {Status::RedisParseErr, "Invalid info argument"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(srv->storage, conn->GetNamespace());
    BloomFilterInfo info;

    auto s = bloom_db.Info(ctx, args_[1], &info);
    if (s.IsNotFound()) return {Status::RedisExecErr, "key is not found"};
    if (!s.ok()) return {Status::RedisExecErr, s.ToString()};

    switch (type_) {
      case BloomInfoType::kAll:
        *output = redis::MultiLen(2 * 5);
        *output += redis::SimpleString("Capacity");
        *output += redis::Integer(info.capacity);
        *output += redis::SimpleString("Size");
        *output += redis::Integer(info.bloom_bytes);
        *output += redis::SimpleString("Number of filters");
        *output += redis::Integer(info.n_filters);
        *output += redis::SimpleString("Number of items inserted");
        *output += redis::Integer(info.size);
        *output += redis::SimpleString("Expansion rate");
        *output += info.expansion == 0 ? conn->NilString() : redis::Integer(info.expansion);
        break;
      case BloomInfoType::kCapacity:
        *output = redis::Integer(info.capacity);
        break;
      case BloomInfoType::kSize:
        *output = redis::Integer(info.bloom_bytes);
        break;
      case BloomInfoType::kFilters:
        *output = redis::Integer(info.n_filters);
        break;
      case BloomInfoType::kItems:
        *output = redis::Integer(info.size);
        break;
      case BloomInfoType::kExpansion:
        *output = info.expansion == 0 ? conn->NilString() : redis::Integer(info.expansion);
        break;
    }

    return Status::OK();
  }

 private:
  BloomInfoType type_ = BloomInfoType::kAll;
};

class CommandBFCard : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::BloomChain bloom_db(srv->storage, conn->GetNamespace());
    BloomFilterInfo info;

    auto s = bloom_db.Info(ctx, args_[1], &info);
    if (!s.ok() && !s.IsNotFound()) return {Status::RedisExecErr, s.ToString()};
    if (s.IsNotFound()) {
      *output = redis::Integer(0);
    } else {
      *output = redis::Integer(info.size);
    }
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(BloomFilter, MakeCmdAttr<CommandBFReserve>("bf.reserve", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFAdd>("bf.add", 3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFMAdd>("bf.madd", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFInsert>("bf.insert", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBFExists>("bf.exists", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFMExists>("bf.mexists", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFInfo>("bf.info", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandBFCard>("bf.card", 2, "read-only", 1, 1, 1), )
}  // namespace redis
