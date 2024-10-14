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

#include <limits>

#include "command_parser.h"
#include "commander.h"
#include "commands/blocking_commander.h"
#include "commands/scan_base.h"
#include "error_constants.h"
#include "parse_util.h"
#include "rocksdb/env.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "string_util.h"
#include "types/redis_zset.h"

namespace redis {

class CommandZAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t index = 2;
    parseFlags(args, index);
    if (auto s = validateFlags(); !s.IsOK()) {
      return s;
    }

    if (auto left = (args.size() - index); left >= 0) {
      if (flags_.HasIncr() && left != 2) {
        return {Status::RedisParseErr, "INCR option supports a single increment-element pair"};
      }

      if (left % 2 != 0 || left == 0) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    for (size_t i = index; i < args.size(); i += 2) {
      auto score = ParseFloat(args[i]);
      if (!score) {
        return {Status::RedisParseErr, errValueIsNotFloat};
      }
      if (std::isnan(*score)) {
        return {Status::RedisParseErr, errScoreIsNotValidFloat};
      }

      member_scores_.emplace_back(MemberScore{args[i + 1], *score});
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    double old_score = member_scores_[0].score;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.Add(ctx, args_[1], flags_, &member_scores_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    srv->WakeupBlockingConns(args_[1], member_scores_.size());

    if (flags_.HasIncr()) {
      auto new_score = member_scores_[0].score;
      if ((flags_.HasNX() || flags_.HasXX() || flags_.HasLT() || flags_.HasGT()) && old_score == new_score &&
          ret == 0) {  // not the first time using incr && score not changed
        *output = conn->NilString();
        return Status::OK();
      }

      *output = conn->Double(new_score);
    } else {
      *output = redis::Integer(ret);
    }
    return Status::OK();
  }

 private:
  std::vector<MemberScore> member_scores_;
  ZAddFlags flags_{0};

  void parseFlags(const std::vector<std::string> &args, size_t &index);
  Status validateFlags() const;
};

void CommandZAdd::parseFlags(const std::vector<std::string> &args, size_t &index) {
  std::unordered_map<std::string, ZSetFlags> options = {{"xx", kZSetXX}, {"nx", kZSetNX}, {"ch", kZSetCH},
                                                        {"lt", kZSetLT}, {"gt", kZSetGT}, {"incr", kZSetIncr}};
  for (size_t i = 2; i < args.size(); i++) {
    auto option = util::ToLower(args[i]);
    if (auto it = options.find(option); it != options.end()) {
      flags_.SetFlag(it->second);
      index++;
    } else {
      break;
    }
  }
}

Status CommandZAdd::validateFlags() const {
  if (!flags_.HasAnyFlags()) {
    return Status::OK();
  }

  if (flags_.HasNX() && flags_.HasXX()) {
    return {Status::RedisParseErr, "XX and NX options at the same time are not compatible"};
  }

  if ((flags_.HasLT() && flags_.HasGT()) || (flags_.HasLT() && flags_.HasNX()) || (flags_.HasGT() && flags_.HasNX())) {
    return {Status::RedisParseErr, errZSetLTGTNX};
  }

  return Status::OK();
}

class CommandZCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeScoreSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) return s;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.Count(ctx, args_[1], spec_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  RangeScoreSpec spec_;
};

class CommandZCard : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t ret = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.Card(ctx, args_[1], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

class CommandZIncrBy : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto increment = ParseFloat(args[2]);
    if (!increment) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    incr_ = *increment;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    double score = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.IncrBy(ctx, args_[1], args_[3], incr_, &score);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = conn->Double(score);
    return Status::OK();
  }

 private:
  double incr_ = 0.0;
};

class CommandZLexCount : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t size = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.RangeByLex(ctx, args_[1], spec_, nullptr, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(size);
    return Status::OK();
  }

 private:
  RangeLexSpec spec_;
};

class CommandZPop : public Commander {
 public:
  explicit CommandZPop(bool min) : min_(min) {}

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
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    std::vector<MemberScore> member_scores;

    auto s = zset_db.Pop(ctx, args_[1], count_, min_, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(redis::MultiLen(member_scores.size() * 2));
    for (const auto &ms : member_scores) {
      output->append(redis::BulkString(ms.member));
      output->append(conn->Double(ms.score));
    }

    return Status::OK();
  }

 private:
  bool min_;
  int count_ = 1;
};

class CommandZPopMin : public CommandZPop {
 public:
  CommandZPopMin() : CommandZPop(true) {}
};

class CommandZPopMax : public CommandZPop {
 public:
  CommandZPopMax() : CommandZPop(false) {}
};

static rocksdb::Status PopFromMultipleZsets(engine::Context &ctx, redis::ZSet *zset_db,
                                            const std::vector<std::string> &keys, bool min, int count,
                                            std::string *user_key, std::vector<MemberScore> *member_scores) {
  rocksdb::Status s;
  for (auto &key : keys) {
    s = zset_db->Pop(ctx, key, count, min, member_scores);
    if (!s.ok()) {
      return s;
    }

    if (!member_scores->empty()) {
      *user_key = key;
      break;
    }
  }

  return rocksdb::Status::OK();
}

class CommandBZPop : public BlockingCommander {
 public:
  explicit CommandBZPop(bool min) : min_(min) {}

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseFloat(args[args.size() - 1]);
    if (!parse_result) {
      return {Status::RedisParseErr, errTimeoutIsNotFloat};
    }
    if (*parse_result < 0) {
      return {Status::RedisParseErr, errTimeoutIsNegative};
    }
    timeout_ = static_cast<int64_t>(*parse_result * 1000 * 1000);

    keys_ = std::vector<std::string>(args.begin() + 1, args.end() - 1);
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    srv_ = srv;
    InitConnection(conn);

    std::string user_key;
    std::vector<MemberScore> member_scores;

    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = PopFromMultipleZsets(ctx, &zset_db, keys_, min_, 1, &user_key, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!member_scores.empty()) {
      SendMembersWithScores(conn, member_scores, user_key);
      return Status::OK();
    }

    return StartBlocking(timeout_, output);
  }

  std::string NoopReply(const Connection *conn) override { return conn->NilArray(); }

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

  void SendMembersWithScores(const Connection *conn, const std::vector<MemberScore> &member_scores,
                             const std::string &user_key) {
    std::string output;
    output.append(redis::MultiLen(member_scores.size() * 2 + 1));
    output.append(redis::BulkString(user_key));
    for (const auto &ms : member_scores) {
      output.append(redis::BulkString(ms.member));
      output.append(conn->Double(ms.score));
    }
    conn_->Reply(output);
  }

  bool OnBlockingWrite() override {
    std::string user_key;
    std::vector<MemberScore> member_scores;

    redis::ZSet zset_db(srv_->storage, conn_->GetNamespace());
    engine::Context ctx(srv_->storage);
    auto s = PopFromMultipleZsets(ctx, &zset_db, keys_, min_, 1, &user_key, &member_scores);
    if (!s.ok()) {
      conn_->Reply(redis::Error({Status::NotOK, s.ToString()}));
      return true;
    }

    bool empty = member_scores.empty();
    if (!empty) {
      SendMembersWithScores(conn_, member_scores, user_key);
    }

    return !empty;
  }

 private:
  bool min_;
  int64_t timeout_ = 0;  // microseconds
  std::vector<std::string> keys_;
  Server *srv_ = nullptr;
};

class CommandBZPopMin : public CommandBZPop {
 public:
  CommandBZPopMin() : CommandBZPop(true) {}
};

class CommandBZPopMax : public CommandBZPop {
 public:
  CommandBZPopMax() : CommandBZPop(false) {}
};

static void SendMembersWithScoresForZMpop(Connection *conn, const std::string &user_key,
                                          const std::vector<MemberScore> &member_scores) {
  std::string output;
  output.append(redis::MultiLen(2));
  output.append(redis::BulkString(user_key));
  output.append(redis::MultiLen(member_scores.size() * 2));
  for (const auto &ms : member_scores) {
    output.append(redis::BulkString(ms.member));
    output.append(conn->Double(ms.score));
  }
  conn->Reply(output);
}

class CommandZMPop : public Commander {
 public:
  CommandZMPop() = default;

  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    numkeys_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
    for (int i = 0; i < numkeys_; ++i) {
      keys_.emplace_back(GET_OR_RET(parser.TakeStr()));
    }

    while (parser.Good()) {
      if (flag_ == ZSET_NONE && parser.EatEqICase("min")) {
        flag_ = ZSET_MIN;
      } else if (flag_ == ZSET_NONE && parser.EatEqICase("max")) {
        flag_ = ZSET_MAX;
      } else if (count_ == 0 && parser.EatEqICase("count")) {
        count_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
      } else {
        return parser.InvalidSyntax();
      }
    }
    if (flag_ == ZSET_NONE) {
      return parser.InvalidSyntax();
    }
    if (count_ == 0) count_ = 1;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    for (auto &user_key : keys_) {
      std::vector<MemberScore> member_scores;
      auto s = zset_db.Pop(ctx, user_key, count_, flag_ == ZSET_MIN, &member_scores);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
      if (member_scores.empty()) {
        continue;
      }

      SendMembersWithScoresForZMpop(conn, user_key, member_scores);
      return Status::OK();
    }
    *output = conn->NilArray();
    return Status::OK();
  }

  static CommandKeyRange Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[1], 10);
    return {2, 1 + num_key, 1};
  }

 private:
  int numkeys_;
  std::vector<std::string> keys_;
  enum { ZSET_MIN, ZSET_MAX, ZSET_NONE } flag_ = ZSET_NONE;
  int count_ = 0;
};

class CommandBZMPop : public BlockingCommander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);

    timeout_ = static_cast<int64_t>(GET_OR_RET(parser.TakeFloat<double>()) * 1000 * 1000);
    if (timeout_ < 0) {
      return {Status::RedisParseErr, errTimeoutIsNegative};
    }

    num_keys_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
    for (int i = 0; i < num_keys_; ++i) {
      keys_.emplace_back(GET_OR_RET(parser.TakeStr()));
    }

    while (parser.Good()) {
      if (flag_ == ZSET_NONE && parser.EatEqICase("min")) {
        flag_ = ZSET_MIN;
      } else if (flag_ == ZSET_NONE && parser.EatEqICase("max")) {
        flag_ = ZSET_MAX;
      } else if (count_ == 0 && parser.EatEqICase("count")) {
        count_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
      } else {
        return parser.InvalidSyntax();
      }
    }

    if (flag_ == ZSET_NONE) {
      return parser.InvalidSyntax();
    }
    if (count_ == 0) count_ = 1;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    srv_ = srv;
    InitConnection(conn);

    std::string user_key;
    std::vector<MemberScore> member_scores;

    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = PopFromMultipleZsets(ctx, &zset_db, keys_, flag_ == ZSET_MIN, count_, &user_key, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (!member_scores.empty()) {
      SendMembersWithScoresForZMpop(conn_, user_key, member_scores);
      return Status::OK();
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

  std::string NoopReply(const Connection *conn) override { return conn->NilString(); }

  bool OnBlockingWrite() override {
    std::string user_key;
    std::vector<MemberScore> member_scores;

    redis::ZSet zset_db(srv_->storage, conn_->GetNamespace());
    engine::Context ctx(srv_->storage);
    auto s = PopFromMultipleZsets(ctx, &zset_db, keys_, flag_ == ZSET_MIN, count_, &user_key, &member_scores);
    if (!s.ok()) {
      conn_->Reply(redis::Error({Status::NotOK, s.ToString()}));
      return true;
    }

    bool empty = member_scores.empty();
    if (!empty) {
      SendMembersWithScoresForZMpop(conn_, user_key, member_scores);
    }

    return !empty;
  }

  static CommandKeyRange Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[2], 10);
    return {3, 2 + num_key, 1};
  }

 private:
  int64_t timeout_ = 0;  // microseconds
  int num_keys_;
  std::vector<std::string> keys_;
  enum { ZSET_MIN, ZSET_MAX, ZSET_NONE } flag_ = ZSET_NONE;
  int count_ = 0;
  Server *srv_ = nullptr;
};

class CommandZRangeStore : public Commander {
 public:
  explicit CommandZRangeStore() : range_type_(kZRangeRank), direction_(kZRangeDirectionForward) {}

  Status Parse(const std::vector<std::string> &args) override {
    dst_ = args[1];
    src_ = args[2];

    int64_t offset = 0;
    int64_t count = -1;
    // skip the <CMD> <dst> <src> <min> <max> args and parse remaining optional arguments
    CommandParser parser(args, 5);
    while (parser.Good()) {
      if (parser.EatEqICase("limit")) {
        auto parse_offset = parser.TakeInt<int64_t>();
        auto parse_count = parser.TakeInt<int64_t>();
        if (!parse_offset || !parse_count) {
          return {Status::RedisParseErr, errValueNotInteger};
        }
        offset = *parse_offset;
        count = *parse_count;
      } else if (parser.EatEqICase("bylex")) {
        range_type_ = kZRangeLex;
      } else if (parser.EatEqICase("byscore")) {
        range_type_ = kZRangeScore;
      } else if (parser.EatEqICase("rev")) {
        direction_ = kZRangeDirectionReverse;
      } else {
        return parser.InvalidSyntax();
      }
    }

    if (count != -1 && range_type_ == kZRangeRank) {
      return {Status::RedisParseErr,
              "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"};
    }

    // resolve index of <min> <max>
    int min_idx = 3;
    int max_idx = 4;
    if (direction_ == kZRangeDirectionReverse && (range_type_ == kZRangeLex || range_type_ == kZRangeScore)) {
      min_idx = 4;
      max_idx = 3;
    }

    // parse range spec
    switch (range_type_) {
      case kZRangeAuto:
      case kZRangeRank:
        GET_OR_RET(ParseRangeRankSpec(args[min_idx], args[max_idx], &rank_spec_));
        if (direction_ == kZRangeDirectionReverse) {
          rank_spec_.reversed = true;
        }
        break;
      case kZRangeLex:
        GET_OR_RET(ParseRangeLexSpec(args[min_idx], args[max_idx], &lex_spec_));
        lex_spec_.offset = offset;
        lex_spec_.count = count;
        if (direction_ == kZRangeDirectionReverse) {
          lex_spec_.reversed = true;
        }
        break;
      case kZRangeScore:
        GET_OR_RET(ParseRangeScoreSpec(args[min_idx], args[max_idx], &score_spec_));
        score_spec_.offset = offset;
        score_spec_.count = count;
        if (direction_ == kZRangeDirectionReverse) {
          score_spec_.reversed = true;
        }
        break;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    std::vector<MemberScore> member_scores;

    rocksdb::Status s;

    switch (range_type_) {
      case kZRangeAuto:
      case kZRangeRank:
        s = zset_db.RangeByRank(ctx, src_, rank_spec_, &member_scores, nullptr);
        break;
      case kZRangeScore:
        s = zset_db.RangeByScore(ctx, src_, score_spec_, &member_scores, nullptr);
        break;
      case kZRangeLex:
        s = zset_db.RangeByLex(ctx, src_, lex_spec_, &member_scores, nullptr);
        break;
    }
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    uint64_t ret = member_scores.size();
    s = zset_db.Overwrite(ctx, dst_, member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::string src_;
  std::string dst_;
  ZRangeType range_type_;
  ZRangeDirection direction_;

  RangeRankSpec rank_spec_;
  RangeLexSpec lex_spec_;
  RangeScoreSpec score_spec_;
};

/*
 * description:
 *    syntax:   `ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]
 *              [WITHSCORES]`
 *
 */
class CommandZRangeGeneric : public Commander {
 public:
  explicit CommandZRangeGeneric(ZRangeType range_type = kZRangeAuto, ZRangeDirection direction = kZRangeDirectionAuto)
      : range_type_(range_type), direction_(direction) {}

  Status Parse(const std::vector<std::string> &args) override {
    key_ = args[1];

    int64_t offset = 0;
    int64_t count = -1;
    // skip the <CMD> <src> <min> <max> args and parse remaining optional arguments
    CommandParser parser(args, 4);
    while (parser.Good()) {
      if (parser.EatEqICase("withscores")) {
        with_scores_ = true;
      } else if (parser.EatEqICase("limit")) {
        auto parse_offset = parser.TakeInt<int64_t>();
        auto parse_count = parser.TakeInt<int64_t>();
        if (!parse_offset || !parse_count) {
          return {Status::RedisParseErr, errValueNotInteger};
        }
        offset = *parse_offset;
        count = *parse_count;
      } else if (range_type_ == kZRangeAuto && parser.EatEqICase("bylex")) {
        range_type_ = kZRangeLex;
      } else if (range_type_ == kZRangeAuto && parser.EatEqICase("byscore")) {
        range_type_ = kZRangeScore;
      } else if (direction_ == kZRangeDirectionAuto && parser.EatEqICase("rev")) {
        direction_ = kZRangeDirectionReverse;
      } else {
        return parser.InvalidSyntax();
      }
    }

    // use defaults if not overridden by arguments
    if (range_type_ == kZRangeAuto) {
      range_type_ = kZRangeRank;
    }
    if (direction_ == kZRangeDirectionAuto) {
      direction_ = kZRangeDirectionForward;
    }

    // check for conflicting arguments
    if (with_scores_ && range_type_ == kZRangeLex) {
      return {Status::RedisParseErr, "syntax error, WITHSCORES not supported in combination with BYLEX"};
    }
    if (count != -1 && range_type_ == kZRangeRank) {
      return {Status::RedisParseErr,
              "syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"};
    }

    // resolve index of <min> <max>
    int min_idx = 2;
    int max_idx = 3;
    if (direction_ == kZRangeDirectionReverse && (range_type_ == kZRangeLex || range_type_ == kZRangeScore)) {
      min_idx = 3;
      max_idx = 2;
    }

    // parse range spec
    switch (range_type_) {
      case kZRangeAuto:
      case kZRangeRank:
        GET_OR_RET(ParseRangeRankSpec(args[min_idx], args[max_idx], &rank_spec_));
        if (direction_ == kZRangeDirectionReverse) {
          rank_spec_.reversed = true;
        }
        break;
      case kZRangeLex:
        GET_OR_RET(ParseRangeLexSpec(args[min_idx], args[max_idx], &lex_spec_));
        lex_spec_.offset = offset;
        lex_spec_.count = count;
        if (direction_ == kZRangeDirectionReverse) {
          lex_spec_.reversed = true;
        }
        break;
      case kZRangeScore:
        GET_OR_RET(ParseRangeScoreSpec(args[min_idx], args[max_idx], &score_spec_));
        score_spec_.offset = offset;
        score_spec_.count = count;
        if (direction_ == kZRangeDirectionReverse) {
          score_spec_.reversed = true;
        }
        break;
    }

    return Status::OK();
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    std::vector<MemberScore> member_scores;

    rocksdb::Status s;

    switch (range_type_) {
      case kZRangeAuto:
      case kZRangeRank:
        s = zset_db.RangeByRank(ctx, key_, rank_spec_, &member_scores, nullptr);
        break;
      case kZRangeScore:
        if (score_spec_.count == 0) {
          *output = conn->MultiBulkString({});
          return Status::OK();
        }
        s = zset_db.RangeByScore(ctx, key_, score_spec_, &member_scores, nullptr);
        break;
      case kZRangeLex:
        if (lex_spec_.count == 0) {
          *output = conn->MultiBulkString({});
          return Status::OK();
        }
        s = zset_db.RangeByLex(ctx, key_, lex_spec_, &member_scores, nullptr);
        break;
    }
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    auto is_resp3 = conn->GetProtocolVersion() == RESP::v3;
    // RESP3 with scores should return an array of arrays,
    // so we don't need to multiply the size by 2 here.
    output->append(redis::MultiLen(member_scores.size() * (with_scores_ && !is_resp3 ? 2 : 1)));
    for (const auto &ms : member_scores) {
      if (with_scores_ && is_resp3) output->append(MultiLen(2));
      output->append(redis::BulkString(ms.member));
      if (with_scores_) output->append(conn->Double(ms.score));
    }
    return Status::OK();
  }

 private:
  std::string key_;
  ZRangeType range_type_;
  ZRangeDirection direction_;
  bool with_scores_ = false;

  RangeRankSpec rank_spec_;
  RangeLexSpec lex_spec_;
  RangeScoreSpec score_spec_;
};

class CommandZRange : public CommandZRangeGeneric {
 public:
  explicit CommandZRange() = default;
};

class CommandZRevRange : public CommandZRangeGeneric {
 public:
  CommandZRevRange() : CommandZRangeGeneric(kZRangeRank, kZRangeDirectionReverse) {}
};

class CommandZRangeByLex : public CommandZRangeGeneric {
 public:
  explicit CommandZRangeByLex() : CommandZRangeGeneric(kZRangeLex, kZRangeDirectionForward) {}
};

class CommandZRevRangeByLex : public CommandZRangeGeneric {
 public:
  CommandZRevRangeByLex() : CommandZRangeGeneric(kZRangeLex, kZRangeDirectionReverse) {}
};

class CommandZRangeByScore : public CommandZRangeGeneric {
 public:
  explicit CommandZRangeByScore() : CommandZRangeGeneric(kZRangeScore, kZRangeDirectionForward) {}
};

class CommandZRevRangeByScore : public CommandZRangeGeneric {
 public:
  CommandZRevRangeByScore() : CommandZRangeGeneric(kZRangeScore, kZRangeDirectionReverse) {}
};

class CommandZRank : public Commander {
 public:
  explicit CommandZRank(bool reversed = false) : reversed_(reversed) {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 4) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    // skip the <CMD> <key> <member> and parse remaining optional arguments
    CommandParser parser(args, 3);
    while (parser.Good()) {
      if (parser.EatEqICase("withscore") && !with_score_) {
        with_score_ = true;
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    int rank = 0;
    double score = 0.0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.Rank(ctx, args_[1], args_[2], reversed_, &rank, &score);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (rank == -1) {
      if (with_score_) {
        output->append(conn->NilArray());
      } else {
        *output = conn->NilString();
      }
    } else {
      if (with_score_) {
        output->append(redis::MultiLen(2));
        output->append(redis::Integer(rank));
        output->append(conn->Double(score));
      } else {
        *output = redis::Integer(rank);
      }
    }
    return Status::OK();
  }

 private:
  bool reversed_;
  bool with_score_ = false;
};

class CommandZRevRank : public CommandZRank {
 public:
  CommandZRevRank() : CommandZRank(true) {}
};

class CommandZRem : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<rocksdb::Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }

    uint64_t size = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.Remove(ctx, args_[1], members, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(size);
    return Status::OK();
  }
};

class CommandZRemRangeByRank : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    spec_.start = *parse_start;
    spec_.stop = *parse_stop;

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    uint64_t cnt = 0;
    spec_.with_deletion = true;

    auto s = zset_db.RangeByRank(ctx, args_[1], spec_, nullptr, &cnt);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(cnt);
    return Status::OK();
  }

 private:
  RangeRankSpec spec_;
};

class CommandZRemRangeByScore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeScoreSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) return s;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    uint64_t cnt = 0;
    spec_.with_deletion = true;

    auto s = zset_db.RangeByScore(ctx, args_[1], spec_, nullptr, &cnt);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(cnt);
    return Status::OK();
  }

 private:
  RangeScoreSpec spec_;
};

class CommandZRemRangeByLex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    Status s = ParseRangeLexSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) return s;
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    uint64_t cnt = 0;
    spec_.with_deletion = true;

    auto s = zset_db.RangeByLex(ctx, args_[1], spec_, nullptr, &cnt);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(cnt);
    return Status::OK();
  }

 private:
  RangeLexSpec spec_;
};

class CommandZScore : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    double score = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.Score(ctx, args_[1], args_[2], &score);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = conn->NilString();
    } else {
      *output = conn->Double(score);
    }
    return Status::OK();
  }
};

class CommandZMScore : public Commander {
 public:
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    std::vector<Slice> members;
    for (size_t i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    std::map<std::string, double> mscores;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.MGet(ctx, args_[1], members, &mscores);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> values;
    if (s.IsNotFound()) {
      values.resize(members.size(), "");
    } else {
      for (const auto &member : members) {
        auto iter = mscores.find(member.ToString());
        if (iter == mscores.end()) {
          values.emplace_back(conn->NilString());
        } else {
          values.emplace_back(conn->Double(iter->second));
        }
      }
    }
    *output = conn->MultiBulkString(values);
    return Status::OK();
  }
};

class CommandZUnion : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    numkeys_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
    for (size_t i = 0; i < numkeys_; ++i) {
      keys_weights_.emplace_back(KeyWeight{GET_OR_RET(parser.TakeStr()), 1});
    }

    while (parser.Good()) {
      if (parser.EatEqICase("aggregate")) {
        std::string aggregate_value = GET_OR_RET(parser.TakeStr());
        if (util::ToLower(aggregate_value) == "sum") {
          aggregate_method_ = kAggregateSum;
        } else if (util::ToLower(aggregate_value) == "min") {
          aggregate_method_ = kAggregateMin;
        } else if (util::ToLower(aggregate_value) == "max") {
          aggregate_method_ = kAggregateMax;
        } else {
          return {Status::RedisParseErr, "aggregate param error"};
        }
      } else if (parser.EatEqICase("weights")) {
        size_t k = 0;
        while (k < numkeys_) {
          auto weight = parser.TakeFloat();
          if (!weight) {
            return {Status::RedisParseErr, errValueIsNotFloat};
          }
          keys_weights_[k].weight = *weight;
          k++;
        }
      } else if (parser.EatEqICase("withscores")) {
        with_scores_ = true;
      } else {
        return parser.InvalidSyntax();
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    std::vector<MemberScore> member_scores;

    auto s = zset_db.Union(ctx, keys_weights_, aggregate_method_, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    auto compare_score = [](const MemberScore &score1, const MemberScore &score2) {
      if (score1.score == score2.score) {
        return score1.member < score2.member;
      }
      return score1.score < score2.score;
    };
    std::sort(member_scores.begin(), member_scores.end(), compare_score);
    output->append(redis::MultiLen(member_scores.size() * (with_scores_ ? 2 : 1)));
    for (const auto &ms : member_scores) {
      output->append(redis::BulkString(ms.member));
      if (with_scores_) output->append(conn->Double(ms.score));
    }
    return Status::OK();
  }

  static CommandKeyRange Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[1], 10);
    return {2, 1 + num_key, 1};
  }

 protected:
  size_t numkeys_ = 0;
  bool with_scores_ = false;
  std::vector<KeyWeight> keys_weights_;
  AggregateMethod aggregate_method_ = kAggregateSum;
};

class CommandZUnionStore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    numkeys_ = *parse_result;
    if (numkeys_ > args.size() - 3) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    size_t j = 0;
    while (j < numkeys_) {
      keys_weights_.emplace_back(KeyWeight{args[j + 3], 1});
      j++;
    }

    size_t i = 3 + numkeys_;
    while (i < args.size()) {
      if (util::ToLower(args[i]) == "aggregate" && i + 1 < args.size()) {
        if (util::ToLower(args[i + 1]) == "sum") {
          aggregate_method_ = kAggregateSum;
        } else if (util::ToLower(args[i + 1]) == "min") {
          aggregate_method_ = kAggregateMin;
        } else if (util::ToLower(args[i + 1]) == "max") {
          aggregate_method_ = kAggregateMax;
        } else {
          return {Status::RedisParseErr, "aggregate param error"};
        }
        i += 2;
      } else if (util::ToLower(args[i]) == "weights" && i + numkeys_ < args.size()) {
        size_t k = 0;
        while (k < numkeys_) {
          auto weight = ParseFloat(args[i + k + 1]);
          if (!weight || std::isnan(*weight)) {
            return {Status::RedisParseErr, "weight is not a double or out of range"};
          }
          keys_weights_[k].weight = *weight;

          k++;
        }
        i += numkeys_ + 1;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t size = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.UnionStore(ctx, args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(size);
    return Status::OK();
  }

  static std::vector<CommandKeyRange> Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[2], 10);
    return {{1, 1, 1}, {3, 2 + num_key, 1}};
  }

 protected:
  size_t numkeys_ = 0;
  std::vector<KeyWeight> keys_weights_;
  AggregateMethod aggregate_method_ = kAggregateSum;
};

class CommandZInterStore : public CommandZUnionStore {
 public:
  CommandZInterStore() : CommandZUnionStore() {}

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    uint64_t size = 0;
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    auto s = zset_db.InterStore(ctx, args_[1], keys_weights_, aggregate_method_, &size);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(size);
    return Status::OK();
  }

  static std::vector<CommandKeyRange> Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[2], 10);
    return {{1, 1, 1}, {3, 2 + num_key, 1}};
  }
};

class CommandZInter : public CommandZUnion {
 public:
  CommandZInter() : CommandZUnion() {}

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    std::vector<MemberScore> member_scores;

    auto s = zset_db.Inter(ctx, keys_weights_, aggregate_method_, &member_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    auto ms_comparator = [](const MemberScore &ms1, const MemberScore &ms2) {
      if (ms1.score == ms2.score) {
        return ms1.member < ms2.member;
      }
      return ms1.score < ms2.score;
    };
    std::sort(member_scores.begin(), member_scores.end(), ms_comparator);
    output->append(redis::MultiLen(member_scores.size() * (with_scores_ ? 2 : 1)));
    for (const auto &member_score : member_scores) {
      output->append(redis::BulkString(member_score.member));
      if (with_scores_) output->append(conn->Double(member_score.score));
    }
    return Status::OK();
  }

  static CommandKeyRange Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[1], 10);
    return {2, 1 + num_key, 1};
  }
};

class CommandZInterCard : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);
    numkeys_ = GET_OR_RET(parser.TakeInt<int>(NumericRange<int>{1, std::numeric_limits<int>::max()}));
    for (size_t i = 0; i < numkeys_; ++i) {
      keys_.emplace_back(GET_OR_RET(parser.TakeStr()));
    }

    // if set limit option
    if (parser.Good()) {
      if (parser.EatEqICase("limit")) {
        auto res = parser.TakeInt<int64_t>();
        if (!res.IsOK() || res.GetValue() < 0) {
          return {Status::RedisParseErr, errLimitIsNegative};
        }
        limit_ = static_cast<size_t>(res.GetValue());
        if (parser.Good()) {
          return parser.InvalidSyntax();
        }
      } else {
        return parser.InvalidSyntax();
      }
    }

    return Commander::Parse(args);
  }
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    uint64_t count = 0;

    auto s = zset_db.InterCard(ctx, keys_, limit_, &count);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::Integer(count);
    return Status::OK();
  }

  static CommandKeyRange Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[1], 10);
    return {2, 1 + num_key, 1};
  }

 private:
  size_t numkeys_ = 0;
  size_t limit_ = 0;
  std::vector<std::string> keys_;
};

class CommandZScan : public CommandSubkeyScanBase {
 public:
  CommandZScan() = default;

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    std::vector<std::string> members;
    std::vector<double> scores;
    auto key_name = srv->GetKeyNameFromCursor(cursor_, CursorType::kTypeZSet);

    auto s = zset_db.Scan(ctx, key_, key_name, limit_, prefix_, &members, &scores);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    auto cursor = GetNextCursor(srv, members, CursorType::kTypeZSet);
    std::vector<std::string> entries;
    entries.reserve(2 * members.size());
    for (size_t i = 0; i < members.size(); i++) {
      entries.emplace_back(redis::BulkString(members[i]));
      entries.emplace_back(conn->Double(scores[i]));
    }
    *output = redis::Array({redis::BulkString(cursor), redis::Array(entries)});
    return Status::OK();
  }
};

class CommandZRandMember : public Commander {
 public:
  CommandZRandMember() = default;

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 4) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    if (args.size() >= 3) {
      no_parameters_ = false;
      auto parse_result = ParseInt<int64_t>(args[2], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      count_ = *parse_result;
    }

    if (args.size() == 4) {
      if (util::ToLower(args[3]) == "withscores") {
        with_scores_ = true;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());
    std::vector<MemberScore> member_scores;

    auto s = zset_db.RandMember(ctx, args_[1], count_, &member_scores);

    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_entries;
    result_entries.reserve(member_scores.size());

    for (const auto &[member, score] : member_scores) {
      result_entries.emplace_back(BulkString(member));
      if (with_scores_) result_entries.emplace_back(conn->Double(score));
    }

    if (no_parameters_)
      *output = s.IsNotFound() ? conn->NilString() : redis::BulkString(result_entries[0]);
    else
      *output = Array(result_entries);
    return Status::OK();
  }

 private:
  int64_t count_ = 1;
  bool with_scores_ = false;
  bool no_parameters_ = true;
};

class CommandZDiff : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[1], 10);
    if (!parse_result) return {Status::RedisParseErr, errValueNotInteger};

    numkeys_ = *parse_result;
    if (numkeys_ > args.size() - 2) return {Status::RedisParseErr, errInvalidSyntax};

    size_t j = 0;
    keys_.reserve(numkeys_);
    while (j < numkeys_) {
      keys_.emplace_back(args[j + 2]);
      j++;
    }

    if (auto i = 2 + numkeys_; i < args.size()) {
      if (util::ToLower(args[i]) == "withscores") {
        with_scores_ = true;
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    std::vector<MemberScore> members_with_scores;

    auto s = zset_db.Diff(ctx, keys_, &members_with_scores);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(redis::MultiLen(members_with_scores.size() * (with_scores_ ? 2 : 1)));
    for (const auto &ms : members_with_scores) {
      output->append(redis::BulkString(ms.member));
      if (with_scores_) output->append(conn->Double(ms.score));
    }

    return Status::OK();
  }

  static CommandKeyRange Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[1], 10);
    return {2, 1 + num_key, 1};
  }

 protected:
  size_t numkeys_ = 0;
  std::vector<rocksdb::Slice> keys_;
  bool with_scores_ = false;
};

class CommandZDiffStore : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) return {Status::RedisParseErr, errValueNotInteger};

    numkeys_ = *parse_result;
    if (numkeys_ > args.size() - 3) return {Status::RedisParseErr, errInvalidSyntax};

    size_t j = 0;
    while (j < numkeys_) {
      keys_.emplace_back(args[j + 3]);
      j++;
    }

    return Commander::Parse(args);
  }

  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    redis::ZSet zset_db(srv->storage, conn->GetNamespace());

    uint64_t stored_count = 0;

    auto s = zset_db.DiffStore(ctx, args_[1], keys_, &stored_count);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    *output = redis::Integer(stored_count);
    return Status::OK();
  }

  static std::vector<CommandKeyRange> Range(const std::vector<std::string> &args) {
    int num_key = *ParseInt<int>(args[2], 10);
    return {{1, 1, 1}, {3, 2 + num_key, 1}};
  }

 protected:
  size_t numkeys_ = 0;
  std::vector<rocksdb::Slice> keys_;
};

REDIS_REGISTER_COMMANDS(ZSet, MakeCmdAttr<CommandZAdd>("zadd", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZCard>("zcard", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZCount>("zcount", 4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZIncrBy>("zincrby", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZInterStore>("zinterstore", -4, "write slow", CommandZInterStore::Range),
                        MakeCmdAttr<CommandZInter>("zinter", -3, "read-only slow", CommandZInter::Range),
                        MakeCmdAttr<CommandZInterCard>("zintercard", -3, "read-only slow", CommandZInterCard::Range),
                        MakeCmdAttr<CommandZLexCount>("zlexcount", 4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZPopMax>("zpopmax", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZPopMin>("zpopmin", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandBZPopMax>("bzpopmax", -3, "write", 1, -2, 1),
                        MakeCmdAttr<CommandBZPopMin>("bzpopmin", -3, "write", 1, -2, 1),
                        MakeCmdAttr<CommandZMPop>("zmpop", -4, "write", CommandZMPop::Range),
                        MakeCmdAttr<CommandBZMPop>("bzmpop", -5, "write", CommandBZMPop::Range),
                        MakeCmdAttr<CommandZRangeStore>("zrangestore", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandZRange>("zrange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRange>("zrevrange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRangeByLex>("zrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRangeByLex>("zrevrangebylex", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRangeByScore>("zrangebyscore", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRank>("zrank", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRem>("zrem", -3, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandZRemRangeByRank>("zremrangebyrank", 4, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandZRemRangeByScore>("zremrangebyscore", 4, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandZRemRangeByLex>("zremrangebylex", 4, "write no-dbsize-check", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRangeByScore>("zrevrangebyscore", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZRevRank>("zrevrank", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZScore>("zscore", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZMScore>("zmscore", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZScan>("zscan", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZUnionStore>("zunionstore", -4, "write slow", CommandZUnionStore::Range),
                        MakeCmdAttr<CommandZUnion>("zunion", -3, "read-only slow", CommandZUnion::Range),
                        MakeCmdAttr<CommandZRandMember>("zrandmember", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandZDiff>("zdiff", -3, "read-only slow", CommandZDiff::Range),
                        MakeCmdAttr<CommandZDiffStore>("zdiffstore", -3, "read-only slow", CommandZDiffStore::Range), )

}  // namespace redis
