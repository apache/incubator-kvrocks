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
#include "error_constants.h"
#include "parse_util.h"
#include "server/server.h"
#include "storage/scripting.h"

namespace redis {

template <bool evalsha, bool read_only>
class CommandEvalImpl : public Commander {
 public:
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    if (evalsha && args_[1].size() != 40) {
      return {Status::RedisNoScript, errNoMatchingScript};
    }

    int64_t numkeys = GET_OR_RET(ParseInt<int64_t>(args_[2], 10));
    if (numkeys > int64_t(args_.size() - 3)) {
      return {Status::NotOK, "Number of keys can't be greater than number of args"};
    } else if (numkeys < 0) {
      return {Status::NotOK, "Number of keys can't be negative"};
    }

    return lua::EvalGenericCommand(
        conn, args_[1], std::vector<std::string>(args_.begin() + 3, args_.begin() + 3 + numkeys),
        std::vector<std::string>(args_.begin() + 3 + numkeys, args_.end()), evalsha, output, read_only);
  }
};

class CommandEval : public CommandEvalImpl<false, false> {};

class CommandEvalSHA : public CommandEvalImpl<true, false> {};

class CommandEvalRO : public CommandEvalImpl<false, true> {};

class CommandEvalSHARO : public CommandEvalImpl<true, true> {};

class CommandScript : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = util::ToLower(args[1]);
    return Status::OK();
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, [[maybe_unused]] Connection *conn,
                 std::string *output) override {
    // There's a little tricky here since the script command was the write type
    // command but some subcommands like `exists` were readonly, so we want to allow
    // executing on slave here. Maybe we should find other way to do this.
    if (srv->IsSlave() && subcommand_ != "exists") {
      return {Status::NotOK, "READONLY You can't write against a read only slave"};
    }

    if (args_.size() == 2 && subcommand_ == "flush") {
      auto s = srv->ScriptFlush();
      if (!s) {
        LOG(ERROR) << "Failed to flush scripts: " << s.Msg();
        return s;
      }
      s = srv->Propagate(engine::kPropagateScriptCommand, args_);
      if (!s) {
        LOG(ERROR) << "Failed to propagate script command: " << s.Msg();
        return s;
      }
      *output = redis::SimpleString("OK");
    } else if (args_.size() >= 3 && subcommand_ == "exists") {
      *output = redis::MultiLen(args_.size() - 2);
      for (size_t j = 2; j < args_.size(); j++) {
        if (srv->ScriptExists(args_[j]).IsOK()) {
          *output += redis::Integer(1);
        } else {
          *output += redis::Integer(0);
        }
      }
    } else if (args_.size() == 3 && subcommand_ == "load") {
      std::string sha;
      auto s = lua::CreateFunction(srv, args_[2], &sha, srv->Lua(), true);
      if (!s.IsOK()) {
        return s;
      }

      *output = redis::BulkString(sha);
    } else {
      return {Status::NotOK, "Unknown SCRIPT subcommand or wrong number of arguments"};
    }
    return Status::OK();
  }

 private:
  std::string subcommand_;
};

CommandKeyRange GetScriptEvalKeyRange(const std::vector<std::string> &args) {
  auto numkeys = ParseInt<int>(args[2], 10).ValueOr(0);

  return {3, 2 + numkeys, 1};
}

uint64_t GenerateScriptFlags(uint64_t flags, const std::vector<std::string> &args) {
  if (util::EqualICase(args[1], "load") || util::EqualICase(args[1], "flush")) {
    return flags | kCmdWrite;
  }

  return flags;
}

REDIS_REGISTER_COMMANDS(
    Script, MakeCmdAttr<CommandEval>("eval", -3, "exclusive write no-script", GetScriptEvalKeyRange),
    MakeCmdAttr<CommandEvalSHA>("evalsha", -3, "exclusive write no-script", GetScriptEvalKeyRange),
    MakeCmdAttr<CommandEvalRO>("eval_ro", -3, "read-only no-script ro-script", GetScriptEvalKeyRange),
    MakeCmdAttr<CommandEvalSHARO>("evalsha_ro", -3, "read-only no-script ro-script", GetScriptEvalKeyRange),
    MakeCmdAttr<CommandScript>("script", -2, "exclusive no-script", NO_KEY), )

}  // namespace redis
