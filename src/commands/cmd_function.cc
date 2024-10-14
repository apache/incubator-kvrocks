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
#include "parse_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "storage/scripting.h"
#include "string_util.h"

namespace redis {

struct CommandFunction : Commander {
  Status Execute(engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    CommandParser parser(args_, 1);

    if (parser.EatEqICase("load")) {
      bool replace = false;
      if (parser.EatEqICase("replace")) {
        replace = true;
      }

      std::string libname;
      auto s = lua::FunctionLoad(conn, GET_OR_RET(parser.TakeStr()), true, replace, &libname);
      if (!s) return s;

      *output = SimpleString(libname);
      return Status::OK();
    } else if (parser.EatEqICase("list")) {
      std::string libname;
      if (parser.EatEqICase("libraryname")) {
        libname = GET_OR_RET(parser.TakeStr());
      }

      bool with_code = false;
      if (parser.EatEqICase("withcode")) {
        with_code = true;
      }

      return lua::FunctionList(srv, conn, libname, with_code, output);
    } else if (parser.EatEqICase("listfunc")) {
      std::string funcname;
      if (parser.EatEqICase("funcname")) {
        funcname = GET_OR_RET(parser.TakeStr());
      }

      return lua::FunctionListFunc(srv, conn, funcname, output);
    } else if (parser.EatEqICase("listlib")) {
      auto libname = GET_OR_RET(parser.TakeStr().Prefixed("expect a library name"));

      return lua::FunctionListLib(srv, conn, libname, output);
    } else if (parser.EatEqICase("delete")) {
      auto libname = GET_OR_RET(parser.TakeStr());
      if (!lua::FunctionIsLibExist(conn, libname)) {
        return {Status::NotOK, "no such library"};
      }
      auto s = lua::FunctionDelete(ctx, srv, libname);
      if (!s) return s;

      *output = SimpleString("OK");
      return Status::OK();
    } else {
      return {Status::NotOK, "no such subcommand"};
    }
  }
};

template <bool read_only = false>
struct CommandFCall : Commander {
  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    int64_t numkeys = GET_OR_RET(ParseInt<int64_t>(args_[2], 10));
    if (numkeys > int64_t(args_.size() - 3)) {
      return {Status::NotOK, "Number of keys can't be greater than number of args"};
    } else if (numkeys < -1) {
      return {Status::NotOK, "Number of keys can't be negative"};
    }

    return lua::FunctionCall(conn, args_[1], std::vector<std::string>(args_.begin() + 3, args_.begin() + 3 + numkeys),
                             std::vector<std::string>(args_.begin() + 3 + numkeys, args_.end()), output, read_only);
  }
};

CommandKeyRange GetScriptEvalKeyRange(const std::vector<std::string> &args);

uint64_t GenerateFunctionFlags(uint64_t flags, const std::vector<std::string> &args) {
  if (util::EqualICase(args[1], "load") || util::EqualICase(args[1], "delete")) {
    return flags | kCmdWrite;
  }

  return flags;
}

REDIS_REGISTER_COMMANDS(
    Function, MakeCmdAttr<CommandFunction>("function", -2, "exclusive no-script", 0, 0, 0, GenerateFunctionFlags),
    MakeCmdAttr<CommandFCall<>>("fcall", -3, "exclusive write no-script", GetScriptEvalKeyRange),
    MakeCmdAttr<CommandFCall<true>>("fcall_ro", -3, "read-only ro-script no-script", GetScriptEvalKeyRange));

}  // namespace redis
