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

// This file is modified from several source code files about lua scripting of Redis.
// See the original code at https://github.com/redis/redis.

#include "scripting.h"

#include <math.h>

#include <algorithm>
#include <cctype>
#include <string>

#include "commands/commander.h"
#include "commands/error_constants.h"
#include "db_util.h"
#include "fmt/format.h"
#include "lua.h"
#include "parse_util.h"
#include "rand.h"
#include "scope_exit.h"
#include "server/redis_connection.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "sha1.h"
#include "storage/storage.h"

/* The maximum number of characters needed to represent a long double
 * as a string (long double has a huge range).
 * This should be the size of the buffer given to doule to string */
constexpr size_t MAX_LONG_DOUBLE_CHARS = 5 * 1024;

enum {
  LL_DEBUG = 0,
  LL_VERBOSE,
  LL_NOTICE,
  LL_WARNING,
};

namespace lua {

lua_State *CreateState(Server *srv) {
  lua_State *lua = lua_open();
  LoadLibraries(lua);
  RemoveUnsupportedFunctions(lua);
  LoadFuncs(lua);

  lua_pushlightuserdata(lua, srv);
  lua_setglobal(lua, REDIS_LUA_SERVER_PTR);

  EnableGlobalsProtection(lua);
  return lua;
}

void DestroyState(lua_State *lua) {
  lua_gc(lua, LUA_GCCOLLECT, 0);
  lua_close(lua);
}

void LoadFuncs(lua_State *lua) {
  lua_newtable(lua);

  /* redis.call */
  lua_pushstring(lua, "call");
  lua_pushcfunction(lua, RedisCallCommand);
  lua_settable(lua, -3);

  /* redis.pcall */
  lua_pushstring(lua, "pcall");
  lua_pushcfunction(lua, RedisPCallCommand);
  lua_settable(lua, -3);

  /* redis.setresp */
  lua_pushstring(lua, "setresp");
  lua_pushcfunction(lua, RedisSetResp);
  lua_settable(lua, -3);

  /* redis.log and log levels. */
  lua_pushstring(lua, "log");
  lua_pushcfunction(lua, RedisLogCommand);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_DEBUG");
  lua_pushnumber(lua, LL_DEBUG);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_VERBOSE");
  lua_pushnumber(lua, LL_VERBOSE);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_NOTICE");
  lua_pushnumber(lua, LL_NOTICE);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_WARNING");
  lua_pushnumber(lua, LL_WARNING);
  lua_settable(lua, -3);

  /* redis.sha1hex */
  lua_pushstring(lua, "sha1hex");
  lua_pushcfunction(lua, RedisSha1hexCommand);
  lua_settable(lua, -3);

  /* redis.error_reply and redis.status_reply */
  lua_pushstring(lua, "error_reply");
  lua_pushcfunction(lua, RedisErrorReplyCommand);
  lua_settable(lua, -3);
  lua_pushstring(lua, "status_reply");
  lua_pushcfunction(lua, RedisStatusReplyCommand);
  lua_settable(lua, -3);

  /* redis.register_function */
  lua_pushstring(lua, "register_function");
  lua_pushcfunction(lua, RedisRegisterFunction);
  lua_settable(lua, -3);

  lua_setglobal(lua, "redis");

  /* Replace math.random and math.randomseed with our implementations. */
  lua_getglobal(lua, "math");

  lua_pushstring(lua, "random");
  lua_pushcfunction(lua, RedisMathRandom);
  lua_settable(lua, -3);

  lua_pushstring(lua, "randomseed");
  lua_pushcfunction(lua, RedisMathRandomSeed);
  lua_settable(lua, -3);

  lua_setglobal(lua, "math");

  /* Add a helper function we use for pcall error reporting.
   * Note that when the error is in the C function we want to report the
   * information about the caller, that's what makes sense from the point
   * of view of the user debugging a script. */
  const char *err_func =
      "local dbg = debug\n"
      "function __redis__err__handler(err)\n"
      "  local i = dbg.getinfo(2,'nSl')\n"
      "  if i and i.what == 'C' then\n"
      "    i = dbg.getinfo(3,'nSl')\n"
      "  end\n"
      "  if i then\n"
      "    return i.source .. ':' .. i.currentline .. ': ' .. err\n"
      "  else\n"
      "    return err\n"
      "  end\n"
      "end\n";
  luaL_loadbuffer(lua, err_func, strlen(err_func), "@err_handler_def");
  lua_pcall(lua, 0, 0, 0);

  const char *compare_func =
      "function __redis__compare_helper(a,b)\n"
      "  if a == false then a = '' end\n"
      "  if b == false then b = '' end\n"
      "  return a<b\n"
      "end\n";
  luaL_loadbuffer(lua, compare_func, strlen(compare_func), "@cmp_func_def");
  lua_pcall(lua, 0, 0, 0);
}

int RedisLogCommand(lua_State *lua) {
  int argc = lua_gettop(lua);

  if (argc < 2) {
    lua_pushstring(lua, "redis.log() requires two arguments or more.");
    return lua_error(lua);
  }
  if (!lua_isnumber(lua, -argc)) {
    lua_pushstring(lua, "First argument must be a number (log level).");
    return lua_error(lua);
  }
  int level = static_cast<int>(lua_tonumber(lua, -argc));
  if (level < LL_DEBUG || level > LL_WARNING) {
    lua_pushstring(lua, "Invalid debug level.");
    return lua_error(lua);
  }

  std::string log_message;
  for (int j = 1; j < argc; j++) {
    size_t len = 0;
    if (const char *s = lua_tolstring(lua, j - argc, &len)) {
      if (j != 1) {
        log_message += " ";
      }
      log_message += std::string(s, len);
    }
  }

  // The min log level was INFO, DEBUG would never take effect
  switch (level) {
    case LL_VERBOSE:  // also regard VERBOSE as INFO here since no VERBOSE level
    case LL_NOTICE:
      LOG(INFO) << "[Lua] " << log_message;
      break;
    case LL_WARNING:
      LOG(WARNING) << "[Lua] " << log_message;
      break;
  }
  return 0;
}

int RedisRegisterFunction(lua_State *lua) {
  int argc = lua_gettop(lua);

  if (argc < 2 || argc > 3) {
    lua_pushstring(lua, "wrong number of arguments to redis.register_function().");
    return lua_error(lua);
  }

  lua_getglobal(lua, REDIS_FUNCTION_LIBNAME);
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1);
    lua_pushstring(lua, "redis.register_function() need to be called from FUNCTION LOAD.");
    return lua_error(lua);
  }

  std::string libname = lua_tostring(lua, -1);
  lua_pop(lua, 1);

  // set this function to global
  std::string name = lua_tostring(lua, 1);
  if (argc == 3) {
    auto flags = ExtractFlagsFromRegisterFunction(lua);
    if (!flags) {
      lua_pushstring(lua, flags.Msg().c_str());
      return lua_error(lua);
    }
    lua_pushinteger(lua, static_cast<lua_Integer>(flags.GetValue()));
    lua_setglobal(lua, (REDIS_LUA_REGISTER_FUNC_FLAGS_PREFIX + name).c_str());
  }
  lua_setglobal(lua, (REDIS_LUA_REGISTER_FUNC_PREFIX + name).c_str());

  // set this function name to REDIS_FUNCTION_LIBRARIES[libname]
  lua_getglobal(lua, REDIS_FUNCTION_LIBRARIES);
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1);
    lua_newtable(lua);
  }
  lua_getfield(lua, -1, libname.c_str());
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1);
    lua_newtable(lua);
  }
  size_t len = lua_objlen(lua, -1);
  lua_pushstring(lua, name.c_str());
  lua_rawseti(lua, -2, static_cast<int>(len) + 1);
  lua_setfield(lua, -2, libname.c_str());
  lua_setglobal(lua, REDIS_FUNCTION_LIBRARIES);

  // check if it needs to store
  lua_getglobal(lua, REDIS_FUNCTION_NEEDSTORE);
  if (!lua_toboolean(lua, -1)) {
    return 0;
  }

  // store the map from function name to library name
  auto s = GetServer(lua)->FunctionSetLib(name, libname);
  if (!s) {
    lua_pushstring(lua, "redis.register_function() failed to store informantion.");
    return lua_error(lua);
  }
  return 0;
}

Status FunctionLoad(redis::Connection *conn, const std::string &script, bool need_to_store, bool replace,
                    [[maybe_unused]] std::string *lib_name, bool read_only) {
  std::string first_line, lua_code;
  if (auto pos = script.find('\n'); pos != std::string::npos) {
    first_line = script.substr(0, pos);
    lua_code = script.substr(pos + 1);
  } else {
    return {Status::NotOK, "Expect a Shebang statement in the first line"};
  }

  const auto libname = GET_OR_RET(ExtractLibNameFromShebang(first_line));

  auto srv = conn->GetServer();
  auto lua = read_only ? conn->Owner()->Lua() : srv->Lua();

  if (FunctionIsLibExist(conn, libname, need_to_store, read_only)) {
    if (!replace) {
      return {Status::NotOK, "library already exists, please specify REPLACE to force load"};
    }
    engine::Context ctx(srv->storage);
    auto s = FunctionDelete(ctx, srv, libname);
    if (!s) return s;
  }

  lua_pushstring(lua, libname.c_str());
  lua_setglobal(lua, REDIS_FUNCTION_LIBNAME);
  auto libname_exit = MakeScopeExit([lua] {
    lua_pushnil(lua);
    lua_setglobal(lua, REDIS_FUNCTION_LIBNAME);
  });

  lua_pushboolean(lua, need_to_store);
  lua_setglobal(lua, REDIS_FUNCTION_NEEDSTORE);
  auto need_store_exit = MakeScopeExit([lua] {
    lua_pushnil(lua);
    lua_setglobal(lua, REDIS_FUNCTION_NEEDSTORE);
  });

  if (luaL_loadbuffer(lua, lua_code.data(), lua_code.size(), "@user_script")) {
    std::string err_msg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return {Status::NotOK, "Error while compiling new function lib: " + err_msg};
  }

  if (lua_pcall(lua, 0, 0, 0)) {
    std::string err_msg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return {Status::NotOK, "Error while running new function lib: " + err_msg};
  }

  if (!FunctionIsLibExist(conn, libname, false, read_only)) {
    return {Status::NotOK, "Please register some function in FUNCTION LOAD"};
  }

  return need_to_store ? srv->FunctionSetCode(libname, script) : Status::OK();
}

bool FunctionIsLibExist(redis::Connection *conn, const std::string &libname, bool need_check_storage, bool read_only) {
  auto srv = conn->GetServer();
  auto lua = read_only ? conn->Owner()->Lua() : srv->Lua();

  lua_getglobal(lua, REDIS_FUNCTION_LIBRARIES);

  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1);
  } else {
    lua_getfield(lua, -1, libname.c_str());
    if (lua_objlen(lua, -1) == 0) {
      lua_pop(lua, 2);
    } else {
      lua_pop(lua, 2);
      return true;
    }
  }

  if (!need_check_storage) {
    return false;
  }

  std::string code;
  auto s = srv->FunctionGetCode(libname, &code);
  if (!s) return false;

  std::string lib_name;
  s = FunctionLoad(conn, code, false, false, &lib_name, read_only);
  return static_cast<bool>(s);
}

// FunctionCall will firstly find the function in the lua runtime,
// if it is not found, it will try to load the library where the function is located from storage
Status FunctionCall(redis::Connection *conn, const std::string &name, const std::vector<std::string> &keys,
                    const std::vector<std::string> &argv, std::string *output, bool read_only) {
  auto srv = conn->GetServer();
  auto lua = read_only ? conn->Owner()->Lua() : srv->Lua();

  lua_getglobal(lua, "__redis__err__handler");

  lua_getglobal(lua, (REDIS_LUA_REGISTER_FUNC_PREFIX + name).c_str());
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1);

    std::string libname;
    auto s = srv->FunctionGetLib(name, &libname);
    if (!s) return s.Prefixed("No such function name found in storage");

    std::string libcode;
    s = srv->FunctionGetCode(libname, &libcode);
    if (!s) return s;
    s = FunctionLoad(conn, libcode, false, false, &libname, read_only);
    if (!s) return s;

    lua_getglobal(lua, (REDIS_LUA_REGISTER_FUNC_PREFIX + name).c_str());
  }

  ScriptRunCtx script_run_ctx;
  script_run_ctx.flags = read_only ? ScriptFlagType::kScriptNoWrites : 0;
  lua_getglobal(lua, (REDIS_LUA_REGISTER_FUNC_FLAGS_PREFIX + name).c_str());
  if (!lua_isnil(lua, -1)) {
    // It should be ensured that the conversion is successful
    auto function_flags = lua_tointeger(lua, -1);
    script_run_ctx.flags |= function_flags;
  }
  lua_pop(lua, 1);

  SaveOnRegistry(lua, REGISTRY_SCRIPT_RUN_CTX_NAME, &script_run_ctx);

  PushArray(lua, keys);
  PushArray(lua, argv);
  if (lua_pcall(lua, 2, 1, -4)) {
    std::string err_msg = lua_tostring(lua, -1);
    lua_pop(lua, 2);
    return {Status::NotOK, fmt::format("Error while running function `{}`: {}", name, err_msg)};
  } else {
    *output = ReplyToRedisReply(conn, lua);
    lua_pop(lua, 2);
  }

  RemoveFromRegistry(lua, REGISTRY_SCRIPT_RUN_CTX_NAME);

  /* Call the Lua garbage collector from time to time to avoid a
   * full cycle performed by Lua, which adds too latency.
   *
   * The call is performed every LUA_GC_CYCLE_PERIOD executed commands
   * (and for LUA_GC_CYCLE_PERIOD collection steps) because calling it
   * for every command uses too much CPU. */
  constexpr int64_t LUA_GC_CYCLE_PERIOD = 50;
  static int64_t gc_count = 0;

  gc_count++;
  if (gc_count == LUA_GC_CYCLE_PERIOD) {
    lua_gc(lua, LUA_GCSTEP, LUA_GC_CYCLE_PERIOD);
    gc_count = 0;
  }
  return Status::OK();
}

// list all library names and their code (enabled via `with_code`)
Status FunctionList(Server *srv, const redis::Connection *conn, const std::string &libname, bool with_code,
                    std::string *output) {
  std::string start_key = engine::kLuaLibCodePrefix + libname;
  std::string end_key = start_key;
  end_key.back()++;

  engine::Context ctx(srv->storage);
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(end_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto *cf = srv->storage->GetCFHandle(ColumnFamilyID::Propagate);
  auto iter = util::UniqueIterator(ctx, read_options, cf);
  std::vector<std::pair<std::string, std::string>> result;
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    Slice lib = iter->key();
    lib.remove_prefix(strlen(engine::kLuaLibCodePrefix));
    result.emplace_back(lib.ToString(), iter->value().ToString());
  }

  if (auto s = iter->status(); !s.ok()) {
    return {Status::NotOK, s.ToString()};
  }

  output->append(redis::MultiLen(result.size()));
  for (const auto &[lib, code] : result) {
    output->append(conn->HeaderOfMap(with_code ? 2 : 1));
    output->append(redis::BulkString("library_name"));
    output->append(redis::BulkString(lib));
    if (with_code) {
      output->append(redis::BulkString("library_code"));
      output->append(redis::BulkString(code));
    }
  }

  return Status::OK();
}

// extension to Redis Function
// list all function names and their corresponding library names
Status FunctionListFunc(Server *srv, const redis::Connection *conn, const std::string &funcname, std::string *output) {
  std::string start_key = engine::kLuaFuncLibPrefix + funcname;
  std::string end_key = start_key;
  end_key.back()++;

  engine::Context ctx(srv->storage);
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(end_key);
  read_options.iterate_upper_bound = &upper_bound;

  auto *cf = srv->storage->GetCFHandle(ColumnFamilyID::Propagate);
  auto iter = util::UniqueIterator(ctx, read_options, cf);
  std::vector<std::pair<std::string, std::string>> result;
  for (iter->Seek(start_key); iter->Valid(); iter->Next()) {
    Slice func = iter->key();
    func.remove_prefix(strlen(engine::kLuaLibCodePrefix));
    result.emplace_back(func.ToString(), iter->value().ToString());
  }

  if (auto s = iter->status(); !s.ok()) {
    return {Status::NotOK, s.ToString()};
  }

  output->append(redis::MultiLen(result.size()));
  for (const auto &[func, lib] : result) {
    output->append(conn->HeaderOfMap(2));
    output->append(redis::BulkString("function_name"));
    output->append(redis::BulkString(func));
    output->append(redis::BulkString("from_library"));
    output->append(redis::BulkString(lib));
  }

  return Status::OK();
}

// extension to Redis Function
// list detailed informantion of a specific library
// NOTE: it is required to load the library to lua runtime before listing (calling this function)
// i.e. it will output nothing if the library is only in storage but not loaded
Status FunctionListLib(Server *srv, const redis::Connection *conn, const std::string &libname, std::string *output) {
  auto lua = srv->Lua();

  lua_getglobal(lua, REDIS_FUNCTION_LIBRARIES);
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1);
    lua_newtable(lua);
  }

  lua_getfield(lua, -1, libname.c_str());
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 2);

    return {Status::NotOK, "The library is not found or not loaded from storage"};
  }

  output->append(conn->HeaderOfMap(3));
  output->append(redis::BulkString("library_name"));
  output->append(redis::BulkString(libname));
  output->append(redis::BulkString("engine"));
  output->append(redis::BulkString("lua"));

  auto count = lua_objlen(lua, -1);
  output->append(redis::SimpleString("functions"));
  output->append(redis::MultiLen(count));

  for (size_t i = 1; i <= count; ++i) {
    lua_rawgeti(lua, -1, static_cast<int>(i));
    auto func = lua_tostring(lua, -1);
    output->append(redis::BulkString(func));
    lua_pop(lua, 1);
  }

  lua_pop(lua, 2);
  return Status::OK();
}

Status FunctionDelete(engine::Context &ctx, Server *srv, const std::string &name) {
  auto lua = srv->Lua();

  lua_getglobal(lua, REDIS_FUNCTION_LIBRARIES);
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1);
    return {Status::NotOK, "the library does not exist in lua environment"};
  }

  lua_getfield(lua, -1, name.c_str());
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 2);
    return {Status::NotOK, "the library does not exist in lua environment"};
  }

  auto storage = srv->storage;
  auto cf = storage->GetCFHandle(ColumnFamilyID::Propagate);

  for (size_t i = 1; i <= lua_objlen(lua, -1); ++i) {
    lua_rawgeti(lua, -1, static_cast<int>(i));
    std::string func = lua_tostring(lua, -1);
    lua_pushnil(lua);
    lua_setglobal(lua, (REDIS_LUA_REGISTER_FUNC_PREFIX + func).c_str());
    lua_pushnil(lua);
    lua_setglobal(lua, (REDIS_LUA_REGISTER_FUNC_FLAGS_PREFIX + func).c_str());
    auto _ = storage->Delete(ctx, rocksdb::WriteOptions(), cf, engine::kLuaFuncLibPrefix + func);
    lua_pop(lua, 1);
  }

  lua_pop(lua, 1);
  lua_pushnil(lua);
  lua_setfield(lua, -2, name.c_str());
  lua_pop(lua, 1);

  auto s = storage->Delete(ctx, rocksdb::WriteOptions(), cf, engine::kLuaLibCodePrefix + name);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  return Status::OK();
}

Status EvalGenericCommand(redis::Connection *conn, const std::string &body_or_sha, const std::vector<std::string> &keys,
                          const std::vector<std::string> &argv, bool evalsha, std::string *output, bool read_only) {
  Server *srv = conn->GetServer();
  // Use the worker's private Lua VM when entering the read-only mode
  lua_State *lua = read_only ? conn->Owner()->Lua() : srv->Lua();

  /* We obtain the script SHA1, then check if this function is already
   * defined into the Lua state */
  char funcname[2 + 40 + 1] = {};
  memcpy(funcname, REDIS_LUA_FUNC_SHA_PREFIX, sizeof(REDIS_LUA_FUNC_SHA_PREFIX));

  if (!evalsha) {
    SHA1Hex(funcname + 2, body_or_sha.c_str(), body_or_sha.size());
  } else {
    for (int j = 0; j < 40; j++) {
      funcname[j + 2] = static_cast<char>(tolower(body_or_sha[j]));
    }
  }

  /* Push the pcall error handler function on the stack. */
  lua_getglobal(lua, "__redis__err__handler");

  /* Try to lookup the Lua function */
  lua_getglobal(lua, funcname);
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1); /* remove the nil from the stack */
    std::string body;
    if (evalsha) {
      auto s = srv->ScriptGet(funcname + 2, &body);
      if (!s.IsOK()) {
        lua_pop(lua, 1); /* remove the error handler from the stack. */
        return {Status::RedisNoScript, redis::errNoMatchingScript};
      }
    } else {
      body = body_or_sha;
    }

    std::string sha = funcname + 2;
    auto s = CreateFunction(srv, body, &sha, lua, false);
    if (!s.IsOK()) {
      lua_pop(lua, 1); /* remove the error handler from the stack. */
      return s;
    }
    /* Now the following is guaranteed to return non nil */
    lua_getglobal(lua, funcname);
  }

  ScriptRunCtx current_script_run_ctx;
  current_script_run_ctx.flags = read_only ? ScriptFlagType::kScriptNoWrites : 0;
  lua_getglobal(lua, fmt::format(REDIS_LUA_FUNC_SHA_FLAGS, funcname + 2).c_str());
  if (!lua_isnil(lua, -1)) {
    // It should be ensured that the conversion is successful
    auto script_flags = lua_tointeger(lua, -1);
    current_script_run_ctx.flags |= script_flags;
  }
  lua_pop(lua, 1);

  SaveOnRegistry(lua, REGISTRY_SCRIPT_RUN_CTX_NAME, &current_script_run_ctx);

  // For the Lua script, should be always run with RESP2 protocol,
  // unless users explicitly set the protocol version in the script via `redis.setresp`.
  // So we need to save the current protocol version and set it to RESP2,
  // and then restore it after the script execution.
  auto saved_protocol_version = conn->GetProtocolVersion();
  conn->SetProtocolVersion(redis::RESP::v2);
  /* Populate the argv and keys table accordingly to the arguments that
   * EVAL received. */
  SetGlobalArray(lua, "KEYS", keys);
  SetGlobalArray(lua, "ARGV", argv);

  if (lua_pcall(lua, 0, 1, -2)) {
    auto msg = fmt::format("running script (call to {}): {}", funcname, lua_tostring(lua, -1));
    *output = redis::Error({Status::NotOK, msg});
    lua_pop(lua, 2);
  } else {
    *output = ReplyToRedisReply(conn, lua);
    lua_pop(lua, 2);
  }
  conn->SetProtocolVersion(saved_protocol_version);

  // clean global variables to prevent information leak in function commands
  lua_pushnil(lua);
  lua_setglobal(lua, "KEYS");
  lua_pushnil(lua);
  lua_setglobal(lua, "ARGV");
  RemoveFromRegistry(lua, REGISTRY_SCRIPT_RUN_CTX_NAME);

  /* Call the Lua garbage collector from time to time to avoid a
   * full cycle performed by Lua, which adds too latency.
   *
   * The call is performed every LUA_GC_CYCLE_PERIOD executed commands
   * (and for LUA_GC_CYCLE_PERIOD collection steps) because calling it
   * for every command uses too much CPU. */
  constexpr int64_t LUA_GC_CYCLE_PERIOD = 50;
  static int64_t gc_count = 0;

  gc_count++;
  if (gc_count == LUA_GC_CYCLE_PERIOD) {
    lua_gc(lua, LUA_GCSTEP, LUA_GC_CYCLE_PERIOD);
    gc_count = 0;
  }

  return Status::OK();
}

bool ScriptExists(lua_State *lua, const std::string &sha) {
  lua_getglobal(lua, (REDIS_LUA_FUNC_SHA_PREFIX + sha).c_str());
  auto exit = MakeScopeExit([lua] { lua_pop(lua, 1); });
  return !lua_isnil(lua, -1);
}

int RedisCallCommand(lua_State *lua) { return RedisGenericCommand(lua, 1); }

int RedisPCallCommand(lua_State *lua) { return RedisGenericCommand(lua, 0); }

Server *GetServer(lua_State *lua) {
  lua_getglobal(lua, REDIS_LUA_SERVER_PTR);
  auto srv = reinterpret_cast<Server *>(lua_touserdata(lua, -1));
  lua_pop(lua, 1);

  return srv;
}

// TODO: we do not want to repeat same logic as Connection::ExecuteCommands,
// so the function need to be refactored
int RedisGenericCommand(lua_State *lua, int raise_error) {
  auto *script_run_ctx = GetFromRegistry<ScriptRunCtx>(lua, REGISTRY_SCRIPT_RUN_CTX_NAME);
  CHECK_NOTNULL(script_run_ctx);

  int argc = lua_gettop(lua);
  if (argc == 0) {
    PushError(lua, "Please specify at least one argument for redis.call()");
    return raise_error ? RaiseError(lua) : 1;
  }

  std::vector<std::string> args;
  for (int j = 1; j <= argc; j++) {
    if (lua_type(lua, j) == LUA_TNUMBER) {
      lua_Number num = lua_tonumber(lua, j);
      args.emplace_back(fmt::format("{:.17g}", static_cast<double>(num)));
    } else {
      size_t obj_len = 0;
      const char *obj_s = lua_tolstring(lua, j, &obj_len);
      if (obj_s == nullptr) {
        PushError(lua, "Lua redis.call() command arguments must be strings or integers");
        return raise_error ? RaiseError(lua) : 1;
      }
      args.emplace_back(obj_s, obj_len);
    }
  }

  auto cmd_s = Server::LookupAndCreateCommand(args[0]);
  if (!cmd_s) {
    PushError(lua, "Unknown Redis command called from Lua script");
    return raise_error ? RaiseError(lua) : 1;
  }
  auto cmd = *std::move(cmd_s);

  auto attributes = cmd->GetAttributes();
  auto cmd_flags = attributes->GenerateFlags(args);

  if ((script_run_ctx->flags & ScriptFlagType::kScriptNoWrites) && !(cmd_flags & redis::kCmdReadOnly)) {
    PushError(lua, "Write commands are not allowed from read-only scripts");
    return raise_error ? RaiseError(lua) : 1;
  }

  if (!attributes->CheckArity(argc)) {
    PushError(lua, "Wrong number of args calling Redis command From Lua script");
    return raise_error ? RaiseError(lua) : 1;
  }

  if (cmd_flags & redis::kCmdNoScript) {
    PushError(lua, "This Redis command is not allowed from scripts");
    return raise_error ? RaiseError(lua) : 1;
  }

  std::string cmd_name = attributes->name;

  auto srv = GetServer(lua);
  Config *config = srv->GetConfig();

  redis::Connection *conn = srv->GetCurrentConnection();
  if (config->cluster_enabled) {
    if (script_run_ctx->flags & ScriptFlagType::kScriptNoCluster) {
      PushError(lua, "Can not run script on cluster, 'no-cluster' flag is set");
      return raise_error ? RaiseError(lua) : 1;
    }
    auto s = srv->cluster->CanExecByMySelf(attributes, args, conn, script_run_ctx);
    if (!s.IsOK()) {
      if (s.Is<Status::RedisMoved>()) {
        PushError(lua, "Script attempted to access a non local key in a cluster node script");
      } else {
        PushError(lua, redis::StatusToRedisErrorMsg(s).c_str());
      }
      return raise_error ? RaiseError(lua) : 1;
    }
  }

  if (config->slave_readonly && srv->IsSlave() && (cmd_flags & redis::kCmdWrite)) {
    PushError(lua, "READONLY You can't write against a read only slave.");
    return raise_error ? RaiseError(lua) : 1;
  }

  if (!config->slave_serve_stale_data && srv->IsSlave() && cmd_name != "info" && cmd_name != "slaveof" &&
      srv->GetReplicationState() != kReplConnected) {
    PushError(lua,
              "MASTERDOWN Link with MASTER is down "
              "and slave-serve-stale-data is set to 'no'.");
    return raise_error ? RaiseError(lua) : 1;
  }

  cmd->SetArgs(args);
  auto s = cmd->Parse();
  if (!s) {
    PushError(lua, s.Msg().data());
    return raise_error ? RaiseError(lua) : 1;
  }

  std::string output;
  // TODO: make it possible for multiple redis commands in lua script to use the same txn context.
  {
    engine::Context ctx(srv->storage);
    s = conn->ExecuteCommand(ctx, cmd_name, args, cmd.get(), &output);
    if (!s) {
      PushError(lua, s.Msg().data());
      return raise_error ? RaiseError(lua) : 1;
    }
  }

  srv->FeedMonitorConns(conn, args);

  RedisProtocolToLuaType(lua, output.data());
  return 1;
}

void RemoveUnsupportedFunctions(lua_State *lua) {
  lua_pushnil(lua);
  lua_setglobal(lua, "loadfile");
  lua_pushnil(lua);
  lua_setglobal(lua, "dofile");
}

void EnableGlobalsProtection(lua_State *lua) {
  const char *code =
      "local dbg=debug\n"
      "local mt = {}\n"
      "setmetatable(_G, mt)\n"
      "mt.__newindex = function (t, n, v)\n"
      "  if dbg.getinfo(2) then\n"
      "    local w = dbg.getinfo(2, \"S\").what\n"
      "    if w ~= \"user_script\" and w ~= \"C\" then\n"
      "      error(\"Script attempted to create global variable '\"..tostring(n)..\"'\", 2)\n"
      "    end\n"
      "  end\n"
      "  rawset(t, n, v)\n"
      "end\n"
      "mt.__index = function (t, n)\n"
      "  if dbg.getinfo(2) and dbg.getinfo(2, \"S\").what ~= \"C\" then\n"
      "    error(\"Script attempted to access nonexistent global variable '\"..tostring(n)..\"'\", 2)\n"
      "  end\n"
      "  return rawget(t, n)\n"
      "end\n"
      "debug = nil\n";

  luaL_loadbuffer(lua, code, strlen(code), "@enable_strict_lua");
  lua_pcall(lua, 0, 0, 0);
}

void LoadLibraries(lua_State *lua) {
  auto load_lib = [](lua_State *lua, const char *libname, lua_CFunction func) {
    lua_pushcfunction(lua, func);
    lua_pushstring(lua, libname);
    lua_call(lua, 1, 0);
  };

  load_lib(lua, "", luaopen_base);
  load_lib(lua, LUA_TABLIBNAME, luaopen_table);
  load_lib(lua, LUA_STRLIBNAME, luaopen_string);
  load_lib(lua, LUA_MATHLIBNAME, luaopen_math);
  load_lib(lua, LUA_DBLIBNAME, luaopen_debug);
  load_lib(lua, "cjson", luaopen_cjson);
  load_lib(lua, "struct", luaopen_struct);
  load_lib(lua, "cmsgpack", luaopen_cmsgpack);
  load_lib(lua, "bit", luaopen_bit);
}

/* Returns a table with a single field 'field' set to the string value
 * passed as argument. This helper function is handy when returning
 * a Redis Protocol error or status reply from Lua:
 *
 * return redis.error_reply("ERR Some Error")
 * return redis.status_reply("ERR Some Error")
 */
int RedisReturnSingleFieldTable(lua_State *lua, const char *field) {
  if (lua_gettop(lua) != 1 || lua_type(lua, -1) != LUA_TSTRING) {
    PushError(lua, "wrong number or type of arguments");
    return 1;
  }

  lua_newtable(lua);
  lua_pushstring(lua, field);
  lua_pushvalue(lua, -3);
  lua_settable(lua, -3);
  return 1;
}

int RedisSetResp(lua_State *lua) {
  auto srv = GetServer(lua);
  auto conn = srv->GetCurrentConnection();

  if (lua_gettop(lua) != 1) {
    PushError(lua, "redis.setresp() requires one argument.");
    return RaiseError(lua);
  }

  auto resp = static_cast<int>(lua_tonumber(lua, -1));
  if (resp != 2 && resp != 3) {
    PushError(lua, "RESP version must be 2 or 3.");
    return RaiseError(lua);
  }
  conn->SetProtocolVersion(resp == 2 ? redis::RESP::v2 : redis::RESP::v3);
  if (resp == 3 && !srv->GetConfig()->resp3_enabled) {
    PushError(lua, "You need set resp3-enabled to yes to enable RESP3.");
    return RaiseError(lua);
  }
  return 0;
}

/* redis.error_reply() */
int RedisErrorReplyCommand(lua_State *lua) { return RedisReturnSingleFieldTable(lua, "err"); }

/* redis.status_reply() */
int RedisStatusReplyCommand(lua_State *lua) { return RedisReturnSingleFieldTable(lua, "ok"); }

/* This adds redis.sha1hex(string) to Lua scripts using the same hashing
 * function used for sha1ing lua scripts. */
int RedisSha1hexCommand(lua_State *lua) {
  int argc = lua_gettop(lua);

  if (argc != 1) {
    lua_pushstring(lua, "wrong number of arguments");
    return lua_error(lua);
  }

  size_t len = 0;
  const char *s = static_cast<const char *>(lua_tolstring(lua, 1, &len));

  char digest[41];
  SHA1Hex(digest, s, len);
  lua_pushstring(lua, digest);
  return 1;
}

/* ---------------------------------------------------------------------------
 * Utility functions.
 * ------------------------------------------------------------------------- */

/* Perform the SHA1 of the input string. We use this both for hashing script
 * bodies in order to obtain the Lua function name, and in the implementation
 * of redis.sha1().
 *
 * 'digest' should point to a 41 bytes buffer: 40 for SHA1 converted into an
 * hexadecimal number, plus 1 byte for null term. */
void SHA1Hex(char *digest, const char *script, size_t len) {
  SHA1_CTX ctx;
  unsigned char hash[20];
  const char *cset = "0123456789abcdef";

  SHA1Init(&ctx);
  SHA1Update(&ctx, (const unsigned char *)script, len);
  SHA1Final(hash, &ctx);

  for (int j = 0; j < 20; j++) {
    digest[j * 2] = cset[((hash[j] & 0xF0) >> 4)];
    digest[j * 2 + 1] = cset[(hash[j] & 0xF)];
  }
  digest[40] = '\0';
}

/*
 * ---------------------------------------------------------------------------
 * Redis reply to Lua type conversion functions.
 * ------------------------------------------------------------------------- */

/* Take a Redis reply in the Redis protocol format and convert it into a
 * Lua type. Thanks to this function, and the introduction of not connected
 * clients, it is trivial to implement the redis() lua function.
 *
 * Basically we take the arguments, execute the Redis command in the context
 * of a non connected client, then take the generated reply and convert it
 * into a suitable Lua type. With this trick the scripting feature does not
 * need the introduction of a full Redis internals API. The script
 * is like a normal client that bypasses all the slow I/O paths.
 *
 * Note: in this function we do not do any sanity check as the reply is
 * generated by Redis directly. This allows us to go faster.
 *
 * Errors are returned as a table with a single 'err' field set to the
 * error string.
 */

const char *RedisProtocolToLuaType(lua_State *lua, const char *reply) {
  const char *p = reply;

  switch (*p) {
    case ':':
      p = RedisProtocolToLuaTypeInt(lua, reply);
      break;
    case '$':
      p = RedisProtocolToLuaTypeBulk(lua, reply);
      break;
    case '+':
      p = RedisProtocolToLuaTypeStatus(lua, reply);
      break;
    case '-':
      p = RedisProtocolToLuaTypeError(lua, reply);
      break;
    case '*':
      p = RedisProtocolToLuaTypeAggregate(lua, reply, *p);
      break;
    case '%':
      p = RedisProtocolToLuaTypeAggregate(lua, reply, *p);
      break;
    case '~':
      p = RedisProtocolToLuaTypeAggregate(lua, reply, *p);
      break;
    case '_':
      p = RedisProtocolToLuaTypeNull(lua, reply);
      break;
    case '#':
      p = RedisProtocolToLuaTypeBool(lua, reply, p[1]);
      break;
    case ',':
      p = RedisProtocolToLuaTypeDouble(lua, reply);
      break;
    case '(':
      p = RedisProtocolToLuaTypeBigNumber(lua, reply);
      break;
    case '=':
      p = RedisProtocolToLuaTypeVerbatimString(lua, reply);
      break;
  }
  return p;
}

const char *RedisProtocolToLuaTypeInt(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  auto value = ParseInt<int64_t>(std::string(reply + 1, p - reply - 1), 10).ValueOr(0);
  lua_pushnumber(lua, static_cast<lua_Number>(value));
  return p + 2;
}

const char *RedisProtocolToLuaTypeBulk(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  auto bulklen = ParseInt<int64_t>(std::string(reply + 1, p - reply - 1), 10).ValueOr(0);

  if (bulklen == -1) {
    lua_pushboolean(lua, 0);
    return p + 2;
  } else {
    lua_pushlstring(lua, p + 2, bulklen);
    return p + 2 + bulklen + 2;
  }
}

const char *RedisProtocolToLuaTypeStatus(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');

  lua_newtable(lua);
  lua_pushstring(lua, "ok");
  lua_pushlstring(lua, reply + 1, p - reply - 1);
  lua_settable(lua, -3);
  return p + 2;
}

const char *RedisProtocolToLuaTypeError(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');

  lua_newtable(lua);
  lua_pushstring(lua, "err");
  lua_pushlstring(lua, reply + 1, p - reply - 1);
  lua_settable(lua, -3);
  return p + 2;
}

const char *RedisProtocolToLuaTypeAggregate(lua_State *lua, const char *reply, int atype) {
  const char *p = strchr(reply + 1, '\r');
  int64_t mbulklen = ParseInt<int64_t>(std::string(reply + 1, p - reply - 1), 10).ValueOr(0);
  int j = 0;

  p += 2;
  if (mbulklen == -1) {
    lua_pushboolean(lua, 0);
    return p;
  }
  if (atype == '*') {
    lua_newtable(lua);
    for (j = 0; j < mbulklen; j++) {
      lua_pushnumber(lua, j + 1);
      p = RedisProtocolToLuaType(lua, p);
      lua_settable(lua, -3);
    }
    return p;
  }

  CHECK(atype == '%' || atype == '~');
  if (atype == '%' || atype == '~') {
    lua_newtable(lua);
    lua_pushstring(lua, atype == '%' ? "map" : "set");
    lua_newtable(lua);
    for (j = 0; j < mbulklen; j++) {
      p = RedisProtocolToLuaType(lua, p);
      if (atype == '%') {  // map
        p = RedisProtocolToLuaType(lua, p);
      } else {  // set
        lua_pushboolean(lua, 1);
      }
      lua_settable(lua, -3);
    }
    lua_settable(lua, -3);
    return p;
  }

  // Unreachable, return the original position if it did reach here.
  return reply;
}

const char *RedisProtocolToLuaTypeNull(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  lua_pushnil(lua);
  return p + 2;
}

const char *RedisProtocolToLuaTypeBool(lua_State *lua, const char *reply, int tf) {
  const char *p = strchr(reply + 1, '\r');
  lua_pushboolean(lua, tf == 't');
  return p + 2;
}

const char *RedisProtocolToLuaTypeDouble(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  char buf[MAX_LONG_DOUBLE_CHARS + 1];
  size_t len = p - reply - 1;
  double d = NAN;

  if (len <= MAX_LONG_DOUBLE_CHARS) {
    memcpy(buf, reply + 1, len);
    buf[len] = '\0';
    d = strtod(buf, nullptr); /* We expect a valid representation. */
  } else {
    d = 0;
  }

  lua_newtable(lua);
  lua_pushstring(lua, "double");
  lua_pushnumber(lua, d);
  lua_settable(lua, -3);
  return p + 2;
}

const char *RedisProtocolToLuaTypeBigNumber(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  lua_newtable(lua);
  lua_pushstring(lua, "big_number");
  lua_pushlstring(lua, reply + 1, p - reply - 1);
  lua_settable(lua, -3);
  return p + 2;
}

const char *RedisProtocolToLuaTypeVerbatimString(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  int64_t bulklen = ParseInt<int64_t>(std::string(reply + 1, p - reply - 1), 10).ValueOr(0);
  p += 2;  // skip \r\n

  lua_newtable(lua);
  lua_pushstring(lua, "verbatim_string");

  lua_newtable(lua);
  lua_pushstring(lua, "string");
  lua_pushlstring(lua, p + 4, bulklen - 4);
  lua_settable(lua, -3);

  lua_pushstring(lua, "format");
  lua_pushlstring(lua, p, 3);
  lua_settable(lua, -3);

  lua_settable(lua, -3);
  return p + bulklen + 2;
}

/* This function is used in order to push an error on the Lua stack in the
 * format used by redis.pcall to return errors, which is a lua table
 * with a single "err" field set to the error string. Note that this
 * table is never a valid reply by proper commands, since the returned
 * tables are otherwise always indexed by integers, never by strings. */
void PushError(lua_State *lua, const char *err) {
  lua_newtable(lua);
  lua_pushstring(lua, "err");
  lua_pushstring(lua, err);
  lua_settable(lua, -3);
}

// this function does not pop any element on the stack
std::string ReplyToRedisReply(redis::Connection *conn, lua_State *lua) {
  std::string output;
  const char *obj_s = nullptr;
  size_t obj_len = 0;
  int j = 0, mbulklen = 0;

  int t = lua_type(lua, -1);
  switch (t) {
    case LUA_TSTRING:
      obj_s = lua_tolstring(lua, -1, &obj_len);
      output = redis::BulkString(std::string(obj_s, obj_len));
      break;
    case LUA_TBOOLEAN:
      if (conn->GetProtocolVersion() == redis::RESP::v2) {
        output = lua_toboolean(lua, -1) ? redis::Integer(1) : conn->NilString();
      } else {
        output = conn->Bool(lua_toboolean(lua, -1));
      }
      break;
    case LUA_TNUMBER:
      output = redis::Integer((int64_t)(lua_tonumber(lua, -1)));
      break;
    case LUA_TTABLE:
      /* We need to check if it is an array, an error, or a status reply.
       * Error are returned as a single element table with 'err' field.
       * Status replies are returned as single element table with 'ok'
       * field. */

      /* Handle error reply. */
      lua_pushstring(lua, "err");
      lua_rawget(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TSTRING) {
        output = redis::Error({Status::RedisErrorNoPrefix, lua_tostring(lua, -1)});
        lua_pop(lua, 1);
        return output;
      }
      lua_pop(lua, 1); /* Discard field name pushed before. */

      /* Handle status reply. */
      lua_pushstring(lua, "ok");
      lua_rawget(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TSTRING) {
        obj_s = lua_tolstring(lua, -1, &obj_len);
        output = redis::BulkString(std::string(obj_s, obj_len));
        lua_pop(lua, 1);
        return output;
      }
      lua_pop(lua, 1); /* Discard the 'ok' field value we pushed */

      /* Handle double reply. */
      lua_pushstring(lua, "double");
      lua_rawget(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TNUMBER) {
        output = conn->Double(lua_tonumber(lua, -1));
        lua_pop(lua, 1);
        return output;
      }
      lua_pop(lua, 1); /* Discard the 'double' field value we pushed */

      /* Handle big number reply. */
      lua_pushstring(lua, "big_number");
      lua_rawget(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TSTRING) {
        obj_s = lua_tolstring(lua, -1, &obj_len);
        output = conn->BigNumber(std::string(obj_s, obj_len));
        lua_pop(lua, 1);
        return output;
      }
      lua_pop(lua, 1); /* Discard the 'big_number' field value we pushed */

      /* Handle verbatim reply. */
      lua_pushstring(lua, "verbatim_string");
      lua_rawget(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TTABLE) {
        lua_pushstring(lua, "format");
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TSTRING) {
          const char *format = lua_tostring(lua, -1);
          lua_pushstring(lua, "string");
          lua_rawget(lua, -3);
          t = lua_type(lua, -1);
          if (t == LUA_TSTRING) {
            obj_s = lua_tolstring(lua, -1, &obj_len);
            output = conn->VerbatimString(std::string(format), std::string(obj_s, obj_len));
            lua_pop(lua, 4);
            return output;
          }
          // discard 'string'
          lua_pop(lua, 1);
        }
        // discard 'format'
        lua_pop(lua, 1);
      }
      lua_pop(lua, 1); /* Discard the 'verbatim_string' field value we pushed */

      /* Handle map reply. */
      lua_pushstring(lua, "map");
      lua_rawget(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TTABLE) {
        int map_len = 0;
        std::string map_output;
        lua_pushnil(lua);
        while (lua_next(lua, -2)) {
          lua_pushvalue(lua, -2);
          // return key
          map_output += ReplyToRedisReply(conn, lua);
          lua_pop(lua, 1);
          // return value
          map_output += ReplyToRedisReply(conn, lua);
          lua_pop(lua, 1);
          map_len++;
        }
        output = conn->HeaderOfMap(map_len) + std::move(map_output);
        lua_pop(lua, 1);
        return output;
      }
      lua_pop(lua, 1); /* Discard the 'map' field value we pushed */

      /* Handle set reply. */
      lua_pushstring(lua, "set");
      lua_rawget(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TTABLE) {
        int set_len = 0;
        std::string set_output;
        lua_pushnil(lua);
        while (lua_next(lua, -2)) {
          lua_pop(lua, 1);
          lua_pushvalue(lua, -1);
          set_output += ReplyToRedisReply(conn, lua);
          lua_pop(lua, 1);
          set_len++;
        }
        output = conn->HeaderOfSet(set_len) + std::move(set_output);
        lua_pop(lua, 1);
        return output;
      }
      lua_pop(lua, 1); /* Discard the 'set' field value we pushed */

      j = 1, mbulklen = 0;
      while (true) {
        lua_pushnumber(lua, j++);
        lua_rawget(lua, -2);
        t = lua_type(lua, -1);
        if (t == LUA_TNIL) {
          lua_pop(lua, 1);
          break;
        }
        mbulklen++;
        output += ReplyToRedisReply(conn, lua);
        lua_pop(lua, 1);
      }
      output = redis::MultiLen(mbulklen) + output;
      break;
    default:
      output = conn->NilString();
  }
  return output;
}

/* In case the error set into the Lua stack by pushError() was generated
 * by the non-error-trapping version of redis.pcall(), which is redis.call(),
 * this function will raise the Lua error so that the execution of the
 * script will be halted. */
[[noreturn]] int RaiseError(lua_State *lua) {
  lua_pushstring(lua, "err");
  lua_gettable(lua, -2);
  lua_error(lua);
  __builtin_unreachable();
}

/* Sort the array currently in the stack. We do this to make the output
 * of commands like KEYS or SMEMBERS something deterministic when called
 * from Lua (to play well with AOf/replication).
 *
 * The array is sorted using table.sort itself, and assuming all the
 * list elements are strings. */
void SortArray(lua_State *lua) {
  /* Initial Stack: array */
  lua_getglobal(lua, "table");
  lua_pushstring(lua, "sort");
  lua_gettable(lua, -2);  /* Stack: array, table, table.sort */
  lua_pushvalue(lua, -3); /* Stack: array, table, table.sort, array */
  if (lua_pcall(lua, 1, 0, 0)) {
    /* Stack: array, table, error */

    /* We are not interested in the error, we assume that the problem is
     * that there are 'false' elements inside the array, so we try
     * again with a slower function but able to handle this case, that
     * is: table.sort(table, __redis__compare_helper) */
    lua_pop(lua, 1);             /* Stack: array, table */
    lua_pushstring(lua, "sort"); /* Stack: array, table, sort */
    lua_gettable(lua, -2);       /* Stack: array, table, table.sort */
    lua_pushvalue(lua, -3);      /* Stack: array, table, table.sort, array */
    lua_getglobal(lua, "__redis__compare_helper");
    /* Stack: array, table, table.sort, array, __redis__compare_helper */
    lua_call(lua, 2, 0);
  }
  /* Stack: array (sorted), table */
  lua_pop(lua, 1); /* Stack: array (sorted) */
}

void SetGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems) {
  PushArray(lua, elems);
  lua_setglobal(lua, var.c_str());
}

void PushArray(lua_State *lua, const std::vector<std::string> &elems) {
  lua_newtable(lua);
  for (size_t i = 0; i < elems.size(); i++) {
    lua_pushlstring(lua, elems[i].c_str(), elems[i].size());
    lua_rawseti(lua, -2, static_cast<int>(i) + 1);
  }
}

/* ---------------------------------------------------------------------------
 * Redis provided math.random
 * ------------------------------------------------------------------------- */

/* We replace math.random() with our implementation that is not affected
 * by specific libc random() implementations and will output the same sequence
 * (for the same seed) in every arch. */

/* The following implementation is the one shipped with Lua itself but with
 * rand() replaced by redisLrand48(). */
int RedisMathRandom(lua_State *lua) {
  /* the `%' avoids the (rare) case of r==1, and is needed also because on
     some systems (SunOS!) `rand()' may return a value larger than RAND_MAX */
  lua_Number r = (lua_Number)(RedisLrand48() % REDIS_LRAND48_MAX) / (lua_Number)REDIS_LRAND48_MAX;
  switch (lua_gettop(lua)) {  /* check number of arguments */
    case 0: {                 /* no arguments */
      lua_pushnumber(lua, r); /* Number between 0 and 1 */
      break;
    }
    case 1: { /* only upper limit */
      int u = luaL_checkint(lua, 1);
      luaL_argcheck(lua, 1 <= u, 1, "interval is empty");
      lua_pushnumber(lua, floor(r * u) + 1); /* int between 1 and `u' */
      break;
    }
    case 2: { /* lower and upper limits */
      int l = luaL_checkint(lua, 1);
      int u = luaL_checkint(lua, 2);
      luaL_argcheck(lua, l <= u, 2, "interval is empty");
      lua_pushnumber(lua, floor(r * (u - l + 1)) + l); /* int between `l' and `u' */
      break;
    }
    default:
      return luaL_error(lua, "wrong number of arguments");
  }
  return 1;
}

int RedisMathRandomSeed(lua_State *lua) {
  RedisSrand48(luaL_checkint(lua, 1));
  return 0;
}

/* ---------------------------------------------------------------------------
 * EVAL and SCRIPT commands implementation
 * ------------------------------------------------------------------------- */

/* Define a Lua function with the specified body.
 * The function name will be generated in the following form:
 *
 *   f_<hex sha1 sum>
 *
 * The function increments the reference count of the 'body' object as a
 * side effect of a successful call.
 *
 * On success a pointer to an SDS string representing the function SHA1 of the
 * just added function is returned (and will be valid until the next call
 * to scriptingReset() function), otherwise NULL is returned.
 *
 * The function handles the fact of being called with a script that already
 * exists, and in such a case, it behaves like in the success case.
 *
 * If 'c' is not NULL, on error the client is informed with an appropriate
 * error describing the nature of the problem and the Lua interpreter error. */
Status CreateFunction(Server *srv, const std::string &body, std::string *sha, lua_State *lua, bool need_to_store) {
  char funcname[2 + 40 + 1] = {};
  memcpy(funcname, REDIS_LUA_FUNC_SHA_PREFIX, sizeof(REDIS_LUA_FUNC_SHA_PREFIX));

  if (sha->empty()) {
    SHA1Hex(funcname + 2, body.c_str(), body.size());
    *sha = funcname + 2;
  } else {
    std::copy(sha->begin(), sha->end(), funcname + 2);
  }

  std::string_view lua_code(body);
  // Cache the flags of the current script
  ScriptFlags script_flags = 0;
  if (auto pos = body.find('\n'); pos != std::string::npos) {
    std::string_view first_line(body.data(), pos);
    if (first_line.substr(0, 2) == "#!") {
      lua_code = lua_code.substr(pos + 1);
    }
    script_flags = GET_OR_RET(ExtractFlagsFromShebang(first_line));
  } else {
    // scripts without #! can run commands that access keys belonging to different cluster hash slots
    script_flags = kScriptAllowCrossSlotKeys;
  }
  lua_pushinteger(lua, static_cast<lua_Integer>(script_flags));
  lua_setglobal(lua, fmt::format(REDIS_LUA_FUNC_SHA_FLAGS, *sha).c_str());

  if (luaL_loadbuffer(lua, lua_code.data(), lua_code.size(), "@user_script")) {
    std::string err_msg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return {Status::NotOK, "Error while compiling new script: " + err_msg};
  }
  lua_setglobal(lua, funcname);

  // would store lua function into propagate column family and propagate those scripts to slaves
  return need_to_store ? srv->ScriptSet(*sha, body) : Status::OK();
}

[[nodiscard]] StatusOr<std::string> ExtractLibNameFromShebang(std::string_view shebang) {
  static constexpr std::string_view lua_shebang_prefix = "#!lua";
  static constexpr std::string_view shebang_libname_prefix = "name=";

  if (shebang.substr(0, 2) != "#!") {
    return {Status::NotOK, "Missing library meta"};
  }

  auto shebang_splits = util::Split(shebang, " ");
  if (shebang_splits.empty() || shebang_splits[0] != lua_shebang_prefix) {
    return {Status::NotOK, "Unexpected engine in script shebang: " + shebang_splits[0]};
  }

  std::string libname;
  bool found_libname = false;
  for (size_t i = 1; i < shebang_splits.size(); i++) {
    std::string_view shebang_split_sv = shebang_splits[i];
    if (shebang_split_sv.substr(0, shebang_libname_prefix.size()) != shebang_libname_prefix) {
      return {Status::NotOK, "Unknown lua shebang option: " + shebang_splits[i]};
    }
    if (found_libname) {
      return {Status::NotOK, "Redundant library name in script shebang"};
    }

    libname = shebang_split_sv.substr(shebang_libname_prefix.size());
    if (libname.empty() ||
        std::any_of(libname.begin(), libname.end(), [](char v) { return !std::isalnum(v) && v != '_'; })) {
      return {
          Status::NotOK,
          "Library names can only contain letters, numbers, or underscores(_) and must be at least one character long"};
    }
    found_libname = true;
  }

  if (found_libname) return libname;
  return {Status::NotOK, "Library name was not given"};
}

[[nodiscard]] StatusOr<ScriptFlags> GetFlagsFromStrings(const std::vector<std::string> &flags_content) {
  ScriptFlags flags = 0;
  for (const auto &flag : flags_content) {
    if (flag == "no-writes") {
      flags |= kScriptNoWrites;
    } else if (flag == "allow-oom") {
      return {Status::NotSupported, "allow-oom is not supported yet"};
    } else if (flag == "allow-stale") {
      return {Status::NotSupported, "allow-stale is not supported yet"};
    } else if (flag == "no-cluster") {
      flags |= kScriptNoCluster;
    } else if (flag == "allow-cross-slot-keys") {
      flags |= kScriptAllowCrossSlotKeys;
    } else {
      return {Status::NotOK, "Unknown flag given: " + flag};
    }
  }
  return flags;
}

[[nodiscard]] StatusOr<ScriptFlags> ExtractFlagsFromShebang(std::string_view shebang) {
  static constexpr std::string_view lua_shebang_prefix = "#!lua";
  static constexpr std::string_view shebang_flags_prefix = "flags=";

  ScriptFlags result_flags = 0;
  if (shebang.substr(0, 2) == "#!") {
    auto shebang_splits = util::Split(shebang, " ");
    if (shebang_splits.empty() || shebang_splits[0] != lua_shebang_prefix) {
      return {Status::NotOK, "Unexpected engine in script shebang: " + shebang_splits[0]};
    }
    bool found_flags = false;
    for (size_t i = 1; i < shebang_splits.size(); i++) {
      std::string_view shebang_split_sv = shebang_splits[i];
      if (shebang_split_sv.substr(0, shebang_flags_prefix.size()) != shebang_flags_prefix) {
        return {Status::NotOK, "Unknown lua shebang option: " + shebang_splits[i]};
      }
      if (found_flags) {
        return {Status::NotOK, "Redundant flags in script shebang"};
      }
      auto flags_content = util::Split(shebang_split_sv.substr(shebang_flags_prefix.size()), ",");
      result_flags |= GET_OR_RET(GetFlagsFromStrings(flags_content));
      found_flags = true;
    }
  } else {
    // scripts without #! can run commands that access keys belonging to different cluster hash slots,
    // but ones with #! inherit the default flags, so they cannot.
    result_flags = kScriptAllowCrossSlotKeys;
  }

  return result_flags;
}

[[nodiscard]] StatusOr<ScriptFlags> ExtractFlagsFromRegisterFunction(lua_State *lua) {
  if (!lua_istable(lua, -1)) {
    return {Status::NotOK, "Expects a valid flags argument to register_function, e.g. flags={ 'no-writes' }"};
  }
  auto flag_count = static_cast<int>(lua_objlen(lua, -1));
  std::vector<std::string> flags_content;
  flags_content.reserve(flag_count);
  for (int i = 1; i <= flag_count; ++i) {
    lua_pushnumber(lua, i);
    lua_gettable(lua, -2);
    if (!lua_isstring(lua, -1)) {
      return {Status::NotOK, "Expects a valid flags argument to register_function, e.g. flags={ 'no-writes' }"};
    }
    flags_content.emplace_back(lua_tostring(lua, -1));
    // pop up the current flag
    lua_pop(lua, 1);
  }
  // pop up the corresponding table of the flags parameter
  lua_pop(lua, 1);

  return GetFlagsFromStrings(flags_content);
}

void RemoveFromRegistry(lua_State *lua, const char *name) {
  lua_pushstring(lua, name);
  lua_pushnil(lua);
  lua_settable(lua, LUA_REGISTRYINDEX);
}

}  // namespace lua
