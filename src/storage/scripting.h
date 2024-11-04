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

#include <string>
#include <vector>

#include "lua.hpp"
#include "server/redis_connection.h"
#include "status.h"

namespace engine {
struct Context;
}

inline constexpr const char REDIS_LUA_FUNC_SHA_PREFIX[] = "f_";
inline constexpr const char REDIS_LUA_FUNC_SHA_FLAGS[] = "f_{}_flags_";
inline constexpr const char REDIS_LUA_REGISTER_FUNC_PREFIX[] = "__redis_registered_";
inline constexpr const char REDIS_LUA_REGISTER_FUNC_FLAGS_PREFIX[] = "__redis_registered_flags_";
inline constexpr const char REDIS_FUNCTION_LIBNAME[] = "REDIS_FUNCTION_LIBNAME";
inline constexpr const char REDIS_FUNCTION_NEEDSTORE[] = "REDIS_FUNCTION_NEEDSTORE";
inline constexpr const char REDIS_FUNCTION_LIBRARIES[] = "REDIS_FUNCTION_LIBRARIES";
inline constexpr const char REGISTRY_SCRIPT_RUN_CTX_NAME[] = "SCRIPT_RUN_CTX";

namespace lua {

lua_State *CreateState();
void DestroyState(lua_State *lua);

void LoadFuncs(lua_State *lua);
void LoadLibraries(lua_State *lua);
void RemoveUnsupportedFunctions(lua_State *lua);
void EnableGlobalsProtection(lua_State *lua);

int RedisCallCommand(lua_State *lua);
int RedisPCallCommand(lua_State *lua);
int RedisGenericCommand(lua_State *lua, int raise_error);
int RedisSha1hexCommand(lua_State *lua);
int RedisStatusReplyCommand(lua_State *lua);
int RedisErrorReplyCommand(lua_State *lua);
int RedisLogCommand(lua_State *lua);
int RedisRegisterFunction(lua_State *lua);
int RedisSetResp(lua_State *lua);

Status CreateFunction(Server *srv, const std::string &body, std::string *sha, lua_State *lua, bool need_to_store);

Status EvalGenericCommand(redis::Connection *conn, const std::string &body_or_sha, const std::vector<std::string> &keys,
                          const std::vector<std::string> &argv, bool evalsha, std::string *output,
                          bool read_only = false);

bool ScriptExists(lua_State *lua, const std::string &sha);

Status FunctionLoad(redis::Connection *conn, const std::string &script, bool need_to_store, bool replace,
                    std::string *lib_name, bool read_only = false);
Status FunctionCall(redis::Connection *conn, const std::string &name, const std::vector<std::string> &keys,
                    const std::vector<std::string> &argv, std::string *output, bool read_only = false);
Status FunctionList(Server *srv, const redis::Connection *conn, const std::string &libname, bool with_code,
                    std::string *output);
Status FunctionListFunc(Server *srv, const redis::Connection *conn, const std::string &funcname, std::string *output);
Status FunctionListLib(Server *srv, const redis::Connection *conn, const std::string &libname, std::string *output);
Status FunctionDelete(engine::Context &ctx, Server *srv, const std::string &name);
bool FunctionIsLibExist(redis::Connection *conn, const std::string &libname, bool need_check_storage = true,
                        bool read_only = false);

const char *RedisProtocolToLuaType(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeInt(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBulk(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeStatus(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeError(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeAggregate(lua_State *lua, const char *reply, int atype);
const char *RedisProtocolToLuaTypeNull(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBool(lua_State *lua, const char *reply, int tf);
const char *RedisProtocolToLuaTypeDouble(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBigNumber(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeVerbatimString(lua_State *lua, const char *reply);

std::string ReplyToRedisReply(redis::Connection *conn, lua_State *lua);

void PushError(lua_State *lua, const char *err);
[[noreturn]] int RaiseError(lua_State *lua);

void SortArray(lua_State *lua);
void SetGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems);
void PushArray(lua_State *lua, const std::vector<std::string> &elems);

void SHA1Hex(char *digest, const char *script, size_t len);

int RedisMathRandom(lua_State *l);
int RedisMathRandomSeed(lua_State *l);

/// ScriptFlagType turn on/off constraints or indicate properties in Eval scripts and functions
///
/// Note: The default for Eval scripts are different than the default for functions(default is 0).
/// As soon as Redis sees the #! comment, it'll treat the script as if it declares flags, even if no flags are defined,
/// it still has a different set of defaults compared to a script without a #! line.
/// Another difference is that scripts without #! can run commands that access keys belonging to different cluster hash
/// slots, but ones with #! inherit the default flags, so they cannot.
enum ScriptFlagType : uint64_t {
  kScriptNoWrites = 1ULL << 0,            // "no-writes" flag
  kScriptAllowOom = 1ULL << 1,            // "allow-oom" flag
  kScriptAllowStale = 1ULL << 2,          // "allow-stale" flag
  kScriptNoCluster = 1ULL << 3,           // "no-cluster" flag
  kScriptAllowCrossSlotKeys = 1ULL << 4,  // "allow-cross-slot-keys" flag
};

/// ScriptFlags is composed of one or more ScriptFlagTypes combined by an OR operation
/// For example, ScriptFlags flags = kScriptNoWrites | kScriptNoCluster
using ScriptFlags = uint64_t;

[[nodiscard]] StatusOr<std::string> ExtractLibNameFromShebang(std::string_view shebang);
[[nodiscard]] StatusOr<ScriptFlags> ExtractFlagsFromShebang(std::string_view shebang);

/// GetFlagsFromStrings gets flags from flags_content and composites them together.
/// Each element in flags_content should correspond to a string form of ScriptFlagType
[[nodiscard]] StatusOr<ScriptFlags> GetFlagsFromStrings(const std::vector<std::string> &flags_content);

/// ExtractFlagsFromRegisterFunction extracts the flags from the redis.register_function
///
/// Note: When using it, you should make sure that
/// the top of the stack of lua is the flags parameter of redis.register_function.
/// The flags parameter in Lua is a table that stores strings.
/// After use, the original flags table on the top of the stack will be popped.
[[nodiscard]] StatusOr<ScriptFlags> ExtractFlagsFromRegisterFunction(lua_State *lua);

/// ScriptRunCtx is used to record context information during the running of Eval scripts and functions.
struct ScriptRunCtx {
  // ScriptFlags
  uint64_t flags = 0;
  // current_slot tracks the slot currently accessed by the script
  // and is used to detect whether there is cross-slot access
  // between multiple commands in a script or function.
  int current_slot = -1;
  // the current connection
  redis::Connection *conn = nullptr;
};

/// SaveOnRegistry saves user-defined data to lua REGISTRY
///
/// Note: Since lua_pushlightuserdata, you need to manage the life cycle of the data stored in the Registry yourself.
template <typename T>
void SaveOnRegistry(lua_State *lua, const char *name, T *ptr) {
  lua_pushstring(lua, name);
  if (ptr) {
    lua_pushlightuserdata(lua, ptr);
  } else {
    lua_pushnil(lua);
  }
  lua_settable(lua, LUA_REGISTRYINDEX);
}

template <typename T>
T *GetFromRegistry(lua_State *lua, const char *name) {
  lua_pushstring(lua, name);
  lua_gettable(lua, LUA_REGISTRYINDEX);

  if (lua_isnil(lua, -1)) {
    // pops the value
    lua_pop(lua, 1);
    return nullptr;
  }

  // must be light user data
  CHECK(lua_islightuserdata(lua, -1));
  auto *ptr = static_cast<T *>(lua_touserdata(lua, -1));

  CHECK_NOTNULL(ptr);

  // pops the value
  lua_pop(lua, 1);

  return ptr;
}

void RemoveFromRegistry(lua_State *lua, const char *name);

}  // namespace lua
