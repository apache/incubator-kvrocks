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

#include "redis_reply.h"

#include <map>
#include <numeric>

const std::map<Status::Code, std::string> redisErrorPrefixMapping = {
    {Status::RedisErrorNoPrefix, ""},          {Status::RedisNoProto, "NOPROTO"},
    {Status::RedisLoading, "LOADING"},         {Status::RedisMasterDown, "MASTERDOWN"},
    {Status::RedisNoScript, "NOSCRIPT"},       {Status::RedisNoAuth, "NOAUTH"},
    {Status::RedisWrongType, "WRONGTYPE"},     {Status::RedisReadOnly, "READONLY"},
    {Status::RedisExecAbort, "EXECABORT"},     {Status::RedisMoved, "MOVED"},
    {Status::RedisCrossSlot, "CROSSSLOT"},     {Status::RedisTryAgain, "TRYAGAIN"},
    {Status::RedisClusterDown, "CLUSTERDOWN"}, {Status::RedisNoGroup, "NOGROUP"},
    {Status::RedisBusyGroup, "BUSYGROUP"}};

namespace redis {

void Reply(evbuffer *output, const std::string &data) { evbuffer_add(output, data.c_str(), data.length()); }

std::string SimpleString(std::string_view data) {
  std::string res;
  res.reserve(data.size() + 3);  // 1 for '+', 2 for CRLF
  res += RESP_PREFIX_SIMPLE_STRING;
  res += data;
  res += CRLF;
  return res;
}

std::string Error(const Status &s) { return RESP_PREFIX_ERROR + StatusToRedisErrorMsg(s) + CRLF; }

std::string StatusToRedisErrorMsg(const Status &s) {
  CHECK(!s.IsOK());
  std::string prefix = "ERR";
  if (auto it = redisErrorPrefixMapping.find(s.GetCode()); it != redisErrorPrefixMapping.end()) {
    prefix = it->second;
  }
  if (prefix.empty()) {
    return s.Msg();
  }
  return prefix + " " + s.Msg();
}

std::string BulkString(const std::string &data) { return "$" + std::to_string(data.length()) + CRLF + data + CRLF; }

std::string Array(const std::vector<std::string> &list) {
  size_t n = std::accumulate(list.begin(), list.end(), 0, [](size_t n, const std::string &s) { return n + s.size(); });
  std::string result = MultiLen(list.size());
  std::string::size_type final_size = result.size() + n;
  result.reserve(final_size);
  for (const auto &i : list) result += i;
  return result;
}

std::string ArrayOfBulkStrings(const std::vector<std::string> &elems) {
  std::string result = MultiLen(elems.size());
  for (const auto &elem : elems) {
    result += BulkString(elem);
  }
  return result;
}

std::string Bool(RESP ver, bool b) {
  if (ver == RESP::v3) {
    return b ? "#t" CRLF : "#f" CRLF;
  }
  return Integer(b ? 1 : 0);
}

std::string MultiBulkString(RESP ver, const std::vector<std::string> &values) {
  std::string result = MultiLen(values.size());
  for (const auto &value : values) {
    if (value.empty()) {
      result += NilString(ver);
    } else {
      result += BulkString(value);
    }
  }
  return result;
}

std::string MultiBulkString(RESP ver, const std::vector<std::string> &values,
                            const std::vector<rocksdb::Status> &statuses) {
  std::string result = MultiLen(values.size());
  for (size_t i = 0; i < values.size(); i++) {
    if (i < statuses.size() && !statuses[i].ok()) {
      result += NilString(ver);
    } else {
      result += BulkString(values[i]);
    }
  }
  return result;
}

std::string SetOfBulkStrings(RESP ver, const std::vector<std::string> &elems) {
  std::string result;
  result += HeaderOfSet(ver, elems.size());
  for (const auto &elem : elems) {
    result += BulkString(elem);
  }
  return result;
}

std::string MapOfBulkStrings(RESP ver, const std::vector<std::string> &elems) {
  CHECK(elems.size() % 2 == 0);

  std::string result;
  result += HeaderOfMap(ver, elems.size() / 2);
  for (const auto &elem : elems) {
    result += BulkString(elem);
  }
  return result;
}

std::string Map(RESP ver, const std::map<std::string, std::string> &map) {
  std::string result = HeaderOfMap(ver, map.size());
  for (const auto &pair : map) {
    result += pair.first;
    result += pair.second;
  }
  return result;
}

}  // namespace redis
