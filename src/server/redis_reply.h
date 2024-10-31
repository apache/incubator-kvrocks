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

#include <event2/buffer.h>

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "rocksdb/status.h"
#include "status.h"
#include "string_util.h"

#define CRLF "\r\n"                    // NOLINT
#define RESP_PREFIX_ERROR "-"          // NOLINT
#define RESP_PREFIX_SIMPLE_STRING "+"  // NOLINT

namespace redis {

enum class RESP { v2, v3 };

void Reply(evbuffer *output, const std::string &data);
std::string SimpleString(std::string_view data);

static const inline std::string RESP_OK = RESP_PREFIX_SIMPLE_STRING "OK" CRLF;

std::string Error(const Status &s);
std::string StatusToRedisErrorMsg(const Status &s);

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string Integer(T data) {
  return ":" + std::to_string(data) + CRLF;
}

inline std::string NilString(RESP ver) {
  if (ver == RESP::v3) {
    return "_" CRLF;
  }
  return "$-1" CRLF;
}

std::string BulkString(const std::string &data);

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string MultiLen(T len) {
  return "*" + std::to_string(len) + CRLF;
}

std::string Array(const std::vector<std::string> &list);
std::string ArrayOfBulkStrings(const std::vector<std::string> &elements);

std::string Bool(RESP ver, bool b);
inline std::string BigNumber(RESP ver, const std::string &n) {
  return ver == RESP::v3 ? "(" + n + CRLF : BulkString(n);
}
inline std::string Double(RESP ver, double d) {
  return ver == RESP::v3 ? "," + util::Float2String(d) + CRLF : BulkString(util::Float2String(d));
}
// ext is the extension of file to send, 'txt' for text file, 'md ' for markdown file
// at most 3 chars, padded with space
// if RESP is V2, treat verbatim string as blob string
// https://github.com/redis/redis/blob/7.2/src/networking.c#L1099
inline std::string VerbatimString(RESP ver, std::string ext, const std::string &data) {
  CHECK(ext.size() <= 3);
  size_t padded_len = 3 - ext.size();
  ext = ext + std::string(padded_len, ' ');
  return ver == RESP::v3 ? "=" + std::to_string(3 + 1 + data.size()) + CRLF + ext + ":" + data + CRLF
                         : BulkString(data);
}

inline std::string NilArray(RESP ver) { return ver == RESP::v3 ? "_" CRLF : "*-1" CRLF; }
std::string MultiBulkString(RESP ver, const std::vector<std::string> &values);
std::string MultiBulkString(RESP ver, const std::vector<std::string> &values,
                            const std::vector<rocksdb::Status> &statuses);

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string HeaderOfSet(RESP ver, T len) {
  return ver == RESP::v3 ? "~" + std::to_string(len) + CRLF : MultiLen(len);
}
std::string SetOfBulkStrings(RESP ver, const std::vector<std::string> &elems);
template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string HeaderOfMap(RESP ver, T len) {
  return ver == RESP::v3 ? "%" + std::to_string(len) + CRLF : MultiLen(len * 2);
}
std::string Map(RESP ver, const std::map<std::string, std::string> &map);
std::string MapOfBulkStrings(RESP ver, const std::vector<std::string> &elems);
template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string HeaderOfAttribute(T len) {
  return "|" + std::to_string(len) + CRLF;
}
template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string HeaderOfPush(RESP ver, T len) {
  return ver == RESP::v3 ? ">" + std::to_string(len) + CRLF : MultiLen(len);
}

}  // namespace redis
