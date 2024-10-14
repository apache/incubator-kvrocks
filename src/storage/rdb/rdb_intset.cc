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

#include "rdb_intset.h"

#include "vendor/endianconv.h"

constexpr const uint32_t IntSetHeaderSize = 8;

constexpr const uint32_t IntSetEncInt16 = (sizeof(int16_t));
constexpr const uint32_t IntSetEncInt32 = (sizeof(int32_t));
constexpr const uint32_t IntSetEncInt64 = (sizeof(int64_t));

Status IntSet::peekOk(size_t n) {
  if (pos_ + n > input_.size()) {
    return {Status::NotOK, "reach the end of intset"};
  }
  return Status::OK();
}

StatusOr<std::vector<std::string>> IntSet::Entries() {
  GET_OR_RET(peekOk(IntSetHeaderSize));
  uint32_t encoding = 0, len = 0;
  memcpy(&encoding, input_.data() + pos_, sizeof(uint32_t));
  pos_ += sizeof(uint32_t);
  memrev32ifbe(&encoding);
  memcpy(&len, input_.data() + pos_, sizeof(uint32_t));
  pos_ += sizeof(uint32_t);
  memrev32ifbe(&len);

  uint32_t record_size = encoding;
  if (record_size == 0) {
    return {Status::NotOK, "invalid intset encoding"};
  }
  if (IntSetHeaderSize + len * record_size != input_.size()) {
    return {Status::NotOK, "invalid intset length"};
  }

  std::vector<std::string> entries;
  for (uint32_t i = 0; i < len; i++) {
    switch (encoding) {
      case IntSetEncInt16: {
        uint16_t v = 0;
        memcpy(&v, input_.data() + pos_, sizeof(uint16_t));
        pos_ += sizeof(uint16_t);
        memrev16ifbe(&v);
        entries.emplace_back(std::to_string(v));
        break;
      }
      case IntSetEncInt32: {
        uint32_t v = 0;
        memcpy(&v, input_.data() + pos_, sizeof(uint32_t));
        pos_ += sizeof(uint32_t);
        memrev32ifbe(&v);
        entries.emplace_back(std::to_string(v));
        break;
      }
      case IntSetEncInt64: {
        uint64_t v = 0;
        memcpy(&v, input_.data() + pos_, sizeof(uint64_t));
        pos_ += sizeof(uint64_t);
        memrev64ifbe(&v);
        entries.emplace_back(std::to_string(v));
        break;
      }
      default:
        return {Status::NotOK, "invalid intset encoding"};
    }
  }
  return entries;
}
