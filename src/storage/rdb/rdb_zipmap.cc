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

#include "rdb_zipmap.h"

#include "vendor/endianconv.h"

// ZipMapBigLen is the largest length of a zip map that can be encoded with.
// Need to traverse the entire zip map to get the length if it's over 254.
const uint8_t ZipMapBigLen = 254;
const uint8_t ZipMapEOF = 0xFF;

Status ZipMap::peekOK(size_t n) {
  if (pos_ + n > input_.size()) {
    return {Status::NotOK, "reach the end of zipmap"};
  }
  return Status::OK();
}

uint32_t ZipMap::getEncodedLengthSize(uint32_t len) { return len < ZipMapBigLen ? 1 : 5; }

StatusOr<uint32_t> ZipMap::decodeLength() {
  GET_OR_RET(peekOK(1));
  unsigned int len = static_cast<uint8_t>(input_[pos_++]);
  if (len == ZipMapBigLen) {
    GET_OR_RET(peekOK(4));
    memcpy(&len, input_.data() + pos_, sizeof(unsigned int));
    memrev32ifbe(&len);
    pos_ += 4;
  }
  return len;
}

StatusOr<std::pair<std::string, std::string>> ZipMap::Next() {
  // The element of zip map is a key-value pair and each element is encoded as:
  //
  // <len>"key"<len><free>"value"
  //
  // if len = 254, then the real length is stored in the next 4 bytes
  // if len = 255, then the zip map ends
  // <free> is the number of free unused bytes after the value, it's always 1byte.
  auto key_len = GET_OR_RET(decodeLength());
  GET_OR_RET(peekOK(key_len));
  auto key = input_.substr(pos_, key_len);
  pos_ += key_len;  // decodeLength already process as getEncodedLengthSize(key_len);
  auto val_len = GET_OR_RET(decodeLength());
  GET_OR_RET(peekOK(val_len + 1 /* free byte */));
  pos_ += 1;
  auto value = input_.substr(pos_, val_len);
  pos_ += val_len;  // + getEncodedLengthSize(val_len) + 1 /* free byte */;
  return std::make_pair(key, value);
}

// Entries will parse all key-value pairs from the zip map binary format:
//
// <zmlen><len>"key"<len><free>"value"<len>"key"<len><free>"value"...
//
// <zmlen> is the number of zip map entries, and it's always 1byte length.
// So you need to traverse the entire zip map to get the length if it's over 254.
//
// For more information, please infer: https://github.com/redis/redis/blob/unstable/src/zipmap.c
StatusOr<std::map<std::string, std::string>> ZipMap::Entries() {
  std::map<std::string, std::string> kvs;
  GET_OR_RET(peekOK(1));
  auto zm_len = static_cast<uint8_t>(input_[pos_++]);

  GET_OR_RET(peekOK(1));
  while (static_cast<uint8_t>(input_[pos_]) != ZipMapEOF) {
    auto kv = GET_OR_RET(Next());
    kvs.insert(kv);
    GET_OR_RET(peekOK(1));
  }
  if (zm_len < ZipMapBigLen && zm_len != kvs.size()) {
    return {Status::NotOK, "invalid zipmap length"};
  }
  return kvs;
}
