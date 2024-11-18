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

#include "rdb_ziplist.h"

#include <cassert>

#include "vendor/endianconv.h"

constexpr const uint8_t ZIP_STR_MASK = 0xC0;
constexpr const uint8_t ZIP_STR_06B = (0 << 6);
constexpr const uint8_t ZIP_STR_14B = (1 << 6);
constexpr const uint8_t ZIP_STR_32B = (2 << 6);
constexpr const uint8_t ZIP_INT_16B = (0xC0 | 0 << 4);
constexpr const uint8_t ZIP_INT_32B = (0xC0 | 1 << 4);
constexpr const uint8_t ZIP_INT_64B = (0xC0 | 2 << 4);
constexpr const uint8_t ZIP_INT_24B = (0xC0 | 3 << 4);
constexpr const uint8_t ZIP_INT_8B = 0xFE;

constexpr const uint8_t ZIP_INT_IMM_MIN = 0xF1; /* 11110001 */
constexpr const uint8_t ZIP_INT_IMM_MAX = 0xFD; /* 11111101 */

StatusOr<std::string> ZipList::Next() {
  auto prev_entry_encoded_size = getEncodedLengthSize(pre_entry_len_);
  pos_ += prev_entry_encoded_size;
  GET_OR_RET(peekOK(1));
  auto encoding = static_cast<uint8_t>(input_[pos_]);
  if (encoding < ZIP_STR_MASK) {
    encoding &= ZIP_STR_MASK;
  }

  uint32_t len = 0, len_bytes = 0;
  std::string value;
  if ((encoding) < ZIP_STR_MASK) {
    // For integer type, needs to convert to uint8_t* to avoid signed extension
    auto data = reinterpret_cast<const uint8_t *>(input_.data());
    if ((encoding) == ZIP_STR_06B) {
      len_bytes = 1;
      len = data[pos_] & 0x3F;
    } else if ((encoding) == ZIP_STR_14B) {
      GET_OR_RET(peekOK(2));
      len_bytes = 2;
      len = ((static_cast<uint32_t>(data[pos_]) & 0x3F) << 8) | static_cast<uint32_t>(data[pos_ + 1]);
    } else if ((encoding) == ZIP_STR_32B) {
      GET_OR_RET(peekOK(5));
      len_bytes = 5;
      len = (static_cast<uint32_t>(data[pos_ + 1]) << 24) | (static_cast<uint32_t>(data[pos_ + 2]) << 16) |
            (static_cast<uint32_t>(data[pos_ + 3]) << 8) | static_cast<uint32_t>(data[pos_ + 4]);
    } else {
      return {Status::NotOK, "invalid ziplist encoding"};
    }
    pos_ += len_bytes;
    GET_OR_RET(peekOK(len));
    value = input_.substr(pos_, len);
    pos_ += len;
    setPreEntryLen(len_bytes + len + prev_entry_encoded_size);
  } else {
    GET_OR_RET(peekOK(1));
    pos_ += 1 /* the number bytes of length*/;
    if ((encoding) == ZIP_INT_8B) {
      GET_OR_RET(peekOK(1));
      setPreEntryLen(2);  // 1byte for encoding and 1byte for the prev entry length
      return std::to_string(input_[pos_++]);
    } else if ((encoding) == ZIP_INT_16B) {
      GET_OR_RET(peekOK(2));
      int16_t i16 = 0;
      memcpy(&i16, input_.data() + pos_, sizeof(int16_t));
      memrev16ifbe(&i16);
      setPreEntryLen(3);  // 2byte for encoding and 1byte for the prev entry length
      pos_ += sizeof(int16_t);
      return std::to_string(i16);
    } else if ((encoding) == ZIP_INT_24B) {
      GET_OR_RET(peekOK(3));
      int32_t i32 = 0;
      memcpy(reinterpret_cast<uint8_t *>(&i32) + 1, input_.data() + pos_, sizeof(int32_t) - 1);
      memrev32ifbe(&i32);
      i32 >>= 8;
      setPreEntryLen(4);  // 3byte for encoding and 1byte for the prev entry length
      pos_ += sizeof(int32_t) - 1;
      return std::to_string(i32);
    } else if ((encoding) == ZIP_INT_32B) {
      GET_OR_RET(peekOK(4));
      int32_t i32 = 0;
      memcpy(&i32, input_.data() + pos_, sizeof(int32_t));
      memrev32ifbe(&i32);
      setPreEntryLen(5);  // 4byte for encoding and 1byte for the prev entry length
      pos_ += sizeof(int32_t);
      return std::to_string(i32);
    } else if ((encoding) == ZIP_INT_64B) {
      GET_OR_RET(peekOK(8));
      int64_t i64 = 0;
      memcpy(&i64, input_.data() + pos_, sizeof(int64_t));
      memrev64ifbe(&i64);
      setPreEntryLen(9);  // 8byte for encoding and 1byte for the prev entry length
      pos_ += sizeof(int64_t);
      return std::to_string(i64);
    } else if (encoding >= ZIP_INT_IMM_MIN && encoding <= ZIP_INT_IMM_MAX) {
      setPreEntryLen(1);  // 8byte for encoding and 1byte for the prev entry length
      return std::to_string((encoding & 0x0F) - 1);
    } else {
      return {Status::NotOK, "invalid ziplist encoding"};
    }
  }
  return value;
}

StatusOr<std::vector<std::string>> ZipList::Entries() {
  GET_OR_RET(peekOK(zlHeaderSize));
  // ignore 8 bytes of total bytes and tail of zip list
  auto zl_len = intrev16ifbe(*reinterpret_cast<const uint16_t *>(input_.data() + 8));
  pos_ += zlHeaderSize;

  std::vector<std::string> entries;
  for (uint16_t i = 0; i < zl_len; i++) {
    GET_OR_RET(peekOK(1));
    if (static_cast<uint8_t>(input_[pos_]) == zlEnd) {
      break;
    }
    auto entry = GET_OR_RET(Next());
    entries.emplace_back(entry);
  }
  if (zl_len != entries.size()) {
    return {Status::NotOK, "invalid ziplist length"};
  }
  return entries;
}

Status ZipList::peekOK(size_t n) {
  if (pos_ + n > input_.size()) {
    return {Status::NotOK, "reach the end of ziplist"};
  }
  return Status::OK();
}

uint32_t ZipList::getEncodedLengthSize(uint32_t len) { return len < ZipListBigLen ? 1 : 5; }

uint32_t ZipList::ZipStorePrevEntryLengthLarge(unsigned char *p, [[maybe_unused]] size_t zl_size, unsigned int len) {
  uint32_t u32 = 0;
  if (p != nullptr) {
    p[0] = ZipListBigLen;
    u32 = len;
    assert(zl_size >= 1 + sizeof(uint32_t) + zlHeaderSize);
    memcpy(p + 1, &u32, sizeof(u32));
    memrev32ifbe(p + 1);
  }
  return 1 + sizeof(uint32_t);
}

uint32_t ZipList::ZipStorePrevEntryLength(unsigned char *p, size_t zl_size, unsigned int len) {
  if (p == nullptr) {
    return (len < ZipListBigLen) ? 1 : sizeof(uint32_t) + 1;
  }
  if (len < ZipListBigLen) {
    p[0] = len;
    return 1;
  }
  return ZipStorePrevEntryLengthLarge(p, zl_size, len);
}

uint32_t ZipList::ZipStoreEntryEncoding(unsigned char *p, [[maybe_unused]] size_t zl_size, unsigned int rawlen) {
  unsigned char len = 1, buf[5];

  /* Although encoding is given it may not be set for strings,
   * so we determine it here using the raw length. */
  if (rawlen <= 0x3f) {
    if (!p) return len;
    buf[0] = ZIP_STR_06B | rawlen;
  } else if (rawlen <= 0x3fff) {
    len += 1;
    if (!p) return len;
    buf[0] = ZIP_STR_14B | ((rawlen >> 8) & 0x3f);
    buf[1] = rawlen & 0xff;
  } else {
    len += 4;
    if (!p) return len;
    buf[0] = ZIP_STR_32B;
    buf[1] = (rawlen >> 24) & 0xff;
    buf[2] = (rawlen >> 16) & 0xff;
    buf[3] = (rawlen >> 8) & 0xff;
    buf[4] = rawlen & 0xff;
  }
  assert(zl_size >= static_cast<size_t>(zlHeaderSize) + len);
  /* Store this length at p. */
  memcpy(p, buf, len);
  return len;
}

void ZipList::SetZipListBytes(unsigned char *zl, [[maybe_unused]] size_t zl_size, uint32_t value) {
  assert(zl_size >= sizeof(uint32_t));
  memcpy(zl, &value, sizeof(uint32_t));
}
void ZipList::SetZipListTailOffset(unsigned char *zl, [[maybe_unused]] size_t zl_size, uint32_t value) {
  assert(zl_size >= sizeof(uint32_t) * 2);
  memcpy(zl + sizeof(uint32_t), &value, sizeof(uint32_t));
}
void ZipList::SetZipListLength(unsigned char *zl, [[maybe_unused]] size_t zl_size, uint16_t value) {
  assert(zl_size >= sizeof(uint32_t) * 2 + sizeof(uint16_t));
  memcpy(zl + sizeof(uint32_t) * 2, &value, sizeof(uint16_t));
}

unsigned char *ZipList::GetZipListEntryHead(unsigned char *zl, [[maybe_unused]] size_t zl_size) {
  assert(zl_size >= zlHeaderSize);
  return ((zl) + zlHeaderSize);
}
