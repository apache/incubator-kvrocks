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

/*
 * ListPack implements the reading of list-pack binary format
 * according to: https://github.com/antirez/listpack/blob/master/listpack.md.
 *
 * A list-pack is encoded into a single linear chunk of memory.
 * It has a fixed length header of six bytes which contains the total bytes and number of elements:
 *
 *     <total-bytes> <num-elements> <element-1> ... <element-N> <end-byte>
 *          |              |
 *        4byte          2byte
 * Each element is encoded as follows:
 *
 *     <encoding-type><element-data><element-total-bytes>
 *
 * encoding-type is used to indicate what type of data is stored in the element-data:
 *   - [0xxxxxxx] represents 7bit unsigned int
 *   - [10xxxxxx] represents 6bit string, the first byte is the length of the string
 *   - [110xxxxx] represents 13bit signed integer, the first byte is the sign bit
 *   - [1110|xxxx yyyyyyyy] represents 12bit string which can up to 4095 bytes
 *   - [1111|0000] represents 32bit string and the next 4 bytes are the length of the string
 *   - [1111|0001] represents 16 bits signed integer
 *   - [1111|0010] represents 24 bits signed integer
 *   - [1111|0011] represents 32 bits signed integer
 *   - [1111|0100] represents 64 bits signed integer
 *   - [1111|0101] not used for now
 *   - [1111|1111] represents the end of the list-pack
 *
 * element-data was determined by the encoding-type and element-total-bytes is the total bytes of the element.
 * The total bytes of the element is used to traverse the list-pack from the end to the beginning.
 */
#include "rdb_listpack.h"

constexpr const int ListPack7BitUIntMask = 0x80;
constexpr const int ListPack7BitUInt = 0;
// skip 1byte for encoding type and 1byte for element length
constexpr const int ListPack7BitIntEntrySize = 2;

constexpr const int ListPack6BitStringMask = 0xC0;
constexpr const int ListPack6BitString = 0x80;

constexpr const int ListPack13BitIntMask = 0xE0;
constexpr const int ListPack13BitInt = 0xC0;
// skip 2byte for encoding type and 1byte for element length
constexpr const int ListPack13BitIntEntrySize = 3;

constexpr const int ListPack12BitStringMask = 0xF0;
constexpr const int ListPack12BitString = 0xE0;

constexpr const int ListPack16BitIntMask = 0xFF;
constexpr const int ListPack16BitInt = 0xF1;
// skip 3byte for encoding type and 1byte for element length
constexpr const int ListPack16BitIntEntrySize = 4;

constexpr const int ListPack24BitIntMask = 0xFF;
constexpr const int ListPack24BitInt = 0xF2;
// skip 4byte for encoding type and 1byte for element length
constexpr const int ListPack24BitIntEntrySize = 5;

constexpr const int ListPack32BitIntMask = 0xFF;
constexpr const int ListPack32BitInt = 0xF3;
// skip 5byte for encoding type and 1byte for element length
constexpr const int ListPack32BitIntEntrySize = 6;

constexpr const int ListPack64BitIntMask = 0xFF;
constexpr const int ListPack64BitInt = 0xF4;
// skip 9byte for encoding type and 1byte for element length
constexpr const int ListPack64BitIntEntrySize = 10;

constexpr const int ListPack32BitStringMask = 0xFF;
constexpr const int ListPack32BitString = 0xF0;

constexpr const int ListPackEOF = 0xFF;

StatusOr<std::vector<std::string>> ListPack::Entries() {
  auto len = GET_OR_RET(Length());
  std::vector<std::string> elements;
  while (len-- != 0) {
    auto element = GET_OR_RET(Next());
    elements.emplace_back(element);
  }
  return elements;
}

StatusOr<uint32_t> ListPack::Length() {
  // list pack header size is 6 bytes which contains the total bytes and number of elements
  constexpr const int listPackHeaderSize = 6;
  if (input_.size() < listPackHeaderSize) {
    return {Status::NotOK, "invalid listpack length"};
  }

  auto data = reinterpret_cast<const uint8_t*>(
      input_.data());  // to avoid the sign extension in static_cast<uint32_t>(input_[n]))
  // total bytes and number of elements are encoded in little endian
  uint32_t total_bytes = (static_cast<uint32_t>(data[0])) | (static_cast<uint32_t>(data[1]) << 8) |
                         (static_cast<uint32_t>(data[2]) << 16) | (static_cast<uint32_t>(data[3]) << 24);
  uint32_t len = (static_cast<uint32_t>(data[4])) | (static_cast<uint32_t>(data[5]) << 8);
  pos_ += listPackHeaderSize;

  if (total_bytes != input_.size()) {
    return {Status::NotOK, "invalid listpack length"};
  }
  return len;
}

// encodeBackLen returns the number of bytes required to encode the length of the element.
uint32_t ListPack::encodeBackLen(uint32_t len) {
  if (len <= 127) {
    return 1;
  } else if (len < 16383) {
    return 2;
  } else if (len < 2097151) {
    return 3;
  } else if (len < 268435455) {
    return 4;
  } else {
    return 5;
  }
}

Status ListPack::peekOK(size_t n) {
  if (pos_ + n > input_.size()) {
    return {Status::NotOK, "reach the end of listpack"};
  }
  return Status::OK();
}

StatusOr<std::string> ListPack::Next() {
  GET_OR_RET(peekOK(1));

  uint32_t value_len = 0;
  uint64_t int_value = 0;
  std::string value;
  // For integer type, need to convert the byte to the uint8_t type to avoid the sign extension.
  auto data = reinterpret_cast<const uint8_t*>(input_.data());
  auto c = data[pos_];
  if ((c & ListPack7BitUIntMask) == ListPack7BitUInt) {  // 7bit unsigned int
    GET_OR_RET(peekOK(2));
    int_value = c & 0x7F;
    value = std::to_string(int_value);
    pos_ += ListPack7BitIntEntrySize;
  } else if ((c & ListPack6BitStringMask) == ListPack6BitString) {  // 6bit string
    value_len = (c & 0x3F);
    // skip the encoding type byte
    pos_ += 1;
    GET_OR_RET(peekOK(value_len));
    value = input_.substr(pos_, value_len);
    // skip the value bytes and the length of the element
    pos_ += value_len + encodeBackLen(value_len + 1);
  } else if ((c & ListPack13BitIntMask) == ListPack13BitInt) {  // 13bit int
    GET_OR_RET(peekOK(3));
    int_value = ((c & 0x1F) << 8) | data[pos_ + 1];
    value = std::to_string(int_value);
    pos_ += ListPack13BitIntEntrySize;
  } else if ((c & ListPack16BitIntMask) == ListPack16BitInt) {  // 16bit int
    GET_OR_RET(peekOK(4));
    int_value = (static_cast<uint64_t>(data[pos_ + 1])) | (static_cast<uint64_t>(data[pos_ + 2]) << 8);
    value = std::to_string(int_value);
    pos_ += ListPack16BitIntEntrySize;
  } else if ((c & ListPack24BitIntMask) == ListPack24BitInt) {  // 24bit int
    GET_OR_RET(peekOK(5));
    int_value = (static_cast<uint64_t>(data[pos_ + 1])) | (static_cast<uint64_t>(data[pos_ + 2]) << 8) |
                (static_cast<uint64_t>(data[pos_ + 3]) << 16);
    value = std::to_string(int_value);
    pos_ += ListPack24BitIntEntrySize;
  } else if ((c & ListPack32BitIntMask) == ListPack32BitInt) {  // 32bit int
    GET_OR_RET(peekOK(6));
    int_value = (static_cast<uint64_t>(data[pos_ + 1])) | (static_cast<uint64_t>(data[pos_ + 2]) << 8) |
                (static_cast<uint64_t>(data[pos_ + 3]) << 16) | (static_cast<uint64_t>(data[pos_ + 4]) << 24);
    value = std::to_string(int_value);
    pos_ += ListPack32BitIntEntrySize;
  } else if ((c & ListPack64BitIntMask) == ListPack64BitInt) {  // 64bit int
    GET_OR_RET(peekOK(10));
    int_value = (static_cast<uint64_t>(data[pos_ + 1])) | (static_cast<uint64_t>(data[pos_ + 2]) << 8) |
                (static_cast<uint64_t>(data[pos_ + 3]) << 16) | (static_cast<uint64_t>(data[pos_ + 4]) << 24) |
                (static_cast<uint64_t>(data[pos_ + 5]) << 32) | (static_cast<uint64_t>(data[pos_ + 6]) << 40) |
                (static_cast<uint64_t>(data[pos_ + 7]) << 48) | (static_cast<uint64_t>(data[pos_ + 8]) << 56);
    value = std::to_string(int_value);
    pos_ += ListPack64BitIntEntrySize;
  } else if ((c & ListPack12BitStringMask) == ListPack12BitString) {  // 12bit string
    GET_OR_RET(peekOK(2));
    value_len = ((data[pos_] & 0xF) << 8) | data[pos_ + 1];
    // skip 2byte encoding type
    pos_ += 2;
    GET_OR_RET(peekOK(value_len));
    value = input_.substr(pos_, value_len);
    // skip the value bytes and the length of the element
    pos_ += value_len + encodeBackLen(value_len + 2);
  } else if ((c & ListPack32BitStringMask) == ListPack32BitString) {  // 32bit string
    GET_OR_RET(peekOK(5));
    value_len = (static_cast<uint32_t>(data[pos_ + 1])) | (static_cast<uint32_t>(data[pos_ + 2]) << 8) |
                (static_cast<uint32_t>(data[pos_ + 3]) << 16) | (static_cast<uint32_t>(data[pos_ + 4]) << 24);
    // skip 5byte encoding type
    pos_ += 5;
    GET_OR_RET(peekOK(value_len));
    value = input_.substr(pos_, value_len);
    // skip the value bytes and the length of the element
    pos_ += value_len + encodeBackLen(value_len + 5);
  } else if (c == ListPackEOF) {
    ++pos_;
  } else {
    return {Status::NotOK, "invalid listpack entry"};
  }
  return value;
}
