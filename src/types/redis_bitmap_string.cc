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

#include "redis_bitmap_string.h"

#include <glog/logging.h>

#include <cstdint>

#include "common/bit_util.h"
#include "redis_string.h"
#include "server/redis_reply.h"
#include "storage/redis_metadata.h"
#include "type_util.h"

namespace redis {

rocksdb::Status BitmapString::GetBit(const std::string &raw_value, uint32_t bit_offset, bool *bit) {
  std::string_view string_value = std::string_view{raw_value}.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  uint32_t byte_index = bit_offset >> 3;
  if (byte_index < string_value.size()) {
    *bit = util::msb::GetBit(reinterpret_cast<const uint8_t *>(string_value.data()), bit_offset);
  } else {
    *bit = false;
  }
  return rocksdb::Status::OK();
}

rocksdb::Status BitmapString::SetBit(engine::Context &ctx, const Slice &ns_key, std::string *raw_value,
                                     uint32_t bit_offset, bool new_bit, bool *old_bit) {
  size_t header_offset = Metadata::GetOffsetAfterExpire((*raw_value)[0]);
  auto string_value = raw_value->substr(header_offset);
  uint32_t byte_index = bit_offset >> 3;
  if (byte_index >= string_value.size()) {  // expand the bitmap
    string_value.append(byte_index - string_value.size() + 1, 0);
  }
  auto *data_ptr = reinterpret_cast<uint8_t *>(string_value.data());
  *old_bit = util::msb::GetBit(data_ptr, bit_offset);
  util::msb::SetBitTo(data_ptr, bit_offset, new_bit);

  *raw_value = raw_value->substr(0, header_offset);
  raw_value->append(string_value);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisString);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) {
    return s;
  }
  s = batch->Put(metadata_cf_handle_, ns_key, *raw_value);
  if (!s.ok()) {
    return s;
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BitmapString::BitCount(const std::string &raw_value, int64_t start, int64_t stop, bool is_bit_index,
                                       uint32_t *cnt) {
  *cnt = 0;
  std::string_view string_value = std::string_view{raw_value}.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  auto strlen = static_cast<int64_t>(string_value.size());
  int64_t totlen = strlen;
  if (is_bit_index) totlen <<= 3;
  std::tie(start, stop) = NormalizeRange(start, stop, totlen);
  // Always return 0 if start is greater than stop after normalization.
  if (start > stop) return rocksdb::Status::OK();

  /* By default:
   * start means start byte in bitmap, stop means stop byte in bitmap.
   * When is_bit_index is true, start and stop means start bit and stop bit.
   * So it should be normalized bit range to byte range. */
  int64_t start_byte = start;
  int64_t stop_byte = stop;
  uint8_t first_byte_neg_mask = 0, last_byte_neg_mask = 0;
  std::tie(start_byte, stop_byte) =
      NormalizeToByteRangeWithPaddingMask(is_bit_index, start, stop, &first_byte_neg_mask, &last_byte_neg_mask);

  /* Precondition: end >= 0 && end < strlen, so the only condition where
   * zero can be returned is: start > stop. */
  int64_t bytes = stop_byte - start_byte + 1;
  *cnt = util::RawPopcount(reinterpret_cast<const uint8_t *>(string_value.data()) + start_byte, bytes);
  if (first_byte_neg_mask != 0 || last_byte_neg_mask != 0) {
    uint8_t firstlast[2] = {0, 0};
    if (first_byte_neg_mask != 0) firstlast[0] = string_value[start_byte] & first_byte_neg_mask;
    if (last_byte_neg_mask != 0) firstlast[1] = string_value[stop_byte] & last_byte_neg_mask;
    *cnt -= util::RawPopcount(firstlast, 2);
  }

  return rocksdb::Status::OK();
}

rocksdb::Status BitmapString::BitPos(const std::string &raw_value, bool bit, int64_t start, int64_t stop,
                                     bool stop_given, int64_t *pos, bool is_bit_index) {
  std::string_view string_value = std::string_view{raw_value}.substr(Metadata::GetOffsetAfterExpire(raw_value[0]));
  auto strlen = static_cast<int64_t>(string_value.size());
  /* Convert negative and out-of-bound indexes */

  int64_t length = is_bit_index ? strlen * 8 : strlen;
  std::tie(start, stop) = NormalizeRange(start, stop, length);

  if (start > stop) {
    *pos = -1;
    return rocksdb::Status::OK();
  }

  int64_t byte_start = is_bit_index ? start / 8 : start;
  int64_t byte_stop = is_bit_index ? stop / 8 : stop;
  int64_t bit_in_start_byte = is_bit_index ? start % 8 : 0;
  int64_t bit_in_stop_byte = is_bit_index ? stop % 8 : 7;
  int64_t bytes_cnt = byte_stop - byte_start + 1;

  auto bit_pos_in_byte_startstop = [](char byte, bool bit, uint32_t start, uint32_t stop) -> int {
    for (uint32_t i = start; i <= stop; i++) {
      if (util::msb::GetBitFromByte(byte, i) == bit) {
        return (int)i;
      }
    }
    return -1;
  };

  // if the bit start and bit end are in the same byte, we can process it manually
  if (is_bit_index && byte_start == byte_stop) {
    int res = bit_pos_in_byte_startstop(string_value[byte_start], bit, bit_in_start_byte, bit_in_stop_byte);
    if (res != -1) {
      *pos = res + byte_start * 8;
      return rocksdb::Status::OK();
    }
    *pos = -1;
    return rocksdb::Status::OK();
  }

  if (is_bit_index && bit_in_start_byte != 0) {
    // process first byte
    int res = bit_pos_in_byte_startstop(string_value[byte_start], bit, bit_in_start_byte, 7);
    if (res != -1) {
      *pos = res + byte_start * 8;
      return rocksdb::Status::OK();
    }

    byte_start++;
    bytes_cnt--;
  }

  *pos = util::msb::RawBitpos(reinterpret_cast<const uint8_t *>(string_value.data()) + byte_start, bytes_cnt, bit);

  if (is_bit_index && *pos != -1 && *pos != bytes_cnt * 8) {
    // if the pos is more than stop bit, then it is not in the range
    if (*pos > stop) {
      *pos = -1;
      return rocksdb::Status::OK();
    }
  }

  /* If we are looking for clear bits, and the user specified an exact
   * range with start-end, we tcan' consider the right of the range as
   * zero padded (as we do when no explicit end is given).
   *
   * So if redisBitpos() returns the first bit outside the range,
   * we return -1 to the caller, to mean, in the specified range there
   * is not a single "0" bit. */
  if (stop_given && bit == 0 && *pos == bytes_cnt * 8) {
    *pos = -1;
    return rocksdb::Status::OK();
  }
  if (*pos != -1) *pos += byte_start * 8; /* Adjust for the bytes we skipped. */

  return rocksdb::Status::OK();
}

std::pair<int64_t, int64_t> BitmapString::NormalizeRange(int64_t origin_start, int64_t origin_end, int64_t length) {
  if (origin_start < 0) origin_start = length + origin_start;
  if (origin_end < 0) origin_end = length + origin_end;
  if (origin_start < 0) origin_start = 0;
  if (origin_end < 0) origin_end = 0;
  if (origin_end >= length) origin_end = length - 1;
  return {origin_start, origin_end};
}

std::pair<int64_t, int64_t> BitmapString::NormalizeToByteRangeWithPaddingMask(bool is_bit, int64_t origin_start,
                                                                              int64_t origin_end,
                                                                              uint8_t *first_byte_neg_mask,
                                                                              uint8_t *last_byte_neg_mask) {
  DCHECK(origin_start <= origin_end);
  if (is_bit) {
    *first_byte_neg_mask = ~((1 << (8 - (origin_start & 7))) - 1) & 0xFF;
    *last_byte_neg_mask = (1 << (7 - (origin_end & 7))) - 1;
    origin_start >>= 3;
    origin_end >>= 3;
  }
  return {origin_start, origin_end};
}

rocksdb::Status BitmapString::Bitfield(engine::Context &ctx, const Slice &ns_key, std::string *raw_value,
                                       const std::vector<BitfieldOperation> &ops,
                                       std::vector<std::optional<BitfieldValue>> *rets) {
  auto header_offset = Metadata::GetOffsetAfterExpire((*raw_value)[0]);
  std::string string_value = raw_value->substr(header_offset);
  for (BitfieldOperation op : ops) {
    // [first_byte, last_byte)
    uint32_t first_byte = op.offset / 8;
    uint32_t last_byte = (op.offset + op.encoding.Bits() - 1) / 8 + 1;

    // expand string if need.
    if (string_value.size() < last_byte) {
      string_value.resize(last_byte);
    }

    ArrayBitfieldBitmap bitfield(first_byte);
    auto str = reinterpret_cast<const uint8_t *>(string_value.data() + first_byte);
    auto s = bitfield.Set(/*byte_offset=*/first_byte, /*bytes=*/last_byte - first_byte, /*src=*/str);
    if (!s.IsOK()) {
      return rocksdb::Status::IOError(s.Msg());
    }

    uint64_t unsigned_old_value = 0;
    if (op.encoding.IsSigned()) {
      unsigned_old_value = bitfield.GetSignedBitfield(op.offset, op.encoding.Bits()).GetValue();
    } else {
      unsigned_old_value = bitfield.GetUnsignedBitfield(op.offset, op.encoding.Bits()).GetValue();
    }

    uint64_t unsigned_new_value = 0;
    std::optional<BitfieldValue> &ret = rets->emplace_back();
    StatusOr<bool> bitfield_op = BitfieldOp(op, unsigned_old_value, &unsigned_new_value);
    if (!bitfield_op.IsOK()) {
      return rocksdb::Status::InvalidArgument(bitfield_op.Msg());
    }
    if (bitfield_op.GetValue()) {
      if (op.type != BitfieldOperation::Type::kGet) {
        // never failed.
        s = bitfield.SetBitfield(op.offset, op.encoding.Bits(), unsigned_new_value);
        CHECK(s.IsOK());
        auto dst = reinterpret_cast<uint8_t *>(string_value.data()) + first_byte;
        s = bitfield.Get(first_byte, last_byte - first_byte, dst);
        CHECK(s.IsOK());
      }

      if (op.type == BitfieldOperation::Type::kSet) {
        unsigned_new_value = unsigned_old_value;
      }

      ret = {op.encoding, unsigned_new_value};
    }
  }

  raw_value->resize(header_offset);
  raw_value->append(string_value);
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisString);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) {
    return s;
  }
  s = batch->Put(metadata_cf_handle_, ns_key, *raw_value);
  if (!s.ok()) {
    return s;
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status BitmapString::BitfieldReadOnly([[maybe_unused]] const Slice &ns_key, const std::string &raw_value,
                                               const std::vector<BitfieldOperation> &ops,
                                               std::vector<std::optional<BitfieldValue>> *rets) {
  std::string_view string_value = raw_value;
  string_value = string_value.substr(Metadata::GetOffsetAfterExpire(string_value[0]));

  for (BitfieldOperation op : ops) {
    if (op.type != BitfieldOperation::Type::kGet) {
      return rocksdb::Status::InvalidArgument("Write bitfield in read-only mode.");
    }

    uint32_t first_byte = op.offset / 8;
    uint32_t last_byte = (op.offset + op.encoding.Bits() - 1) / 8 + 1;

    ArrayBitfieldBitmap bitfield(first_byte);
    auto s = bitfield.Set(first_byte, last_byte - first_byte,
                          reinterpret_cast<const uint8_t *>(string_value.data() + first_byte));
    if (!s.IsOK()) {
      return rocksdb::Status::IOError(s.Msg());
    }

    if (op.encoding.IsSigned()) {
      int64_t value = bitfield.GetSignedBitfield(op.offset, op.encoding.Bits()).GetValue();
      rets->emplace_back(std::in_place, op.encoding, static_cast<uint64_t>(value));
    } else {
      uint64_t value = bitfield.GetUnsignedBitfield(op.offset, op.encoding.Bits()).GetValue();
      rets->emplace_back(std::in_place, op.encoding, value);
    }
  }

  return rocksdb::Status::OK();
}

}  // namespace redis
