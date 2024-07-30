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

#include <cstdint>
#include <vector>

#include "redis_bitmap.h"

/* The greater is Pow, the smaller the error. */
constexpr uint32_t kHyperLogLogRegisterCountPow = 14;
/* The number of bits of the hash value used for determining the number of leading zeros. */
constexpr uint32_t kHyperLogLogHashBitCount = 50;
constexpr uint32_t kHyperLogLogRegisterCount = 1 << kHyperLogLogRegisterCountPow; /* With Pow=14, 16384 registers. */

constexpr size_t kHyperLogLogSegmentBytes = 768;
constexpr size_t kHyperLogLogSegmentRegisters = 1024;

constexpr uint32_t kHyperLogLogRegisterCountPerSegment = redis::kBitmapSegmentBits / 8;

constexpr uint32_t kHyperLogLogSegmentCount = kHyperLogLogRegisterCount / kHyperLogLogRegisterCountPerSegment;
constexpr uint32_t kHyperLogLogRegisterBits = 6;
constexpr uint32_t kHyperLogLogRegisterCountMask = kHyperLogLogRegisterCount - 1; /* Mask to index register. */
constexpr uint32_t kHyperLogLogRegisterMax = ((1 << kHyperLogLogRegisterBits) - 1);
/* constant for 0.5/ln(2) */
constexpr double kHyperLogLogAlpha = 0.721347520444481703680;
constexpr uint32_t kHyperLogLogRegisterBytesPerSegment =
    (kHyperLogLogRegisterCountPerSegment * kHyperLogLogRegisterBits) / 8;
constexpr uint32_t kHyperLogLogRegisterBytes = (kHyperLogLogRegisterCount * kHyperLogLogRegisterBits + 7) / 8;
// Copied from redis
// https://github.com/valkey-io/valkey/blob/14e09e981e0039edbf8c41a208a258c18624cbb7/src/hyperloglog.c#L472
constexpr uint32_t kHyperLogLogHashSeed = 0xadc83b19;

struct DenseHllResult {
  uint32_t register_index;
  uint8_t hll_trailing_zero;
};

DenseHllResult ExtractDenseHllResult(uint64_t hash);

uint8_t HllDenseGetRegister(const uint8_t *registers, uint32_t index);
void HllDenseSetRegister(uint8_t *registers, uint32_t index, uint8_t val);
uint64_t HllCount(const std::vector<uint8_t> &registers);
void HllMerge(std::vector<uint8_t> *registers_max, const std::vector<uint8_t> &registers);
