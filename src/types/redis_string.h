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
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

struct StringPair {
  Slice key;
  Slice value;
};

enum class StringSetType { NONE, NX, XX };

struct StringSetArgs {
  // Expire time in mill seconds.
  uint64_t expire;
  StringSetType type;
  bool get;
  bool keep_ttl;
};

enum class StringLCSType { NONE, LEN, IDX };

struct StringLCSArgs {
  StringLCSType type;
  int64_t min_match_len;
};

struct StringLCSRange {
  uint32_t start;
  uint32_t end;
};

struct StringLCSMatchedRange {
  StringLCSRange a;
  StringLCSRange b;
  uint32_t match_len;

  StringLCSMatchedRange(StringLCSRange ra, StringLCSRange rb, uint32_t len) : a(ra), b(rb), match_len(len) {}
};

struct StringLCSIdxResult {
  // Matched ranges.
  std::vector<StringLCSMatchedRange> matches;
  // LCS length.
  uint32_t len;
};

using StringLCSResult = std::variant<std::string, uint32_t, StringLCSIdxResult>;

namespace redis {
class String : public Database {
 public:
  explicit String(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Append(engine::Context &ctx, const std::string &user_key, const std::string &value,
                         uint64_t *new_size);
  rocksdb::Status Get(engine::Context &ctx, const std::string &user_key, std::string *value);
  rocksdb::Status GetEx(engine::Context &ctx, const std::string &user_key, std::string *value,
                        std::optional<uint64_t> expire);
  rocksdb::Status GetSet(engine::Context &ctx, const std::string &user_key, const std::string &new_value,
                         std::optional<std::string> &old_value);
  rocksdb::Status GetDel(engine::Context &ctx, const std::string &user_key, std::string *value);
  rocksdb::Status Set(engine::Context &ctx, const std::string &user_key, const std::string &value);
  rocksdb::Status Set(engine::Context &ctx, const std::string &user_key, const std::string &value, StringSetArgs args,
                      std::optional<std::string> &ret);
  rocksdb::Status SetEX(engine::Context &ctx, const std::string &user_key, const std::string &value,
                        uint64_t expire_ms);
  rocksdb::Status SetNX(engine::Context &ctx, const std::string &user_key, const std::string &value, uint64_t expire_ms,
                        bool *flag);
  rocksdb::Status SetXX(engine::Context &ctx, const std::string &user_key, const std::string &value, uint64_t expire_ms,
                        bool *flag);
  rocksdb::Status SetRange(engine::Context &ctx, const std::string &user_key, size_t offset, const std::string &value,
                           uint64_t *new_size);
  rocksdb::Status IncrBy(engine::Context &ctx, const std::string &user_key, int64_t increment, int64_t *new_value);
  rocksdb::Status IncrByFloat(engine::Context &ctx, const std::string &user_key, double increment, double *new_value);
  std::vector<rocksdb::Status> MGet(engine::Context &ctx, const std::vector<Slice> &keys,
                                    std::vector<std::string> *values);
  rocksdb::Status MSet(engine::Context &ctx, const std::vector<StringPair> &pairs, uint64_t expire_ms);
  rocksdb::Status MSetNX(engine::Context &ctx, const std::vector<StringPair> &pairs, uint64_t expire_ms, bool *flag);
  rocksdb::Status CAS(engine::Context &ctx, const std::string &user_key, const std::string &old_value,
                      const std::string &new_value, uint64_t expire_ms, int *flag);
  rocksdb::Status CAD(engine::Context &ctx, const std::string &user_key, const std::string &value, int *flag);
  rocksdb::Status LCS(engine::Context &ctx, const std::string &user_key1, const std::string &user_key2,
                      StringLCSArgs args, StringLCSResult *rst);

 private:
  rocksdb::Status getValue(engine::Context &ctx, const std::string &ns_key, std::string *value);
  rocksdb::Status getValueAndExpire(engine::Context &ctx, const std::string &ns_key, std::string *value,
                                    uint64_t *expire_ms);
  std::vector<rocksdb::Status> getValues(engine::Context &ctx, const std::vector<Slice> &ns_keys,
                                         std::vector<std::string> *values);
  rocksdb::Status getRawValue(engine::Context &ctx, const std::string &ns_key, std::string *raw_value);
  std::vector<rocksdb::Status> getRawValues(engine::Context &ctx, const std::vector<Slice> &keys,
                                            std::vector<std::string> *raw_values);
  rocksdb::Status updateRawValue(engine::Context &ctx, const std::string &ns_key, const std::string &raw_value);
};

}  // namespace redis
