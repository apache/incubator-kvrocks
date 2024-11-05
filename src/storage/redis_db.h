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

#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "cluster/cluster_defs.h"
#include "redis_metadata.h"
#include "storage.h"

namespace redis {

/// SORT_LENGTH_LIMIT limits the number of elements to be sorted
/// to avoid using too much memory and causing system crashes.
/// TODO: Expect to expand or eliminate SORT_LENGTH_LIMIT
/// through better mechanisms such as memory restriction logic.
constexpr uint64_t SORT_LENGTH_LIMIT = 512;

struct SortArgument {
  std::string sortby;                    // BY
  bool dontsort = false;                 // DONT SORT
  int offset = 0;                        // LIMIT OFFSET
  int count = -1;                        // LIMIT COUNT
  std::vector<std::string> getpatterns;  // GET
  bool desc = false;                     // ASC/DESC
  bool alpha = false;                    // ALPHA
  std::string storekey;                  // STORE
};

struct RedisSortObject {
  std::string obj;
  std::variant<double, std::string> v;

  /// SortCompare is a helper function that enables `RedisSortObject` to be sorted based on `SortArgument`.
  ///
  /// It can assist in implementing the third parameter `Compare comp` required by `std::sort`
  ///
  /// \param args The basis used to compare two RedisSortObjects.
  /// If `args.alpha` is false, `RedisSortObject.v` will be taken as double for comparison
  /// If `args.alpha` is true and `args.sortby` is not empty, `RedisSortObject.v` will be taken as string for comparison
  /// If `args.alpha` is true and `args.sortby` is empty, the comparison is by `RedisSortObject.obj`.
  ///
  /// \return If `desc` is false, returns true when `a < b`, otherwise returns true when `a > b`
  static bool SortCompare(const RedisSortObject &a, const RedisSortObject &b, const SortArgument &args);
};

/// Database is a wrapper of underlying storage engine, it provides
/// some common operations for redis commands.
class Database {
 public:
  static constexpr uint64_t RANDOM_KEY_SCAN_LIMIT = 60;

  explicit Database(engine::Storage *storage, std::string ns = "");
  /// Parsing metadata with type of `types` from bytes, the metadata is a base class of all metadata.
  /// When parsing, the bytes will be consumed.
  [[nodiscard]] rocksdb::Status ParseMetadata(RedisTypes types, Slice *bytes, Metadata *metadata);
  /// GetMetadata is a helper function to get metadata from the database. It will read the "raw metadata"
  /// from underlying storage, and then parse the raw metadata to the specified metadata type.
  ///
  /// \param options The read options, including whether uses a snapshot during reading the metadata.
  /// \param types The candidate types of the metadata.
  /// \param ns_key The key with namespace of the metadata.
  /// \param metadata The output metadata.
  [[nodiscard]] rocksdb::Status GetMetadata(engine::Context &ctx, RedisTypes types, const Slice &ns_key,
                                            Metadata *metadata);
  /// GetMetadata is a helper function to get metadata from the database. It will read the "raw metadata"
  /// from underlying storage, and then parse the raw metadata to the specified metadata type.
  ///
  /// Compared with the above function, this function will also return the rest of the bytes
  /// after parsing the metadata.
  ///
  /// \param options The read options, including whether uses a snapshot during reading the metadata.
  /// \param types The candidate types of the metadata.
  /// \param ns_key The key with namespace of the metadata.
  /// \param raw_value Holding the raw metadata.
  /// \param metadata The output metadata.
  /// \param rest The rest of the bytes after parsing the metadata.
  [[nodiscard]] rocksdb::Status GetMetadata(engine::Context &ctx, RedisTypes types, const Slice &ns_key,
                                            std::string *raw_value, Metadata *metadata, Slice *rest);
  /// GetRawMetadata is a helper function to get the "raw metadata" from the database without parsing
  /// it to the specified metadata type.
  ///
  /// \param options The read options, including whether uses a snapshot during reading the metadata.
  /// \param ns_key The key with namespace of the metadata.
  /// \param bytes The output raw metadata.
  [[nodiscard]] rocksdb::Status GetRawMetadata(engine::Context &ctx, const Slice &ns_key, std::string *bytes);
  [[nodiscard]] rocksdb::Status Expire(engine::Context &ctx, const Slice &user_key, uint64_t timestamp);
  [[nodiscard]] rocksdb::Status Del(engine::Context &ctx, const Slice &user_key);
  [[nodiscard]] rocksdb::Status MDel(engine::Context &ctx, const std::vector<Slice> &keys, uint64_t *deleted_cnt);
  [[nodiscard]] rocksdb::Status Exists(engine::Context &ctx, const std::vector<Slice> &keys, int *ret);
  [[nodiscard]] rocksdb::Status TTL(engine::Context &ctx, const Slice &user_key, int64_t *ttl);
  [[nodiscard]] rocksdb::Status GetExpireTime(engine::Context &ctx, const Slice &user_key, uint64_t *timestamp);
  [[nodiscard]] rocksdb::Status Type(engine::Context &ctx, const Slice &key, RedisType *type);
  [[nodiscard]] rocksdb::Status Dump(engine::Context &ctx, const Slice &user_key, std::vector<std::string> *infos);
  [[nodiscard]] rocksdb::Status FlushDB(engine::Context &ctx);
  [[nodiscard]] rocksdb::Status FlushAll(engine::Context &ctx);
  [[nodiscard]] rocksdb::Status GetKeyNumStats(engine::Context &ctx, const std::string &prefix, KeyNumStats *stats);
  [[nodiscard]] rocksdb::Status Keys(engine::Context &ctx, const std::string &prefix, const std::string &suffix_glob,
                                     std::vector<std::string> *keys = nullptr, KeyNumStats *stats = nullptr);
  [[nodiscard]] rocksdb::Status Scan(engine::Context &ctx, const std::string &cursor, uint64_t limit,
                                     const std::string &prefix, const std::string &suffix_glob,
                                     std::vector<std::string> *keys, std::string *end_cursor = nullptr,
                                     RedisType type = kRedisNone);
  [[nodiscard]] rocksdb::Status RandomKey(engine::Context &ctx, const std::string &cursor, std::string *key);
  std::string AppendNamespacePrefix(const Slice &user_key);
  [[nodiscard]] rocksdb::Status ClearKeysOfSlotRange(engine::Context &ctx, const rocksdb::Slice &ns,
                                                     const SlotRange &slot_range);
  [[nodiscard]] rocksdb::Status KeyExist(engine::Context &ctx, const std::string &key);

  // Copy <key,value> to <new_key,value> (already an internal key)
  enum class CopyResult { KEY_NOT_EXIST, KEY_ALREADY_EXIST, DONE };
  [[nodiscard]] rocksdb::Status Copy(engine::Context &ctx, const std::string &key, const std::string &new_key, bool nx,
                                     bool delete_old, CopyResult *res);
  enum class SortResult { UNKNOWN_TYPE, DOUBLE_CONVERT_ERROR, LIMIT_EXCEEDED, DONE };
  /// Sort sorts keys of the specified type according to SortArgument
  ///
  /// \param type is the type of sort key, which must be LIST, SET or ZSET
  /// \param key is to be sorted
  /// \param args provide the parameters to sort by
  /// \param elems contain the sorted results
  /// \param res represents the sorted result type.
  /// When status is not ok, `res` should not been checked, otherwise it should be checked whether `res` is `DONE`
  [[nodiscard]] rocksdb::Status Sort(engine::Context &ctx, RedisType type, const std::string &key,
                                     const SortArgument &args, std::vector<std::optional<std::string>> *elems,
                                     SortResult *res);

 protected:
  engine::Storage *storage_;
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_;
  std::string namespace_;

 private:
  // Already internal keys
  [[nodiscard]] rocksdb::Status existsInternal(engine::Context &ctx, const std::vector<std::string> &keys, int *ret);
  [[nodiscard]] rocksdb::Status typeInternal(engine::Context &ctx, const Slice &key, RedisType *type);

  /// lookupKeyByPattern is a helper function of `Sort` to support `GET` and `BY` fields.
  ///
  /// \param pattern can be the value of a `BY` or `GET` field
  /// \param subst is used to replace the "*" or "#" matched in the pattern string.
  /// \return  Returns the value associated to the key with a name obtained using the following rules:
  ///   1) The first occurrence of '*' in 'pattern' is substituted with 'subst'.
  ///   2) If 'pattern' matches the "->" string, everything on the left of
  ///      the arrow is treated as the name of a hash field, and the part on the
  ///      left as the key name containing a hash. The value of the specified
  ///      field is returned.
  ///   3) If 'pattern' equals "#", the function simply returns 'subst' itself so
  ///      that the SORT command can be used like: SORT key GET # to retrieve
  ///      the Set/List elements directly.
  std::optional<std::string> lookupKeyByPattern(engine::Context &ctx, const std::string &pattern,
                                                const std::string &subst);
};

class SubKeyScanner : public redis::Database {
 public:
  explicit SubKeyScanner(engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Scan(engine::Context &ctx, RedisType type, const Slice &user_key, const std::string &cursor,
                       uint64_t limit, const std::string &subkey_prefix, std::vector<std::string> *keys,
                       std::vector<std::string> *values = nullptr);
};

class WriteBatchLogData {
 public:
  WriteBatchLogData() = default;
  explicit WriteBatchLogData(RedisType type) : type_(type) {}
  explicit WriteBatchLogData(RedisType type, std::vector<std::string> &&args) : type_(type), args_(std::move(args)) {}

  RedisType GetRedisType() const;
  std::vector<std::string> *GetArguments();
  std::string Encode() const;
  Status Decode(const rocksdb::Slice &blob);

 private:
  RedisType type_ = kRedisNone;
  std::vector<std::string> args_;
};

}  // namespace redis
