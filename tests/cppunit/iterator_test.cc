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

#include <cluster/redis_slot.h>
#include <fmt/format.h>
#include <storage/iterator.h>
#include <types/redis_bitmap.h>
#include <types/redis_bloom_chain.h>
#include <types/redis_json.h>
#include <types/redis_list.h>
#include <types/redis_set.h>
#include <types/redis_sortedint.h>
#include <types/redis_stream.h>
#include <types/redis_zset.h>

#include "test_base.h"
#include "types/redis_string.h"

class DBIteratorTest : public TestBase {
 protected:
  explicit DBIteratorTest() = default;
  ~DBIteratorTest() override = default;

  void SetUp() override {
    {  // string
      redis::String string(storage_.get(), "test_ns0");
      string.Set(*ctx_, "a", "1");
      string.Set(*ctx_, "b", "2");
      string.Set(*ctx_, "c", "3");
      // Make sure the key "c" is expired
      auto s = string.Expire(*ctx_, "c", 1);
      ASSERT_TRUE(s.ok());
      string.Set(*ctx_, "d", "4");
    }

    {  // hash
      uint64_t ret = 0;
      redis::Hash hash(storage_.get(), "test_ns1");
      hash.MSet(*ctx_, "hash-1", {{"f0", "v0"}, {"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}}, false, &ret);
    }

    {  // set
      uint64_t ret = 0;
      redis::Set set(storage_.get(), "test_ns2");
      set.Add(*ctx_, "set-1", {"e0", "e1", "e2"}, &ret);
    }

    {  // sorted set
      uint64_t ret = 0;
      redis::ZSet zset(storage_.get(), "test_ns3");
      auto mscores = std::vector<MemberScore>{{"z0", 0}, {"z1", 1}, {"z2", 2}};
      zset.Add(*ctx_, "zset-1", ZAddFlags(), &mscores, &ret);
    }

    {  // list
      uint64_t ret = 0;
      redis::List list(storage_.get(), "test_ns4");
      list.Push(*ctx_, "list-1", {"l0", "l1", "l2"}, false, &ret);
    }

    {  // stream
      redis::Stream stream(storage_.get(), "test_ns5");
      redis::StreamEntryID ret;
      redis::StreamAddOptions options;
      options.next_id_strategy = std::make_unique<redis::AutoGeneratedEntryID>();
      stream.Add(*ctx_, "stream-1", options, {"x0"}, &ret);
      stream.Add(*ctx_, "stream-1", options, {"x1"}, &ret);
      stream.Add(*ctx_, "stream-1", options, {"x2"}, &ret);
      // TODO(@git-hulk): add stream group after it's finished
    }

    {  // bitmap
      redis::Bitmap bitmap(storage_.get(), "test_ns6");
      bool ret = false;
      bitmap.SetBit(*ctx_, "bitmap-1", 0, true, &ret);
      bitmap.SetBit(*ctx_, "bitmap-1", 8 * 1024, true, &ret);
      bitmap.SetBit(*ctx_, "bitmap-1", 2 * 8 * 1024, true, &ret);
    }

    {  // json
      redis::Json json(storage_.get(), "test_ns7");
      json.Set(*ctx_, "json-1", "$", "{\"a\": 1, \"b\": 2}");
      json.Set(*ctx_, "json-2", "$", "{\"a\": 1, \"b\": 2}");
      json.Set(*ctx_, "json-3", "$", "{\"a\": 1, \"b\": 2}");
      json.Set(*ctx_, "json-4", "$", "{\"a\": 1, \"b\": 2}");
      auto s = json.Expire(*ctx_, "json-4", 1);
      ASSERT_TRUE(s.ok());
    }

    {
      // sorted integer
      redis::Sortedint sortedint(storage_.get(), "test_ns8");
      uint64_t ret = 0;
      sortedint.Add(*ctx_, "sortedint-1", {1, 2, 3}, &ret);
    }
  }
};

TEST_F(DBIteratorTest, AllKeys) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());
  std::vector<std::string> live_keys = {"a",        "b",        "d",      "hash-1", "set-1",  "zset-1",     "list-1",
                                        "stream-1", "bitmap-1", "json-1", "json-2", "json-3", "sortedint-1"};
  std::reverse(live_keys.begin(), live_keys.end());
  for (iter.Seek(); iter.Valid(); iter.Next()) {
    ASSERT_TRUE(!live_keys.empty());
    auto [_, user_key] = iter.UserKey();
    ASSERT_EQ(live_keys.back(), user_key.ToString());
    live_keys.pop_back();
  }
  ASSERT_TRUE(live_keys.empty());
}

TEST_F(DBIteratorTest, BasicString) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());

  std::vector<std::string> expected_keys = {"a", "b", "d"};
  std::reverse(expected_keys.begin(), expected_keys.end());
  auto prefix = ComposeNamespaceKey("test_ns0", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    if (expected_keys.empty()) {
      FAIL() << "Unexpected key: " << iter.Key().ToString();
    }
    ASSERT_EQ(kRedisString, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns0", ns.ToString());
    ASSERT_EQ(expected_keys.back(), key.ToString());
    expected_keys.pop_back();
    // Make sure there is no subkey iterator
    ASSERT_TRUE(!iter.GetSubKeyIterator());
  }
  // Make sure all keys are iterated except the expired one: "c"
  ASSERT_TRUE(expected_keys.empty());
}

TEST_F(DBIteratorTest, BasicHash) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());
  auto prefix = ComposeNamespaceKey("test_ns1", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    ASSERT_EQ(kRedisHash, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns1", ns.ToString());

    auto subkey_iter = iter.GetSubKeyIterator();
    ASSERT_TRUE(subkey_iter);
    std::vector<std::string> expected_keys = {"f0", "f1", "f2", "f3"};
    std::reverse(expected_keys.begin(), expected_keys.end());
    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      if (expected_keys.empty()) {
        FAIL() << "Unexpected key: " << subkey_iter->UserKey().ToString();
      }
      ASSERT_EQ(expected_keys.back(), subkey_iter->UserKey().ToString());
      expected_keys.pop_back();
    }
    ASSERT_TRUE(expected_keys.empty());
  }
}

TEST_F(DBIteratorTest, BasicSet) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());
  auto prefix = ComposeNamespaceKey("test_ns2", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    ASSERT_EQ(kRedisSet, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns2", ns.ToString());

    auto subkey_iter = iter.GetSubKeyIterator();
    ASSERT_TRUE(subkey_iter);
    std::vector<std::string> expected_keys = {"e0", "e1", "e2"};
    std::reverse(expected_keys.begin(), expected_keys.end());
    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      if (expected_keys.empty()) {
        FAIL() << "Unexpected key: " << subkey_iter->UserKey().ToString();
      }
      ASSERT_EQ(expected_keys.back(), subkey_iter->UserKey().ToString());
      expected_keys.pop_back();
    }
    ASSERT_TRUE(expected_keys.empty());
  }
}

TEST_F(DBIteratorTest, BasicZSet) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());
  auto prefix = ComposeNamespaceKey("test_ns3", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    ASSERT_EQ(kRedisZSet, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns3", ns.ToString());

    auto subkey_iter = iter.GetSubKeyIterator();
    ASSERT_TRUE(subkey_iter);
    std::vector<std::string> expected_members = {"z0", "z1", "z2"};
    std::reverse(expected_members.begin(), expected_members.end());
    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      if (expected_members.empty()) {
        FAIL() << "Unexpected key: " << subkey_iter->UserKey().ToString();
      }
      ASSERT_EQ(expected_members.back(), subkey_iter->UserKey().ToString());
      expected_members.pop_back();
    }
    ASSERT_TRUE(expected_members.empty());
  }
}

TEST_F(DBIteratorTest, BasicList) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());
  auto prefix = ComposeNamespaceKey("test_ns4", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    ASSERT_EQ(kRedisList, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns4", ns.ToString());

    auto subkey_iter = iter.GetSubKeyIterator();
    ASSERT_TRUE(subkey_iter);
    std::vector<std::string> expected_values = {"l0", "l1", "l2"};
    std::reverse(expected_values.begin(), expected_values.end());
    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      if (expected_values.empty()) {
        FAIL() << "Unexpected value: " << subkey_iter->Value().ToString();
      }
      ASSERT_EQ(expected_values.back(), subkey_iter->Value().ToString());
      expected_values.pop_back();
    }
    ASSERT_TRUE(expected_values.empty());
  }
}

TEST_F(DBIteratorTest, BasicStream) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());
  auto prefix = ComposeNamespaceKey("test_ns5", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    ASSERT_EQ(kRedisStream, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns5", ns.ToString());

    auto subkey_iter = iter.GetSubKeyIterator();
    ASSERT_TRUE(subkey_iter);
    std::vector<std::string> expected_values = {"x0", "x1", "x2"};
    std::reverse(expected_values.begin(), expected_values.end());
    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      if (expected_values.empty()) {
        FAIL() << "Unexpected value: " << subkey_iter->Value().ToString();
      }
      std::vector<std::string> elems;
      auto s = redis::DecodeRawStreamEntryValue(subkey_iter->Value().ToString(), &elems);
      ASSERT_TRUE(s.IsOK() && !elems.empty());
      ASSERT_EQ(expected_values.back(), elems[0]);
      expected_values.pop_back();
    }
    ASSERT_TRUE(expected_values.empty());
  }
}

TEST_F(DBIteratorTest, BasicBitmap) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());
  auto prefix = ComposeNamespaceKey("test_ns6", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    ASSERT_EQ(kRedisBitmap, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns6", ns.ToString());

    auto subkey_iter = iter.GetSubKeyIterator();
    ASSERT_TRUE(subkey_iter);
    std::vector<std::string> expected_values = {"\x1", "\x1", "\x1"};
    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      if (expected_values.empty()) {
        FAIL() << "Unexpected value: " << subkey_iter->Value().ToString();
      }
      ASSERT_EQ(expected_values.back(), subkey_iter->Value().ToString());
      expected_values.pop_back();
    }
    ASSERT_TRUE(expected_values.empty());
  }
}

TEST_F(DBIteratorTest, BasicJSON) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());

  std::vector<std::string> expected_keys = {"json-1", "json-2", "json-3"};
  std::reverse(expected_keys.begin(), expected_keys.end());
  auto prefix = ComposeNamespaceKey("test_ns7", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    if (expected_keys.empty()) {
      FAIL() << "Unexpected key: " << iter.Key().ToString();
    }
    ASSERT_EQ(kRedisJson, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns7", ns.ToString());
    ASSERT_EQ(expected_keys.back(), key.ToString());
    expected_keys.pop_back();
    // Make sure there is no subkey iterator
    ASSERT_TRUE(!iter.GetSubKeyIterator());
  }
  // Make sure all keys are iterated except the expired one: "json-4"
  ASSERT_TRUE(expected_keys.empty());
}

TEST_F(DBIteratorTest, BasicSortedInt) {
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions());

  auto prefix = ComposeNamespaceKey("test_ns8", "", storage_->IsSlotIdEncoded());
  for (iter.Seek(prefix); iter.Valid() && iter.Key().starts_with(prefix); iter.Next()) {
    ASSERT_EQ(kRedisSortedint, iter.Type());
    auto [ns, key] = iter.UserKey();
    ASSERT_EQ("test_ns8", ns.ToString());

    auto subkey_iter = iter.GetSubKeyIterator();
    ASSERT_TRUE(subkey_iter);
    std::vector<uint64_t> expected_keys = {1, 2, 3};
    std::reverse(expected_keys.begin(), expected_keys.end());
    for (subkey_iter->Seek(); subkey_iter->Valid(); subkey_iter->Next()) {
      auto value = DecodeFixed64(subkey_iter->UserKey().data());
      if (expected_keys.empty()) {
        FAIL() << "Unexpected value: " << value;
      }
      ASSERT_EQ(expected_keys.back(), value);
      expected_keys.pop_back();
    }
  }
}

class SlotIteratorTest : public TestBase {
 protected:
  explicit SlotIteratorTest() = default;
  ~SlotIteratorTest() override = default;
  void SetUp() override { storage_->GetConfig()->slot_id_encoded = true; }
};

TEST_F(SlotIteratorTest, LiveKeys) {
  redis::String string(storage_.get(), kDefaultNamespace);
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  std::vector<std::string> keys = {"{x}a", "{x}b", "{y}c", "{y}d", "{x}e"};
  for (const auto &key : keys) {
    string.Set(*ctx_, key, "1");
  }

  std::set<std::string> same_slot_keys;
  auto slot_id = GetSlotIdFromKey(keys[0]);
  for (const auto &key : keys) {
    if (GetSlotIdFromKey(key) == slot_id) {
      same_slot_keys.insert(key);
    }
  }
  engine::DBIterator iter(*ctx_, rocksdb::ReadOptions(), slot_id);
  int count = 0;
  for (iter.Seek(); iter.Valid(); iter.Next()) {
    auto [_, user_key] = iter.UserKey();
    ASSERT_EQ(slot_id, GetSlotIdFromKey(user_key.ToString()));
    count++;
  }
  ASSERT_EQ(count, same_slot_keys.size());

  engine::WALIterator wal_iter(storage_.get(), slot_id);
  count = 0;
  for (wal_iter.Seek(start_seq + 1); wal_iter.Valid(); wal_iter.Next()) {
    auto item = wal_iter.Item();
    if (item.type == engine::WALItem::Type::kTypePut) {
      auto [_, user_key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
      ASSERT_EQ(slot_id, GetSlotIdFromKey(user_key.ToString())) << user_key.ToString();
      count++;
    }
  }
  ASSERT_EQ(count, same_slot_keys.size());
}

class WALIteratorTest : public TestBase {
 protected:
  explicit WALIteratorTest() = default;
  ~WALIteratorTest() override = default;
  void SetUp() override {}
};

TEST_F(WALIteratorTest, BasicString) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  redis::String string(storage_.get(), "test_ns0");
  string.Set(*ctx_, "a", "1");
  string.MSet(*ctx_, {{"b", "2"}, {"c", "3"}}, 0);
  ASSERT_TRUE(string.Del(*ctx_, "b").ok());

  std::vector<std::string> put_keys, delete_keys;
  auto expected_put_keys = {"a", "b", "c"};
  auto expected_delete_keys = {"b"};

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
        ASSERT_EQ(ns.ToString(), "test_ns0");
        put_keys.emplace_back(key.ToString());
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisString);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
        ASSERT_EQ(ns.ToString(), "test_ns0");
        delete_keys.emplace_back(key.ToString());
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }
  ASSERT_EQ(expected_put_keys.size(), put_keys.size());
  ASSERT_EQ(expected_delete_keys.size(), delete_keys.size());
  ASSERT_TRUE(std::equal(expected_put_keys.begin(), expected_put_keys.end(), put_keys.begin()));
  ASSERT_TRUE(std::equal(expected_delete_keys.begin(), expected_delete_keys.end(), delete_keys.begin()));
}

TEST_F(WALIteratorTest, BasicHash) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  redis::Hash hash(storage_.get(), "test_ns1");
  uint64_t ret = 0;
  hash.MSet(*ctx_, "hash-1", {{"f0", "v0"}, {"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}}, false, &ret);
  uint64_t deleted_cnt = 0;
  hash.Delete(*ctx_, "hash-1", {"f0"}, &deleted_cnt);

  // Delete will put meta key again
  auto expected_put_keys = {"hash-1", "hash-1"};
  // Sub key will be putted in reverse order
  auto expected_put_fields = {"f3", "f2", "f1", "f0"};
  auto expected_delete_fields = {"f0"};
  std::vector<std::string> put_keys, put_fields, delete_fields;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
          InternalKey internal_key(item.key, storage_->IsSlotIdEncoded());
          put_fields.emplace_back(internal_key.GetSubKey().ToString());
        } else if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
          auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
          ASSERT_EQ(ns.ToString(), "test_ns1");
          put_keys.emplace_back(key.ToString());
        }
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisHash);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        InternalKey internal_key(item.key, storage_->IsSlotIdEncoded());
        delete_fields.emplace_back(internal_key.GetSubKey().ToString());
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }
  ASSERT_EQ(expected_put_keys.size(), put_keys.size());
  ASSERT_EQ(expected_put_fields.size(), put_fields.size());
  ASSERT_EQ(expected_delete_fields.size(), delete_fields.size());
  ASSERT_TRUE(std::equal(expected_put_keys.begin(), expected_put_keys.end(), put_keys.begin()));
  ASSERT_TRUE(std::equal(expected_put_fields.begin(), expected_put_fields.end(), put_fields.begin()));
  ASSERT_TRUE(std::equal(expected_delete_fields.begin(), expected_delete_fields.end(), delete_fields.begin()));
}

TEST_F(WALIteratorTest, BasicSet) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();

  uint64_t ret = 0;
  redis::Set set(storage_.get(), "test_ns2");
  set.Add(*ctx_, "set-1", {"e0", "e1", "e2"}, &ret);
  uint64_t removed_cnt = 0;
  set.Remove(*ctx_, "set-1", {"e0", "e1"}, &removed_cnt);

  auto expected_put_keys = {"set-1", "set-1"};
  auto expected_put_members = {"e0", "e1", "e2"};
  auto expected_delete_members = {"e0", "e1"};
  std::vector<std::string> put_keys, put_members, delete_members;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
          InternalKey internal_key(item.key, storage_->IsSlotIdEncoded());
          put_members.emplace_back(internal_key.GetSubKey().ToString());
        } else if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
          auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
          ASSERT_EQ(ns.ToString(), "test_ns2");
          put_keys.emplace_back(key.ToString());
        }
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisSet);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        InternalKey internal_key(item.key, storage_->IsSlotIdEncoded());
        delete_members.emplace_back(internal_key.GetSubKey().ToString());
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }

  ASSERT_EQ(expected_put_keys.size(), put_keys.size());
  ASSERT_EQ(expected_put_members.size(), put_members.size());
  ASSERT_EQ(expected_delete_members.size(), delete_members.size());
  ASSERT_TRUE(std::equal(expected_put_keys.begin(), expected_put_keys.end(), put_keys.begin()));
  ASSERT_TRUE(std::equal(expected_put_members.begin(), expected_put_members.end(), put_members.begin()));
  ASSERT_TRUE(std::equal(expected_delete_members.begin(), expected_delete_members.end(), delete_members.begin()));
}

TEST_F(WALIteratorTest, BasicZSet) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  uint64_t ret = 0;
  redis::ZSet zset(storage_.get(), "test_ns3");
  auto mscores = std::vector<MemberScore>{{"z0", 0}, {"z1", 1}, {"z2", 2}};
  zset.Add(*ctx_, "zset-1", ZAddFlags(), &mscores, &ret);
  uint64_t removed_cnt = 0;
  zset.Remove(*ctx_, "zset-1", {"z0"}, &removed_cnt);

  auto expected_put_keys = {"zset-1", "zset-1"};
  auto expected_put_members = {"z2", "z1", "z0"};
  // member and score
  int expected_delete_count = 2, delete_count = 0;
  std::vector<std::string> put_keys, put_members;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
          InternalKey internal_key(item.key, storage_->IsSlotIdEncoded());
          put_members.emplace_back(internal_key.GetSubKey().ToString());
        } else if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
          auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
          ASSERT_EQ(ns.ToString(), "test_ns3");
          put_keys.emplace_back(key.ToString());
        }
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisZSet);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        delete_count++;
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }

  ASSERT_EQ(expected_put_keys.size(), put_keys.size());
  ASSERT_EQ(expected_put_members.size(), put_members.size());
  ASSERT_EQ(expected_delete_count, delete_count);
  ASSERT_TRUE(std::equal(expected_put_keys.begin(), expected_put_keys.end(), put_keys.begin()));
  ASSERT_TRUE(std::equal(expected_put_members.begin(), expected_put_members.end(), put_members.begin()));
}

TEST_F(WALIteratorTest, BasicList) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  uint64_t ret = 0;
  redis::List list(storage_.get(), "test_ns4");
  list.Push(*ctx_, "list-1", {"l0", "l1", "l2", "l3", "l4"}, false, &ret);
  ASSERT_TRUE(list.Trim(*ctx_, "list-1", 2, 4).ok());

  auto expected_put_keys = {"list-1", "list-1"};
  auto expected_put_values = {"l0", "l1", "l2", "l3", "l4"};
  auto expected_delete_count = 2, delete_count = 0;
  std::vector<std::string> put_keys, put_values;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
          put_values.emplace_back(item.value);
        } else if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
          auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
          ASSERT_EQ(ns.ToString(), "test_ns4");
          put_keys.emplace_back(key.ToString());
        }
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisList);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        delete_count++;
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }

  ASSERT_EQ(expected_put_keys.size(), put_keys.size());
  ASSERT_EQ(expected_put_values.size(), put_values.size());
  ASSERT_EQ(expected_delete_count, delete_count);
  ASSERT_TRUE(std::equal(expected_put_keys.begin(), expected_put_keys.end(), put_keys.begin()));
  ASSERT_TRUE(std::equal(expected_put_values.begin(), expected_put_values.end(), put_values.begin()));
}

TEST_F(WALIteratorTest, BasicStream) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  redis::Stream stream(storage_.get(), "test_ns5");
  redis::StreamEntryID ret;
  redis::StreamAddOptions options;
  options.next_id_strategy = std::make_unique<redis::AutoGeneratedEntryID>();
  stream.Add(*ctx_, "stream-1", options, {"x0"}, &ret);
  stream.Add(*ctx_, "stream-1", options, {"x1"}, &ret);
  stream.Add(*ctx_, "stream-1", options, {"x2"}, &ret);
  uint64_t deleted = 0;
  ASSERT_TRUE(stream.DeleteEntries(*ctx_, "stream-1", {ret}, &deleted).ok());

  auto expected_put_keys = {"stream-1", "stream-1", "stream-1", "stream-1"};
  auto expected_put_values = {"x0", "x1", "x2"};
  int delete_count = 0;
  std::vector<std::string> put_keys, put_values;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::Stream)) {
          std::vector<std::string> elems;
          auto s = redis::DecodeRawStreamEntryValue(item.value, &elems);
          ASSERT_TRUE(s.IsOK() && !elems.empty());
          put_values.emplace_back(elems[0]);
        } else if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
          auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
          ASSERT_EQ(ns.ToString(), "test_ns5");
          put_keys.emplace_back(key.ToString());
        }
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisStream);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        delete_count++;
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }

  ASSERT_EQ(expected_put_keys.size(), put_keys.size());
  ASSERT_EQ(expected_put_values.size(), put_values.size());
  ASSERT_EQ(deleted, delete_count);
  ASSERT_TRUE(std::equal(expected_put_keys.begin(), expected_put_keys.end(), put_keys.begin()));
  ASSERT_TRUE(std::equal(expected_put_values.begin(), expected_put_values.end(), put_values.begin()));
}

TEST_F(WALIteratorTest, BasicBitmap) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();

  redis::Bitmap bitmap(storage_.get(), "test_ns6");
  bool ret = false;
  bitmap.SetBit(*ctx_, "bitmap-1", 0, true, &ret);
  bitmap.SetBit(*ctx_, "bitmap-1", 8 * 1024, true, &ret);
  bitmap.SetBit(*ctx_, "bitmap-1", 2 * 8 * 1024, true, &ret);

  auto expected_put_values = {"\x1", "\x1", "\x1"};
  std::vector<std::string> put_values;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
          put_values.emplace_back(item.value);
        }
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisBitmap);
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }
  ASSERT_EQ(expected_put_values.size(), put_values.size());
  ASSERT_TRUE(std::equal(expected_put_values.begin(), expected_put_values.end(), put_values.begin()));
}

TEST_F(WALIteratorTest, BasicJSON) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  redis::Json json(storage_.get(), "test_ns7");
  json.Set(*ctx_, "json-1", "$", "{\"a\": 1, \"b\": 2}");
  json.Set(*ctx_, "json-2", "$", "{\"a\": 1, \"b\": 2}");
  json.Set(*ctx_, "json-3", "$", "{\"a\": 1, \"b\": 2}");

  size_t result = 0;
  ASSERT_TRUE(json.Del(*ctx_, "json-3", "$", &result).ok());

  auto expected_put_keys = {"json-1", "json-2", "json-3"};
  auto expected_delete_keys = {"json-3"};
  std::vector<std::string> put_keys, delete_keys;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        ASSERT_EQ(item.column_family_id, static_cast<uint32_t>(ColumnFamilyID::Metadata));
        auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
        ASSERT_EQ(ns.ToString(), "test_ns7");
        put_keys.emplace_back(key.ToString());
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisJson);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        ASSERT_EQ(item.column_family_id, static_cast<uint32_t>(ColumnFamilyID::Metadata));
        auto [ns, key] = ExtractNamespaceKey(item.key, storage_->IsSlotIdEncoded());
        ASSERT_EQ(ns.ToString(), "test_ns7");
        delete_keys.emplace_back(key.ToString());
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }

  ASSERT_EQ(expected_put_keys.size(), put_keys.size());
  ASSERT_EQ(expected_delete_keys.size(), delete_keys.size());
  ASSERT_TRUE(std::equal(expected_put_keys.begin(), expected_put_keys.end(), put_keys.begin()));
  ASSERT_TRUE(std::equal(expected_delete_keys.begin(), expected_delete_keys.end(), delete_keys.begin()));
}

TEST_F(WALIteratorTest, BasicSortedInt) {
  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  redis::Sortedint sortedint(storage_.get(), "test_ns8");
  uint64_t ret = 0;
  sortedint.Add(*ctx_, "sortedint-1", {1, 2, 3}, &ret);
  uint64_t removed_cnt = 0;
  sortedint.Remove(*ctx_, "sortedint-1", {2}, &removed_cnt);

  std::vector<uint64_t> expected_values = {1, 2, 3}, put_values;
  std::vector<uint64_t> expected_delete_values = {2}, delete_values;

  engine::WALIterator iter(storage_.get());

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    auto item = iter.Item();
    switch (item.type) {
      case engine::WALItem::Type::kTypePut: {
        if (item.column_family_id == static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey)) {
          const InternalKey internal_key(item.key, storage_->IsSlotIdEncoded());
          auto value = DecodeFixed64(internal_key.GetSubKey().data());
          put_values.emplace_back(value);
        }
        break;
      }
      case engine::WALItem::Type::kTypeLogData: {
        redis::WriteBatchLogData log_data;
        ASSERT_TRUE(log_data.Decode(item.key).IsOK());
        ASSERT_EQ(log_data.GetRedisType(), kRedisSortedint);
        break;
      }
      case engine::WALItem::Type::kTypeDelete: {
        const InternalKey internal_key(item.key, storage_->IsSlotIdEncoded());
        auto value = DecodeFixed64(internal_key.GetSubKey().data());
        delete_values.emplace_back(value);
        break;
      }
      default:
        FAIL() << "Unexpected wal item type" << uint8_t(item.type);
    }
  }
  ASSERT_EQ(expected_values.size(), put_values.size());
  ASSERT_EQ(expected_delete_values.size(), delete_values.size());
  ASSERT_TRUE(std::equal(expected_values.begin(), expected_values.end(), put_values.begin()));
  ASSERT_TRUE(std::equal(expected_delete_values.begin(), expected_delete_values.end(), delete_values.begin()));
}

TEST_F(WALIteratorTest, NextSequence) {
  std::vector<rocksdb::SequenceNumber> expected_next_sequences;
  std::set<rocksdb::SequenceNumber> next_sequences_set;

  auto start_seq = storage_->GetDB()->GetLatestSequenceNumber();
  uint64_t ret = 0;
  redis::List list(storage_.get(), "test_ns2");
  list.Push(*ctx_, "list-1", {"l0", "l1", "l2", "l3", "l4"}, false, &ret);
  expected_next_sequences.emplace_back(storage_->GetDB()->GetLatestSequenceNumber() + 1);
  list.Push(*ctx_, "list-2", {"l0", "l1", "l2"}, false, &ret);
  expected_next_sequences.emplace_back(storage_->GetDB()->GetLatestSequenceNumber() + 1);
  ASSERT_TRUE(list.Trim(*ctx_, "list-1", 2, 4).ok());
  expected_next_sequences.emplace_back(storage_->GetDB()->GetLatestSequenceNumber() + 1);

  engine::WALIterator iter(storage_.get());

  ASSERT_EQ(iter.NextSequenceNumber(), 0);

  for (iter.Seek(start_seq + 1); iter.Valid(); iter.Next()) {
    next_sequences_set.emplace(iter.NextSequenceNumber());
  }

  std::vector<rocksdb::SequenceNumber> next_sequences(next_sequences_set.begin(), next_sequences_set.end());

  ASSERT_EQ(expected_next_sequences.size(), next_sequences.size());
  ASSERT_TRUE(std::equal(expected_next_sequences.begin(), expected_next_sequences.end(), next_sequences.begin()));
}
