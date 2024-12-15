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

#include <rocksdb/db.h>

#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

class LockManager {
 public:
  explicit LockManager(unsigned hash_power)
      : hash_power_(hash_power), hash_mask_((1U << hash_power) - 1), mutex_pool_(Size()) {}
  ~LockManager() = default;

  LockManager(const LockManager &) = delete;
  LockManager &operator=(const LockManager &) = delete;

  unsigned Size() const { return (1U << hash_power_); }

  void Lock(std::string_view key) { mutex_pool_[hash(key)].lock(); }
  void UnLock(std::string_view key) { mutex_pool_[hash(key)].unlock(); }
  void Lock(rocksdb::Slice key) { Lock(key.ToStringView()); }
  void UnLock(rocksdb::Slice key) { UnLock(key.ToStringView()); }

  template <typename Key>
  std::mutex *Get(const Key &key) {
    return &mutex_pool_[hash(key)];
  }

  template <typename Keys>
  std::vector<std::mutex *> MultiGet(const Keys &keys) {
    std::set<unsigned, std::greater<unsigned>> to_acquire_indexes;
    // We are using the `set` to avoid retrieving the mutex, as well as guarantee to retrieve
    // the order of locks.
    //
    // For example, we need lock the key `A` and `B` and they have the same lock hash
    // index, it will be deadlock if lock the same mutex twice. Besides, we also need
    // to order the mutex before acquiring locks since different threads may acquire
    // same keys with different order.
    for (const auto &key : keys) {
      to_acquire_indexes.insert(hash(key));
    }

    std::vector<std::mutex *> locks;
    locks.reserve(to_acquire_indexes.size());
    for (auto index : to_acquire_indexes) {
      locks.emplace_back(&mutex_pool_[index]);
    }
    return locks;
  }

 private:
  unsigned hash_power_;
  unsigned hash_mask_;
  std::vector<std::mutex> mutex_pool_;

  unsigned hash(std::string_view key) const { return std::hash<std::string_view>{}(key)&hash_mask_; }
};

class LockGuard {
 public:
  template <typename KeyType>
  explicit LockGuard(LockManager *lock_mgr, const KeyType &key) : lock_(lock_mgr->Get(key)) {
    lock_->lock();
  }
  ~LockGuard() {
    if (lock_) lock_->unlock();
  }

  LockGuard(const LockGuard &) = delete;
  LockGuard &operator=(const LockGuard &) = delete;

  LockGuard(LockGuard &&guard) noexcept : lock_(guard.lock_) { guard.lock_ = nullptr; }

  LockGuard &operator=(LockGuard &&other) noexcept {
    if (&other != this) {
      std::destroy_at(this);
      new (this) LockGuard(std::move(other));
    }
    return *this;
  }

 private:
  std::mutex *lock_{nullptr};
};

class MultiLockGuard {
 public:
  template <typename Keys>
  explicit MultiLockGuard(LockManager *lock_mgr, const Keys &keys) : locks_(lock_mgr->MultiGet(keys)) {
    for (const auto &iter : locks_) {
      iter->lock();
    }
  }

  ~MultiLockGuard() {
    // Lock with order `A B C` and unlock should be `C B A`
    for (auto iter = locks_.rbegin(); iter != locks_.rend(); ++iter) {
      (*iter)->unlock();
    }
  }

  MultiLockGuard(const MultiLockGuard &) = delete;
  MultiLockGuard &operator=(const MultiLockGuard &) = delete;

  MultiLockGuard(MultiLockGuard &&guard) : locks_(std::move(guard.locks_)) { guard.locks_.clear(); }
  MultiLockGuard &operator=(MultiLockGuard &&other) noexcept {
    if (this != &other) {
      std::destroy_at(this);
      new (this) MultiLockGuard(std::move(other));
    }
    return *this;
  }

 private:
  std::vector<std::mutex *> locks_;
};
