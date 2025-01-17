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

#include <glog/logging.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/transaction_log.h>
#include <rocksdb/write_batch.h>

#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "batch_sender.h"
#include "server/server.h"
#include "status.h"
#include "storage/redis_db.h"
#include "unique_fd.h"

enum class MigrationType {
  /// Use Redis commands to migrate data.
  /// It will try to extract commands from existing data and log, then replay
  /// them on the destination node.
  kRedisCommand = 0,
  /// Using raw key-value and "APPLYBATCH" command in kvrocks to migrate data.
  ///
  /// If downstream is not compatible with raw key-value, this migration type will
  /// auto switch to kRedisCommand.
  kRawKeyValue
};

enum class MigrationState { kNone = 0, kStarted, kSuccess, kFailed };

enum class SlotMigrationStage { kNone, kStart, kSnapshot, kWAL, kSuccess, kFailed, kClean };

enum class KeyMigrationResult { kMigrated, kExpired, kUnderlyingStructEmpty };

struct SlotMigrationJob {
  SlotMigrationJob(const SlotRange &slot_range_in, std::string dst_ip, int dst_port, int speed, int pipeline_size,
                   int seq_gap)
      : slot_range(slot_range_in),
        dst_ip(std::move(dst_ip)),
        dst_port(dst_port),
        max_speed(speed),
        max_pipeline_size(pipeline_size),
        seq_gap_limit(seq_gap) {}
  SlotMigrationJob(const SlotMigrationJob &other) = delete;
  SlotMigrationJob &operator=(const SlotMigrationJob &other) = delete;
  ~SlotMigrationJob() = default;

  SlotRange slot_range;
  std::string dst_ip;
  int dst_port;
  int max_speed;
  int max_pipeline_size;
  int seq_gap_limit;
};

class SyncMigrateContext;

class SlotMigrator : public redis::Database {
 public:
  explicit SlotMigrator(Server *srv);
  SlotMigrator(const SlotMigrator &other) = delete;
  SlotMigrator &operator=(const SlotMigrator &other) = delete;
  ~SlotMigrator();

  Status CreateMigrationThread();
  Status PerformSlotRangeMigration(const std::string &node_id, std::string &dst_ip, int dst_port,
                                   const SlotRange &range, SyncMigrateContext *blocking_ctx = nullptr);
  void ReleaseForbiddenSlotRange();
  void SetMaxMigrationSpeed(int value) {
    if (value >= 0) max_migration_speed_ = value;
  }
  void SetMaxPipelineSize(int value) {
    if (value > 0) max_pipeline_size_ = value;
  }
  void SetSequenceGapLimit(int value) {
    if (value > 0) seq_gap_limit_ = value;
  }
  void SetMigrateBatchRateLimit(size_t bytes_per_sec) { migrate_batch_bytes_per_sec_ = bytes_per_sec; }
  void SetMigrateBatchSize(size_t size) { migrate_batch_size_bytes_ = size; }
  void SetStopMigrationFlag(bool value) { stop_migration_ = value; }
  bool IsMigrationInProgress() const { return migration_state_ == MigrationState::kStarted; }
  SlotMigrationStage GetCurrentSlotMigrationStage() const { return current_stage_; }
  SlotRange GetForbiddenSlotRange() const { return forbidden_slot_range_; }
  SlotRange GetMigratingSlotRange() const { return slot_range_; }
  std::string GetDstNode() const { return dst_node_; }
  void GetMigrationInfo(std::string *info) const;
  void CancelSyncCtx();

 private:
  void loop();
  void runMigrationProcess();
  bool isTerminated() const { return thread_state_ == ThreadState::Terminated; }
  Status startMigration();
  Status sendSnapshot();
  Status syncWAL();
  Status finishSuccessfulMigration();
  Status finishFailedMigration();
  void clean();

  Status authOnDstNode(int sock_fd, const std::string &password);
  Status setImportStatusOnDstNode(int sock_fd, int status);
  static StatusOr<bool> supportedApplyBatchCommandOnDstNode(int sock_fd);

  Status sendSnapshotByCmd();
  Status syncWALByCmd();
  Status checkSingleResponse(int sock_fd);
  Status checkMultipleResponses(int sock_fd, int total);

  StatusOr<KeyMigrationResult> migrateOneKey(const rocksdb::Slice &key, const rocksdb::Slice &encoded_metadata,
                                             std::string *restore_cmds);
  Status migrateSimpleKey(const rocksdb::Slice &key, const Metadata &metadata, const std::string &bytes,
                          std::string *restore_cmds);
  Status migrateComplexKey(const rocksdb::Slice &key, const Metadata &metadata, std::string *restore_cmds);
  Status migrateStream(const rocksdb::Slice &key, const StreamMetadata &metadata, std::string *restore_cmds);
  Status migrateBitmapKey(const InternalKey &inkey, std::unique_ptr<rocksdb::Iterator> *iter,
                          std::vector<std::string> *user_cmd, std::string *restore_cmds);

  Status sendCmdsPipelineIfNeed(std::string *commands, bool need);
  void applyMigrationSpeedLimit() const;
  Status generateCmdsFromBatch(rocksdb::BatchResult *batch, std::string *commands);
  Status migrateIncrementData(std::unique_ptr<rocksdb::TransactionLogIterator> *iter, uint64_t end_seq);
  Status syncWalBeforeForbiddingSlot();
  Status syncWalAfterForbiddingSlot();

  Status sendMigrationBatch(BatchSender *batch);
  Status sendSnapshotByRawKV();
  Status syncWALByRawKV();
  bool catchUpIncrementalWAL();
  Status migrateIncrementalDataByRawKV(uint64_t end_seq, BatchSender *batch_sender);

  void setForbiddenSlotRange(const SlotRange &slot_range);
  std::unique_lock<std::mutex> blockingLock() { return std::unique_lock<std::mutex>(blocking_mutex_); }

  void resumeSyncCtx(const Status &migrate_result);

  enum class ParserState { ArrayLen, BulkLen, BulkData, ArrayData, OneRspEnd };
  enum class ThreadState { Uninitialized, Running, Terminated };

  static constexpr int kDefaultMaxPipelineSize = 16;
  static constexpr int kDefaultMaxMigrationSpeed = 4096;
  static constexpr int kDefaultSequenceGapLimit = 10000;
  static constexpr int kMaxItemsInCommand = 16;  // number of items in every write command of complex keys
  static constexpr int kMaxLoopTimes = 10;

  Server *srv_;

  int max_migration_speed_ = kDefaultMaxMigrationSpeed;
  int max_pipeline_size_ = kDefaultMaxPipelineSize;
  uint64_t seq_gap_limit_ = kDefaultSequenceGapLimit;
  std::atomic<size_t> migrate_batch_bytes_per_sec_ = 1 * GiB;
  std::atomic<size_t> migrate_batch_size_bytes_;

  SlotMigrationStage current_stage_ = SlotMigrationStage::kNone;
  ParserState parser_state_ = ParserState::ArrayLen;
  std::atomic<ThreadState> thread_state_ = ThreadState::Uninitialized;
  std::atomic<MigrationState> migration_state_ = MigrationState::kNone;

  int current_pipeline_size_ = 0;
  uint64_t last_send_time_ = 0;

  std::thread t_;
  std::mutex job_mutex_;
  std::condition_variable job_cv_;
  std::unique_ptr<SlotMigrationJob> migration_job_;  // GUARDED_BY(job_mutex_)

  std::string dst_node_;
  std::string dst_ip_;
  int dst_port_ = -1;
  UniqueFD dst_fd_;

  MigrationType migration_type_ = MigrationType::kRedisCommand;

  static_assert(std::atomic<SlotRange>::is_always_lock_free, "SlotRange is not lock free.");
  std::atomic<SlotRange> forbidden_slot_range_ = SlotRange{-1, -1};
  std::atomic<SlotRange> slot_range_ = SlotRange{-1, -1};
  std::atomic<SlotRange> migrate_failed_slot_range_ = SlotRange{-1, -1};

  std::atomic<bool> stop_migration_ = false;  // if is true migration will be stopped but the thread won't be destroyed
  const rocksdb::Snapshot *slot_snapshot_ = nullptr;
  uint64_t wal_begin_seq_ = 0;

  std::mutex blocking_mutex_;
  SyncMigrateContext *blocking_context_ = nullptr;
};
