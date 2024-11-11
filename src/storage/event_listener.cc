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

#include "event_listener.h"

#include <string>
#include <vector>

#include "fmt/format.h"

std::string BackgroundErrorReason2String(const rocksdb::BackgroundErrorReason reason) {
  std::vector<std::string> background_error_reason = {
      "flush", "compaction", "write_callback", "memtable", "manifest_write", "flush_no_wal", "manifest_write_no_wal"};
  if (static_cast<size_t>(reason) < background_error_reason.size()) {
    return background_error_reason[static_cast<size_t>(reason)];
  }
  return "unknown";
}

std::string FileCreatedReason2String(const rocksdb::TableFileCreationReason reason) {
  std::vector<std::string> file_created_reason = {"flush", "compaction", "recovery", "misc"};
  if (static_cast<size_t>(reason) < file_created_reason.size()) {
    return file_created_reason[static_cast<size_t>(reason)];
  }
  return "unknown";
}

std::string StallConditionType2String(const rocksdb::WriteStallCondition type) {
  switch (type) {
    case rocksdb::WriteStallCondition::kDelayed:
      return "delay";
    case rocksdb::WriteStallCondition::kStopped:
      return "stop";
    case rocksdb::WriteStallCondition::kNormal:
      return "normal";
  }
  return "unknown";
}

std::string CompressType2String(const rocksdb::CompressionType type) {
  for (const auto &option : engine::CompressionOptions) {
    if (option.type == type) {
      return option.name;
    }
  }
  return "unknown";
}

std::string ExtractSSTFileNameFromError(const std::string &error) {
  auto match_results = util::RegexMatch(error, ".*(/\\w*\\.sst).*");
  if (match_results.size() == 2) {
    return match_results[1];
  }
  return {};
}

bool IsDiskQuotaExceeded(const rocksdb::Status &bg_error) {
  // EDQUOT: Disk quota exceeded (POSIX.1-2001)
  std::string exceeded_quota_str = "Disk quota exceeded";
  std::string err_msg = bg_error.ToString();

  return err_msg.find(exceeded_quota_str) != std::string::npos;
}

void EventListener::OnCompactionBegin([[maybe_unused]] rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  LOG(INFO) << "[event_listener/compaction_begin] column family: " << ci.cf_name << ", job_id: " << ci.job_id
            << ", compaction reason: " << rocksdb::GetCompactionReasonString(ci.compaction_reason)
            << ", output compression type: " << CompressType2String(ci.compression)
            << ", base input level(files): " << ci.base_input_level << "(" << ci.input_files.size() << ")"
            << ", output level(files): " << ci.output_level << "(" << ci.output_files.size() << ")"
            << ", input bytes: " << ci.stats.total_input_bytes << ", output bytes:" << ci.stats.total_output_bytes
            << ", is_manual_compaction:" << (ci.stats.is_manual_compaction ? "yes" : "no");
}

void EventListener::OnCompactionCompleted([[maybe_unused]] rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  LOG(INFO) << "[event_listener/compaction_completed] column family: " << ci.cf_name << ", job_id: " << ci.job_id
            << ", compaction reason: " << rocksdb::GetCompactionReasonString(ci.compaction_reason)
            << ", output compression type: " << CompressType2String(ci.compression)
            << ", base input level(files): " << ci.base_input_level << "(" << ci.input_files.size() << ")"
            << ", output level(files): " << ci.output_level << "(" << ci.output_files.size() << ")"
            << ", input bytes: " << ci.stats.total_input_bytes << ", output bytes:" << ci.stats.total_output_bytes
            << ", is_manual_compaction:" << (ci.stats.is_manual_compaction ? "yes" : "no")
            << ", elapsed(micro): " << ci.stats.elapsed_micros;
  storage_->RecordStat(engine::StatType::CompactionCount, 1);
  storage_->CheckDBSizeLimit();
}

void EventListener::OnSubcompactionBegin(const rocksdb::SubcompactionJobInfo &si) {
  LOG(INFO) << "[event_listener/subcompaction_begin] column family: " << si.cf_name << ", job_id: " << si.job_id
            << ", compaction reason: " << rocksdb::GetCompactionReasonString(si.compaction_reason)
            << ", output compression type: " << CompressType2String(si.compression);
}

void EventListener::OnSubcompactionCompleted(const rocksdb::SubcompactionJobInfo &si) {
  LOG(INFO) << "[event_listener/subcompaction_completed] column family: " << si.cf_name << ", job_id: " << si.job_id
            << ", compaction reason: " << rocksdb::GetCompactionReasonString(si.compaction_reason)
            << ", output compression type: " << CompressType2String(si.compression)
            << ", base input level(files): " << si.base_input_level << ", output level(files): " << si.output_level
            << ", input bytes: " << si.stats.total_input_bytes << ", output bytes:" << si.stats.total_output_bytes
            << ", is_manual_compaction:" << (si.stats.is_manual_compaction ? "yes" : "no")
            << ", elapsed(micro): " << si.stats.elapsed_micros;
}

void EventListener::OnFlushBegin([[maybe_unused]] rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  LOG(INFO) << "[event_listener/flush_begin] column family: " << fi.cf_name << ", thread_id: " << fi.thread_id
            << ", job_id: " << fi.job_id << ", reason: " << rocksdb::GetFlushReasonString(fi.flush_reason);
}

void EventListener::OnFlushCompleted([[maybe_unused]] rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  storage_->RecordStat(engine::StatType::FlushCount, 1);
  storage_->CheckDBSizeLimit();
  LOG(INFO) << "[event_listener/flush_completed] column family: " << fi.cf_name << ", thread_id: " << fi.thread_id
            << ", job_id: " << fi.job_id << ", file: " << fi.file_path
            << ", reason: " << static_cast<int>(fi.flush_reason)
            << ", is_write_slowdown: " << (fi.triggered_writes_slowdown ? "yes" : "no")
            << ", is_write_stall: " << (fi.triggered_writes_stop ? "yes" : "no")
            << ", largest seqno: " << fi.largest_seqno << ", smallest seqno: " << fi.smallest_seqno;
}

void EventListener::OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *bg_error) {
  auto reason_str = BackgroundErrorReason2String(reason);
  auto error_str = bg_error->ToString();
  if (bg_error->IsCorruption() || bg_error->IsIOError()) {
    // Background error may occur when SST are generated during flush/compaction. If those files are not applied
    // to Version, we consider them non-fatal background error. We can override bg_error to recover from
    // background error.
    // Note that we cannot call Resume() manually because the error severity is unrecoverable.
    auto corrupt_sst = ExtractSSTFileNameFromError(error_str);
    if (!corrupt_sst.empty()) {
      std::vector<std::string> live_files;
      uint64_t manifest_size = 0;
      auto s = storage_->GetDB()->GetLiveFiles(live_files, &manifest_size, false /* flush_memtable */);
      if (s.ok() && std::find(live_files.begin(), live_files.end(), corrupt_sst) == live_files.end()) {
        *bg_error = rocksdb::Status::OK();
        LOG(WARNING) << fmt::format(
            "[event_listener/background_error] ignore no-fatal background error about sst file, reason: {}, bg_error: "
            "{}",
            reason_str, error_str);
        return;
      }
    }
  }

  if ((bg_error->IsNoSpace() || IsDiskQuotaExceeded(*bg_error)) &&
      bg_error->severity() < rocksdb::Status::kFatalError) {
    storage_->SetDBInRetryableIOError(true);
  }

  LOG(ERROR) << fmt::format("[event_listener/background_error] reason: {}, bg_error: {}", reason_str, error_str);
}

void EventListener::OnTableFileDeleted(const rocksdb::TableFileDeletionInfo &info) {
  LOG(INFO) << "[event_listener/table_file_deleted] db: " << info.db_name << ", sst file: " << info.file_path
            << ", status: " << info.status.ToString();
}

void EventListener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) {
  LOG(WARNING) << "[event_listener/stall_cond_changed] column family: " << info.cf_name
               << " write stall condition was changed, from " << StallConditionType2String(info.condition.prev)
               << " to " << StallConditionType2String(info.condition.cur);
}

void EventListener::OnTableFileCreated(const rocksdb::TableFileCreationInfo &info) {
  LOG(INFO) << "[event_listener/table_file_created] column family: " << info.cf_name
            << ", file path: " << info.file_path << ", file size: " << info.file_size << ", job_id: " << info.job_id
            << ", reason: " << FileCreatedReason2String(info.reason) << ", status: " << info.status.ToString();
}
