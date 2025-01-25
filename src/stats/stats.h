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

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

enum StatsMetricFlags {
  STATS_METRIC_COMMAND = 0,       // Number of commands executed
  STATS_METRIC_NET_INPUT,         // Bytes read to network
  STATS_METRIC_NET_OUTPUT,        // Bytes written to network
  STATS_METRIC_ROCKSDB_PUT,       // Number of calls of Put and Write in rocksdb
  STATS_METRIC_ROCKSDB_GET,       // Number of calls of get in rocksdb
  STATS_METRIC_ROCKSDB_MULTIGET,  // Number of calls of mulget in rocksdb
  STATS_METRIC_ROCKSDB_SEEK,      // Number of calls of seek in rocksdb
  STATS_METRIC_ROCKSDB_NEXT,      // Number of calls of next in rocksdb
  STATS_METRIC_ROCKSDB_PREV,      // Number of calls of prev in rocksdb
  STATS_METRIC_COUNT
};

constexpr int STATS_METRIC_SAMPLES = 16;  // Number of samples per metric

// Experimental part to support histograms for cmd statistics
struct CommandHistogram {
  std::vector<std::unique_ptr<std::atomic<uint64_t>>> buckets;
  std::atomic<uint64_t> calls;
  std::atomic<uint64_t> sum;
};

struct CommandStat {
  std::atomic<uint64_t> calls;
  std::atomic<uint64_t> latency;
};

struct InstMetric {
  uint64_t last_sample_time_ms;  // Timestamp of the last sample in ms
  uint64_t last_sample_count;    // Count in the last sample
  uint64_t samples[STATS_METRIC_SAMPLES];
  int idx;
};

class Stats {
 public:
  std::atomic<uint64_t> total_calls = {0};
  std::atomic<uint64_t> in_bytes = {0};
  std::atomic<uint64_t> out_bytes = {0};

  mutable std::shared_mutex inst_metrics_mutex;
  std::vector<InstMetric> inst_metrics;

  std::atomic<uint64_t> fullsync_count = {0};
  std::atomic<uint64_t> psync_err_count = {0};
  std::atomic<uint64_t> psync_ok_count = {0};
  std::map<std::string, CommandStat> commands_stats;

  using BucketBoundaries = std::vector<double>;
  BucketBoundaries bucket_boundaries;
  std::map<std::string, CommandHistogram> commands_histogram;

  explicit Stats(std::vector<double> histogram_bucket_boundaries);

  void IncrCalls(const std::string &command_name);
  void IncrLatency(uint64_t latency, const std::string &command_name);
  void IncrInboundBytes(uint64_t bytes) { in_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void IncrOutboundBytes(uint64_t bytes) { out_bytes.fetch_add(bytes, std::memory_order_relaxed); }
  void IncrFullSyncCount() { fullsync_count.fetch_add(1, std::memory_order_relaxed); }
  void IncrPSyncErrCount() { psync_err_count.fetch_add(1, std::memory_order_relaxed); }
  void IncrPSyncOKCount() { psync_ok_count.fetch_add(1, std::memory_order_relaxed); }
  static int64_t GetMemoryRSS();
  void TrackInstantaneousMetric(int metric, uint64_t current_reading);
  uint64_t GetInstantaneousMetric(int metric) const;
};
