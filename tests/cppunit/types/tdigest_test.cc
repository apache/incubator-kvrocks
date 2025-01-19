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

#include "types/tdigest.h"

#include <gtest/gtest.h>

#include <cmath>
#include <memory>
#include <range/v3/range.hpp>
#include <string>
#include <string_view>
#include <vector>

#include "glog/logging.h"
#include "range/v3/view/iota.hpp"
#include "range/v3/view/join.hpp"
#include "range/v3/view/transform.hpp"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "time_util.h"
#include "types/redis_tdigest.h"

namespace {
// class TDSample {
//   public:
//   struct Iterator {
//     Iterator* Clone() const;
//     bool Next();
//     bool Valid() const;
//     const Centroid& Centroid() const;
//   };

//   Iterator* Begin();
//   Iterator* End();
//   double TotalWeight();
//   double Min() const;
//   double Max() const;
// };
// class CentroidSimpleStorage {

// private:
// std::list<Centroid> centroids_;
// std::vector<double> buffer_;
// };

std::vector<double> QuantileOf(const std::vector<double> &samples, const std::vector<double> &qs) {
  std::vector<double> result;
  result.reserve(qs.size());
  std::vector<double> sorted_samples = samples;
  std::sort(sorted_samples.begin(), sorted_samples.end());
  for (auto q : qs) {
    auto index = q * static_cast<double>(sorted_samples.size());
    if (index <= 1) {
      result.push_back(sorted_samples.front());
    } else if (index >= static_cast<double>(sorted_samples.size() - 1)) {
      result.push_back(sorted_samples.back());
    } else {
      auto left = sorted_samples[static_cast<int>(index)];
      auto right = sorted_samples[static_cast<int>(index) + 1];
      auto diff = index - static_cast<int>(index);
      result.push_back(left + (right - left) * diff);
    }
  }
  return result;
}

std::vector<double> GenerateSamples(int count) {
  return ranges::views::iota(1, count + 1) | ranges::views::transform([](int i) { return i; }) |
         ranges::to<std::vector<double>>();
}

std::vector<double> GenerateQuantiles(int count, bool with_head = false, bool with_tail = false) {
  std::vector<double> qs;
  qs.reserve(count);
  for (int i = 1; i <= count; i++) {
    qs.push_back(static_cast<double>(i) / static_cast<double>(count));
  }
  if (with_head) {
    qs.insert(qs.begin(), 0);
  }
  if (with_tail) {
    qs.push_back(1);
  }
  return qs;
}

}  // namespace

class RedisTDigestTest : public TestBase {
 protected:
  RedisTDigestTest() : name_("tdigest_test") {
    tdigest_ = std::make_unique<redis::TDigest>(storage_.get(), "tdigest_ns");
  }

  std::string name_;
  std::unique_ptr<redis::TDigest> tdigest_;
};

TEST_F(RedisTDigestTest, CentroidTest) {
  Centroid c1{
      2.,
      3.,
  };
  Centroid c2{
      3.,
      4.,
  };

  c1.Merge(c2);

  EXPECT_NEAR(c1.weight, 7., 0.01);
  EXPECT_NEAR(c1.mean, 2.57, 0.01);
}

// TEST_F(RedisTDigestTest, TempBufferMerge) {
//   constexpr auto delta = 10;
//   CentroidsWithDelta digest_centroids{.delta = delta};
//   // from 1.0 to 100.0 using an elegant way we are in cpp17 and with rangev3
//   std::vector<double> samples = ranges::views::iota(1, 101) | ranges::to<std::vector<double>>();
//   auto status = TDigestMerge(samples, digest_centroids);
//   ASSERT_TRUE(status.IsOK());
//   digest_centroids = *status;
//   LOG(INFO) << "tdigest internel centriods: "
//             << (digest_centroids.centroids | ranges::views::transform([](const auto& c) {
//                   return fmt::format("mean: {}, weight: {}", c.mean, c.weight);
//                 }) |
//                 ranges::views::join(' '));
//   EXPECT_EQ(digest_centroids.centroids.size(), delta);
//   EXPECT_NEAR(digest_centroids.centroids[0].mean, 1.0, 0.01);
//   EXPECT_NEAR(digest_centroids.centroids[0].weight, 1.0, 0.01);
//   EXPECT_NEAR(digest_centroids.centroids[9].mean, 10.0, 0.01);
//   EXPECT_NEAR(digest_centroids.centroids[9].weight, 1.0, 0.01);
// }

// TEST_F(RedisTDigestTest, CentroidsMerge) {}

TEST_F(RedisTDigestTest, Create) {
  std::string test_digest_name = "test_digest_create";
  {
    auto status = tdigest_->Create(*ctx_, test_digest_name, {100});
    ASSERT_TRUE(status);
    ASSERT_TRUE(status->ok());

    status = tdigest_->Create(*ctx_, test_digest_name, {100});
    ASSERT_FALSE(status);

    auto ns_key = tdigest_->AppendNamespacePrefix(test_digest_name);
    TDigestMetadata metadata;
    auto get_status = tdigest_->GetMetaData(*ctx_, ns_key, &metadata);
    ASSERT_TRUE(get_status.ok()) << get_status.ToString();
    ASSERT_EQ(metadata.compression, 100) << metadata.compression;
  }
}

TEST_F(RedisTDigestTest, Quantile) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());
  {
    auto status = tdigest_->Create(*ctx_, test_digest_name, {100});
    ASSERT_TRUE(status);
    ASSERT_TRUE(status->ok());
  }
  std::vector<double> samples = ranges::views::iota(1, 101) | ranges::views::transform([](int i) { return i; }) |
                                ranges::to<std::vector<double>>();
  auto status = tdigest_->Add(*ctx_, test_digest_name, samples);
  ASSERT_TRUE(status.ok()) << status.ToString();

  std::vector<double> qs = {0.5, 0.9, 0.99};
  redis::TDigestQuantitleResult result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  ASSERT_EQ(result.quantiles.size(), qs.size());
  EXPECT_NEAR(result.quantiles[0], 50.5, 0.01);
  EXPECT_NEAR(result.quantiles[1], 90.5, 0.01);
  EXPECT_NEAR(result.quantiles[2], 100, 0.01);
}

TEST_F(RedisTDigestTest, PlentyQuantile_10000_144) {
  std::string test_digest_name = "test_digest_quantile" + std::to_string(util::GetTimeStampMS());
  {
    auto status = tdigest_->Create(*ctx_, test_digest_name, {100});
    ASSERT_TRUE(status);
    ASSERT_TRUE(status->ok());
  }

  int sample_count = 10000;
  int quantile_count = 144;
  auto samples = GenerateSamples(sample_count);
  auto status = tdigest_->Add(*ctx_, test_digest_name, samples);
  ASSERT_TRUE(status.ok()) << status.ToString();
  
  auto qs = GenerateQuantiles(quantile_count);
  auto result = QuantileOf(samples, qs);

  redis::TDigestQuantitleResult tdigest_result;
  status = tdigest_->Quantile(*ctx_, test_digest_name, qs, &tdigest_result);
  ASSERT_TRUE(status.ok()) << status.ToString();

  for (int i = 0; i < quantile_count; i++) {
    EXPECT_NEAR(tdigest_result.quantiles[i], result[i], 1) << "quantile is: " << qs[i];
  }
}

TEST_F(RedisTDigestTest, CDF) {}