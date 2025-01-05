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

#include <range/v3/range.hpp>

#include "glog/logging.h"
#include "range/v3/view/iota.hpp"
#include "range/v3/view/join.hpp"
#include "range/v3/view/transform.hpp"
#include "test_base.h"

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
}  // namespace

class RedisTDigestTest : public TestBase {};

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

TEST_F(RedisTDigestTest, TempBufferMerge) {
  constexpr auto delta = 10;
  CentroidsWithDelta digest_centroids{.delta = delta};
  // from 1.0 to 100.0 using an elegant way we are in cpp17 and with rangev3
  std::vector<double> samples = ranges::views::iota(1, 101) | ranges::to<std::vector<double>>();
  auto status = TDigestMerge(samples, digest_centroids);
  ASSERT_TRUE(status.IsOK());
  digest_centroids = *status;
  LOG(INFO) << "tdigest internel centriods: " << (digest_centroids.centroids |
      ranges::views::transform([](const auto& c) { return fmt::format("mean: {}, weight: {}", c.mean, c.weight); }) |
      ranges::views::join(' '));
  EXPECT_EQ(digest_centroids.centroids.size(), delta);
  EXPECT_NEAR(digest_centroids.centroids[0].mean, 1.0, 0.01);
  EXPECT_NEAR(digest_centroids.centroids[0].weight, 1.0, 0.01);
  EXPECT_NEAR(digest_centroids.centroids[9].mean, 10.0, 0.01);
  EXPECT_NEAR(digest_centroids.centroids[9].weight, 1.0, 0.01);
}

TEST_F(RedisTDigestTest, CentroidsMerge) {}

TEST_F(RedisTDigestTest, Quantile) {}

TEST_F(RedisTDigestTest, CDF) {}