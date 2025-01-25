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

#include <vector>

#include <fmt/format.h>

#include "status.h"

struct Centroid {
  double mean;
  double weight = 1.0;

  // merge with another centroid
  void Merge(const Centroid& centroid) {
    weight += centroid.weight;
    mean += (centroid.mean - mean) * centroid.weight / weight;
  }

  std::string ToString() const { return fmt::format("centroid<mean: {}, weight: {}>", mean, weight); }

  template <typename T>
  explicit Centroid(T&& centroid) : mean(centroid.mean), weight(centroid.weight) {}

  explicit Centroid() = default;
  explicit Centroid(double mean, double weight) : mean(mean), weight(weight) {}
};

struct CentroidsWithDelta {
  std::vector<Centroid> centroids;
  uint64_t delta;
  double min;
  double max;
  double total_weight;
};

StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<CentroidsWithDelta>& centroids_list);
StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<double>& buffer, const CentroidsWithDelta& centroid_list);

// TD should looks like below:
// class TDSample {
//   public:
//   struct Iterator {
//     Iterator* Clone() const;
//     bool Next();
//     bool Valid() const;
//     StatusOr<Centroid> GetCentroid() const;
//   };

//   Iterator* Begin();
//   Iterator* End();
//   double TotalWeight();
//   double Min() const;
//   double Max() const;
// };

template <typename TD, typename Lerp>
inline StatusOr<double> TDigestQuantile(TD&& td, double q, Lerp lerp) {
  if (q < 0 || q > 1 || td.Size() == 0) {
    return NAN;
  }

  const double index = q * td.TotalWeight();
  if (index <= 1) {
    return td.Min();
  } else if (index >= td.TotalWeight() - 1) {
    return td.Max();
  }

  // find centroid contains the index
  double weight_sum = 0;
  auto iter = td.Begin();
  for (; iter->Valid(); iter->Next()) {
    auto centroid = iter->GetCentroid();
    if (!centroid) {
      return centroid.ToStatus();
    }
    weight_sum += centroid->weight;
    if (index <= weight_sum) {
      break;
    }
  }

  auto centroid = iter->GetCentroid();
  if (!centroid) {
    return centroid.ToStatus();
  }

  // deviation of index from the centroid center
  double diff = index + centroid->weight / 2 - weight_sum;

  // index happen to be in a unit weight centroid
  if (centroid->weight == 1 && std::abs(diff) < 0.5) {
    return centroid->mean;
  }

  // find adjacent centroids for interpolation
  auto ci_left = iter->Clone();
  auto ci_right = iter->Clone();
  if (diff > 0) {
    if (ci_right == td.End()) {
      // index larger than center of last bin
      auto c = ci_left->GetCentroid();
      if (!c) {
        return c.ToStatus();
      }
      DCHECK_GE(c->weight, 2);
      return lerp(c->mean, td.Max(), diff / (c->weight / 2));
    }
    ci_right->Next();
  } else {
    if (ci_left == td.Begin()) {
      // index smaller than center of first bin
      auto c = ci_left->GetCentroid();
      if (!c) {
        return c.ToStatus();
      }
      DCHECK_GE(c->weight, 2);
      return lerp(td.Min(), c->mean, index / (c->weight / 2));
    }
    ci_left->Prev();
    auto lc = ci_left->GetCentroid();
    if (!lc) {
      return lc.ToStatus();
    }
    auto rc = ci_right->GetCentroid();
    if (!rc) {
      return rc.ToStatus();
    }
    diff += lc->weight / 2 + rc->weight / 2;
  }

  auto lc = ci_left->GetCentroid();
  if (!lc) {
    return lc.ToStatus();
  }
  auto rc = ci_right->GetCentroid();
  if (!rc) {
    return rc.ToStatus();
  }

  // interpolate from adjacent centroids
  diff /= (lc->weight / 2 + rc->weight / 2);
  return lerp(lc->mean, rc->mean, diff);
}