#pragma once

#include <memory>
#include <vector>

#include "rocksdb/status.h"
#include "status.h"

struct Centroid {
  double mean;
  double weight = 1.0;

  // merge with another centroid
  void Merge(const Centroid& centroid) {
    weight += centroid.weight;
    mean += (centroid.mean - mean) * centroid.weight / weight;
  }
};

struct CentroidsWithDelta {
  std::vector<Centroid> centroids;
  uint64_t delta;
};

StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<CentroidsWithDelta>& centroids_list);
StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<double>& buffer,
                                             const CentroidsWithDelta& centroid_list);

template <typename TD, typename Lerp>
inline double Quantile(const TD& td, double q) {
  if (q < 0 || q > 1 || td.size() == 0) {
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
    const Centroid centroid = iter->Get();
    weight_sum += centroid.weight;
    if (index <= weight_sum) {
      break;
    }
  }

  const Centroid centroid = iter->Get();

  // deviation of index from the centroid center
  double diff = index + centroid.weight / 2 - weight_sum;

  // index happen to be in a unit weight centroid
  if (centroid.weight == 1 && std::abs(diff) < 0.5) {
    return centroid.mean;
  }

  // find adjacent centroids for interpolation
  auto ci_left = iter->Clone();
  auto ci_right = iter->Clone();
  if (diff > 0) {
    if (ci_right == td.End()) {
      // index larger than center of last bin
      const Centroid c = ci_left->Get();
      DCHECK_GE(c.weight, 2);
      return Lerp(c.mean, td.Max(), diff / (c.weight / 2));
    }
    ci_right->Next();
  } else {
    if (ci_left == td.Begin()) {
      // index smaller than center of first bin
      const Centroid c = ci_left->Get();
      DCHECK_GE(c.weight, 2);
      return Lerp(td.Min(), c.mean, index / (c.weight / 2));
    }
    --ci_left;
    diff += td[ci_left].weight / 2 + td[ci_right].weight / 2;
  }

  // interpolate from adjacent centroids
  diff /= (td[ci_left].weight / 2 + td[ci_right].weight / 2);
  return Lerp(td[ci_left].mean, td[ci_right].mean, diff);
}