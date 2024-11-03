#pragma once

#include <memory>
#include <vector>

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

class CentroidBuffer {
 public:
  class CentroidBufferIterator {
   public:
    virtual bool Next() = 0;
    virtual StatusOr<Centroid> Get() const = 0;
    virtual bool Valid() const = 0;
    virtual bool Merge(const Centroid& other) = 0;
  };
  virtual CentroidBufferIterator Begin() const = 0;
  virtual CentroidBufferIterator End() const = 0;
  virtual StatusOr<uint64_t> Size() const = 0;
  virtual CentroidBufferIterator Add(const Centroid& centroid) = 0;
};

class TDigest {
 public:
  explicit TDigest(uint64_t compression, std::unique_ptr<CentroidBuffer> buffer,
                   std::unique_ptr<CentroidBuffer> centroids)
      : delta_(compression), buffer_(std::move(buffer)), centroids_(std::move(centroids)) {}

  void Merge(const std::vector<TDigest>& others);
  void Add(const std::vector<Centroid>& centroids);
  double Quantile(double q) const;

 private:
  class TDigestImpl;
  const uint64_t delta_;
  std::unique_ptr<TDigestImpl> tdigest_impl_;
  std::unique_ptr<CentroidBuffer> buffer_;     // this can be random ordered.
  std::unique_ptr<CentroidBuffer> centroids_;  // this should be sorted.
};


template <typename TD, typename Lerp>
inline double Quantile(const TD &td, double q) {
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