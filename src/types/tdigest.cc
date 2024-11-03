#include "tdigest.h"

namespace {

// reference:
// https://github.com/apache/arrow/blob/a08037f33f2fe00763032623e18ba049d19a024f/cpp/src/arrow/util/tdigest.cc a
// numerically stable lerp is unbelievably complex but we are *approximating* the quantile, so let's keep it simple
double Lerp(double a, double b, double t) { return a + t * (b - a); }

// scale function K0: linear function, as baseline
struct ScalerK0 {
  explicit ScalerK0(uint32_t delta) : delta_norm(delta / 2.0) {}

  double K(double q) const { return delta_norm * q; }
  double Q(double k) const { return k / delta_norm; }

  const double delta_norm;
};

// scale function K1
struct ScalerK1 {
  explicit ScalerK1(uint32_t delta) : delta_norm(delta / (2.0 * M_PI)) {}

  double K(double q) const { return delta_norm * std::asin(2 * q - 1); }
  double Q(double k) const { return (std::sin(k / delta_norm) + 1) / 2; }

  const double delta_norm;
};
}  // namespace

using Iterator = std::unique_ptr<CentroidBuffer::CentroidBufferIterator>;

class TDigestMerger : private ScalerK1 {
 public:
  explicit TDigestMerger(uint64_t delta, CentroidBuffer* centroids) : ScalerK1(delta), centroids_(centroids) {}
  void Reset(double total_weight) { total_weight_ = total_weight; }

  void Add(const Centroid& centroid) {
    const double weight = weight_so_far_ + centroid.weight;
    if (weight < weight_limit_) {
    }
    centroids_
  }

 private:
  CentroidBuffer* centroids_;
  double total_weight_;   // total weight of this tdigest
  double weight_so_far_;  // accumulated weight till current bin
  double weight_limit_;   // max accumulated weight to move to next bin
};

class TDigest::TDigestImpl {
  explicit TDigestImpl(std::unique_ptr<CentroidBuffer> buffer, std::unique_ptr<CentroidBuffer> centroids)
      : buffer_(std::move(buffer)), centroids_(std::move(centroids)) {}

 private:
  std::unique_ptr<CentroidBuffer> buffer_;
  std::unique_ptr<CentroidBuffer> centroids_;
};