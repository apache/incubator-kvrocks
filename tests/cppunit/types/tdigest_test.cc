#include "types/tdigest.h"

#include <gtest/gtest.h>

#include <list>

#include "test_base.h"

namespace {
// class TDSample {
//   public:
//   struct Iterator {
//     Iterator* Clone() const;
//     bool Next();
//     bool Valid() const;
//     const Centroid& Centriod() const;
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
  Centroid c2 {
    3.,
    4.,
  };

  c1.Merge(c2);

  EXPECT_NEAR(c1.weight, 7., 0.01);
  EXPECT_NEAR(c1.mean, 2.57, 0.01);
}

TEST_F(RedisTDigestTest, TempBufferMerge) {
  // constexpr auto delta = 10;
  
}

TEST_F(RedisTDigestTest, CentroidsMerge) {}

TEST_F(RedisTDigestTest, Quantile) {}

TEST_F(RedisTDigestTest, CDF) {}