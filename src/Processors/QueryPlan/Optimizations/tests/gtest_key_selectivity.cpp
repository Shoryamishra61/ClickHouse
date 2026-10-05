#include <gtest/gtest.h>

#include <cmath>

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

TEST(KeySelectivity, EqualityFromKnownDistinctValues)
{
    /// Zero is an unknown NDV: the known side decides, both unknown give nothing.
    EXPECT_EQ(equalitySelectivity(0, 0), std::nullopt);
    EXPECT_DOUBLE_EQ(*equalitySelectivity(100, 0), 0.01);
    EXPECT_DOUBLE_EQ(*equalitySelectivity(0, 100), 0.01);
    EXPECT_DOUBLE_EQ(*equalitySelectivity(100, 1000), 0.001);
}

TEST(KeySelectivity, CombineMostSelectiveOrExponentialBackoff)
{
    EXPECT_DOUBLE_EQ(combineKeySelectivities({}, false), 1.0);
    EXPECT_DOUBLE_EQ(combineKeySelectivities({}, true), 1.0);
    EXPECT_DOUBLE_EQ(combineKeySelectivities({0.1}, true), 0.1);

    /// Without backoff the most selective key decides alone.
    EXPECT_DOUBLE_EQ(combineKeySelectivities({0.5, 0.1}, false), 0.1);

    /// With backoff the keys are sorted from the most selective and take the exponents 1, 1/2, 1/4, 1/8.
    EXPECT_DOUBLE_EQ(combineKeySelectivities({0.5, 0.1}, true), 0.1 * std::sqrt(0.5));
    EXPECT_DOUBLE_EQ(combineKeySelectivities({0.1, 0.5}, true), 0.1 * std::sqrt(0.5));
    EXPECT_DOUBLE_EQ(
        combineKeySelectivities({0.5, 0.4, 0.3, 0.2, 0.1}, true),
        0.1 * std::pow(0.2, 0.5) * std::pow(0.3, 0.25) * std::pow(0.4, 0.125));
}
