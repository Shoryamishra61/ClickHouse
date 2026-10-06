#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>

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

TEST(KeySelectivity, MostSelectiveKeyDecides)
{
    /// No predicates: everything matches. Several keys: the most selective one decides, and the
    /// smallest containment of each side.
    JoinKeyEstimate none;
    EXPECT_DOUBLE_EQ(none.selectivity, 1.0);
    EXPECT_DOUBLE_EQ(none.left_match_fraction, 1.0);

    JoinKeyEstimate keys;
    keys.add(10, 10, 0);
    keys.add(1000, 100, 0);
    keys.add(0, 0, 0);  /// Both NDVs unknown: not a key.
    EXPECT_DOUBLE_EQ(keys.selectivity, 0.001);
    /// 100 right values lie within the 1000 left values: a tenth of the left side matches.
    EXPECT_DOUBLE_EQ(keys.left_match_fraction, 0.1);
    EXPECT_DOUBLE_EQ(keys.right_match_fraction, 1.0);
}
