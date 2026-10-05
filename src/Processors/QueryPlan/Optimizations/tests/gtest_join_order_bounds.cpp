#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>

using namespace DB;

TEST(JoinOrderBounds, RowsUpperBoundByJoinKind)
{
    constexpr UInt64 max = std::numeric_limits<UInt64>::max();
    const auto bound = [](std::optional<UInt64> left, std::optional<UInt64> right, JoinKind kind, JoinStrictness strictness = JoinStrictness::All)
    {
        return estimateJoinRowsUpperBound(left, right, kind, strictness);
    };

    EXPECT_EQ(bound(10, 20, JoinKind::Inner), 200);
    EXPECT_EQ(bound(10, 20, JoinKind::Cross), 200);
    /// A preserved row stays even without a match, so an empty other side does not zero the bound.
    EXPECT_EQ(bound(10, 20, JoinKind::Left), 200);
    EXPECT_EQ(bound(10, 0, JoinKind::Left), 10);
    EXPECT_EQ(bound(0, 20, JoinKind::Right), 20);
    EXPECT_EQ(bound(10, 20, JoinKind::Full), 220);
    EXPECT_EQ(bound(10, 0, JoinKind::Full), 10);
    /// Semi and anti joins keep at most the preserved input, whatever the other side.
    EXPECT_EQ(bound(10, 20, JoinKind::Left, JoinStrictness::Semi), 10);
    EXPECT_EQ(bound(10, 20, JoinKind::Left, JoinStrictness::Anti), 10);
    EXPECT_EQ(bound(10, 20, JoinKind::Right, JoinStrictness::Semi), 20);
    EXPECT_EQ(bound(10, std::nullopt, JoinKind::Left, JoinStrictness::Semi), 10);
    /// An unknown input bound makes the result unknown.
    EXPECT_EQ(bound(std::nullopt, 20, JoinKind::Inner), std::nullopt);
    EXPECT_EQ(bound(10, std::nullopt, JoinKind::Left), std::nullopt);
    /// Saturation instead of wrap-around.
    EXPECT_EQ(bound(max, 2, JoinKind::Inner), max);
    EXPECT_EQ(bound(max, 2, JoinKind::Full), max);
}

TEST(JoinOrderBounds, SemiAntiRowsFromKeyContainment)
{
    /// Without the containment the other side's rows per preserved key stand in: 100000 rows over
    /// 10000 keys give one match per preserved row, so nothing is dropped.
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Semi), 100000);
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Anti), 1);
    /// With it, a tenth of the preserved key values has a match whatever the other side's rows.
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Semi, 0.1), 10000);
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, 1.0 / 10000, JoinKind::Left, JoinStrictness::Anti, 0.1), 90000);
    EXPECT_EQ(estimateJoinCardinality(100000, 1000, 1.0 / 10000, JoinKind::Right, JoinStrictness::Semi, 1.0), 1000);
    /// A key estimate picks the preserved side's fraction by the join kind.
    JoinKeyEstimate keys{.selectivity = 1.0 / 10000, .left_match_fraction = 0.1, .right_match_fraction = 1.0};
    EXPECT_EQ(estimateJoinCardinality(100000, 100000, keys, JoinKind::Left, JoinStrictness::Anti), 90000);
    EXPECT_EQ(estimateJoinCardinality(100000, 1000, keys, JoinKind::Right, JoinStrictness::Anti), 1);
}

TEST(JoinOrderBounds, KeyContainment)
{
    /// Fewer values on the other side lie within this side's values.
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(10000, 1000), 0.1);
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(10000, 1000, 150000), 0.1);
    /// At least as many values on the other side: all matched without a domain, their share of
    /// the domain with one (1363 filtered customers against the 100000 customers with orders, of
    /// 150000).
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(1000, 10000), 1.0);
    EXPECT_NEAR(*QueryPlanOptimizations::keyContainment(1363, 100000, 150000), 0.6667, 0.001);
    EXPECT_DOUBLE_EQ(*QueryPlanOptimizations::keyContainment(1363, 100000, 50000), 1.0);
    EXPECT_EQ(QueryPlanOptimizations::keyContainment(0, 1000), std::nullopt);
    EXPECT_EQ(QueryPlanOptimizations::keyContainment(1000, 0), std::nullopt);
}

TEST(JoinOrderBounds, DistinctValuesAfterFilter)
{
    /// Four rows per value and a third of the rows kept: a value survives unless all four of its
    /// rows are dropped, (1 - (2/3)^4) = 80 percent of the values.
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(10000, 40000, 13333), 8025);
    /// Unique values go with their rows.
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(40000, 40000, 13333), 13333);
    /// Nothing filtered, unknown NDV, and the bound by the rows kept.
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(10000, 40000, 40000), 10000);
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(0, 40000, 13333), 0);
    EXPECT_EQ(QueryPlanOptimizations::distinctValuesAfterFilter(10000, 40000, 5), 5);
}
