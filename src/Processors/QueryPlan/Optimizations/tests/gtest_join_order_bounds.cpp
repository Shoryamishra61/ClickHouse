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
