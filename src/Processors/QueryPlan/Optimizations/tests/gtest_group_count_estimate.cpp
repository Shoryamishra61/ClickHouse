#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

TEST(GroupCountEstimate, SharedFormula)
{
    /// No keys: one group.
    auto global = estimateGroupCount({}, 1000, 1000);
    EXPECT_EQ(global.estimated_rows, 1);
    EXPECT_EQ(global.max_rows, 1);

    /// The largest key NDV is the estimate; the bound stays the input's, the NDVs are estimates.
    auto two_keys = estimateGroupCount({100, 50}, 100000, 100000);
    EXPECT_EQ(two_keys.estimated_rows, 100);
    EXPECT_EQ(two_keys.max_rows, 100000);

    /// Both are capped by the rows.
    auto few_rows = estimateGroupCount({100, 50}, 60, 60);
    EXPECT_EQ(few_rows.estimated_rows, 60);
    EXPECT_EQ(few_rows.max_rows, 60);

    /// A key without an NDV leaves the estimate to the other keys and the bound to the input.
    auto one_unknown = estimateGroupCount({0, 50}, 100000, 100000);
    EXPECT_EQ(one_unknown.estimated_rows, 50);
    EXPECT_EQ(one_unknown.max_rows, 100000);

    /// No key NDV: unknown, bounded by the input.
    auto all_unknown = estimateGroupCount({0, 0}, 100000, 200000);
    EXPECT_EQ(all_unknown.estimated_rows, std::nullopt);
    EXPECT_EQ(all_unknown.max_rows, 200000);

    /// Unknown input rows leave the estimate uncapped and the bound unknown: an NDV undercount
    /// must not become a proven cap.
    auto unknown_rows = estimateGroupCount({100}, std::nullopt, std::nullopt);
    EXPECT_EQ(unknown_rows.estimated_rows, 100);
    EXPECT_EQ(unknown_rows.max_rows, std::nullopt);

    /// Huge NDVs saturate the estimate instead of wrapping.
    constexpr UInt64 big = UInt64(1) << 40;
    auto saturated = estimateGroupCount({big, big}, std::nullopt, std::nullopt, true);
    EXPECT_EQ(saturated.estimated_rows, UInt64(1) << 60);
    EXPECT_EQ(saturated.max_rows, std::nullopt);
}

TEST(GroupCountEstimate, DampedProduct)
{
    /// Sorted from the largest NDV, the keys take the exponents 1, 1/2, 1/4, ...; unknown keys
    /// contribute nothing; the rows cap the result.
    EXPECT_EQ(estimateGroupCount({100}, 100000, 100000, true).estimated_rows, 100);
    EXPECT_EQ(estimateGroupCount({50, 100}, 100000, 100000, true).estimated_rows, 707);
    EXPECT_EQ(estimateGroupCount({100, 50}, 100000, 100000, true).estimated_rows, 707);
    EXPECT_EQ(estimateGroupCount({16, 16, 16}, 100000, 100000, true).estimated_rows, 128);
    EXPECT_EQ(estimateGroupCount({0, 100, 50}, 100000, 100000, true).estimated_rows, 707);
    EXPECT_EQ(estimateGroupCount({100, 50}, 300, 300, true).estimated_rows, 300);
    EXPECT_EQ(estimateGroupCount({0, 0}, 100000, 100000, true).estimated_rows, std::nullopt);
    /// The bound does not depend on the model.
    EXPECT_EQ(estimateGroupCount({100, 50}, 100000, 100000, true).max_rows, 100000);
}
