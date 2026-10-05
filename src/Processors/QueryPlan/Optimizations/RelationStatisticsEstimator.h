#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{
struct QueryPlanOptimizationSettings;
}

namespace DB::QueryPlanOptimizations
{

/// The query settings the estimate depends on.
struct RelationEstimationSettings
{
    RelationEstimationSettings() = default;
    explicit RelationEstimationSettings(const QueryPlanOptimizationSettings & optimization_settings);

    /// See `estimateGroupCount`.
    bool group_count_damped_product = false;
};

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for.
RelationStats estimateReadRowsCount(
    QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr, const RelationEstimationSettings & settings = {});

}
