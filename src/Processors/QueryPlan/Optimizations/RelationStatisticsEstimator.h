#pragma once

#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{
class AggregatingStep;
}

namespace DB::QueryPlanOptimizations
{

/// Estimate the number of rows and per-column statistics of the relation produced by the subtree
/// rooted at `node`, keyed by the subtree's output column names. `filter` is an optional predicate
/// over these columns to account for.
RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter = nullptr);

/// Rows of an aggregation or a `DISTINCT` over `keys`, by the shared group count formula; the output
/// columns are the keys with their NDVs, zero for a key without one.
RelationStats estimateGroupStats(const Names & keys, const RelationStats & input_stats);

/// The same for an aggregating step; `GROUPING SETS` count the groups of every set.
RelationStats estimateAggregatingStepStats(const AggregatingStep & aggregating_step, const RelationStats & input_stats);

}
