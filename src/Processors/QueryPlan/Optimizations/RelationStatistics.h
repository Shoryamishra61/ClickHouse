#pragma once

#include <optional>
#include <unordered_map>

#include <Core/Joins.h>
#include <Processors/QueryPlan/CostEstimationInfo.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <base/types.h>

namespace DB
{

class ActionsDAG;

struct RelationStats
{
    std::optional<UInt64> estimated_rows = {};
    std::optional<Float64> avg_row_bytes = {};
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;
    bool imprecise_estimate = false;

    /// Diagnostic annotation of where `estimated_rows` came from; see `RowEstimateSource`.
    /// `NoSource` means the producer of the estimate did not track it; set it wherever it is known.
    RowEstimateSource source = RowEstimateSource::NoSource;
};

/// The plan node annotation of a relation estimate, for `EXPLAIN estimates = 1` and the
/// processors profile log. A missing row count stays missing.
inline CostEstimationInfo toCostEstimationInfo(const RelationStats & stats)
{
    return CostEstimationInfo{
        .rows = stats.estimated_rows ? std::optional<Float64>(Float64(*stats.estimated_rows)) : std::nullopt,
        .cost = std::nullopt,
        .source = stats.source,
        .imprecise = stats.imprecise_estimate};
}

namespace QueryPlanOptimizations
{

/// Propagate per-column statistics through `actions`, rekeying the map in place by output name.
/// An output inherits an input's stats when it is that input, an alias of it, or a deterministic
/// single-argument function of it (which cannot increase the distinct count).
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions);

/// Tighten equi-join key NDVs to their minimum, respecting which side each join kind preserves.
/// Anti joins and full joins leave both inputs unchanged.
void updateJoinKeyDistinctCounts(
    ColumnStats & left_stats,
    ColumnStats & right_stats,
    JoinKind kind,
    JoinStrictness strictness);

}

}
