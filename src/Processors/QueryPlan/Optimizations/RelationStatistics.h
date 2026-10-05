#pragma once

#include <optional>
#include <unordered_map>
#include <vector>

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
    /// Rows the relation cannot exceed: the selected rows of a read, carried through the steps above
    /// it by their semantics (a filter keeps the bound, a limit lowers it). Known for many relations
    /// whose point estimate is unknown, so a missing point does not have to be costed as one row.
    std::optional<UInt64> max_rows = {};
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

/// Distinct value combinations of a set of keys among `rows` rows: the rows of an aggregation or a
/// `DISTINCT` over them.
struct GroupCountEstimate
{
    /// Capped by the rows; unknown when no key has an NDV. Every planner uses this one formula, so
    /// an aggregation is the same relation wherever it is estimated. By default the largest known
    /// key NDV, a lower bound on the groups. With `damped_product` the keys count as partially
    /// correlated: sorted from the largest NDV, they take the exponents 1, 1/2, 1/4, ... .
    std::optional<UInt64> estimated_rows;
    /// The product of the key NDVs when all are known, else the rows the input cannot exceed.
    std::optional<UInt64> max_rows;
};

/// `key_distinct_values` holds one NDV per key, zero for a key without one.
GroupCountEstimate estimateGroupCount(
    const std::vector<UInt64> & key_distinct_values, std::optional<UInt64> rows, std::optional<UInt64> max_rows, bool damped_product = false);

/// Selectivity of one equality predicate from the NDVs of its sides (zero = unknown): one over
/// the larger known NDV, nothing when neither is known.
std::optional<double> equalitySelectivity(UInt64 left_distinct_values, UInt64 right_distinct_values);

/// Distinct values of a column left by a filter that keeps `rows_after` of `rows_before` rows, when
/// the column's `distinct_values` are spread evenly over the rows: a value survives when any of its
/// rows does. Zero when the NDV is unknown; never above `rows_after`.
UInt64 distinctValuesAfterFilter(UInt64 distinct_values, UInt64 rows_before, UInt64 rows_after);

/// Fraction of one side's key values that the other side also has, from the two NDVs (zero =
/// unknown) and the key's domain. Fewer values on the other side are taken to lie within this
/// side's (a set derived from this side, a dimension's filtered keys); at least as many meet this
/// side's values in proportion to their share of the domain, or all of them when the domain is
/// unknown. Nothing when either NDV is unknown.
std::optional<double> keyContainment(UInt64 side_distinct_values, UInt64 other_distinct_values, UInt64 domain_distinct_values = 0);

/// Selectivity of a conjunction of equality predicates from the selectivity of each. Without
/// `exponential_backoff` the most selective decides alone. With it the keys count as partially
/// correlated: sorted from the most selective, they take the exponents 1, 1/2, 1/4 and 1/8, and
/// further keys are dropped. No predicates: 1.
double combineKeySelectivities(std::vector<double> selectivities, bool exponential_backoff);

/// Tighten equi-join key NDVs to their minimum, respecting which side each join kind preserves.
/// Anti joins and full joins leave both inputs unchanged. A zero NDV is unknown: it is bounded by
/// the other side's NDV when the join filters its side, and never narrows the other side.
void updateJoinKeyDistinctCounts(
    ColumnStats & left_stats,
    ColumnStats & right_stats,
    JoinKind kind,
    JoinStrictness strictness);

}

}
