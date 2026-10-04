#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>

#include <algorithm>
#include <limits>

namespace DB
{

using PlanMemo = std::unordered_map<BitSet, DPJoinEntryPtr>;
using SelectivityCache = std::unordered_map<JoinActionRef, double>;

inline size_t getColumnStats(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    const BitSet & rels,
    const String & column_name)
{
    const auto & relation_stats = query_graph.relation_stats;
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        /// Look up NDV from the dp_table entry's column_stats (propagated through joins).
        if (auto it = dp_table.find(rels); it != dp_table.end())
        {
            /// A zero NDV is an unknown NDV: the entry only carries other column facts.
            auto col_it = it->second->column_stats.find(column_name);
            if (col_it != it->second->column_stats.end() && col_it->second.num_distinct_values > 0)
                return col_it->second.num_distinct_values;
            return it->second->estimated_rows.value_or(0);
        }
        return 0;
    }

    const auto & relation_stat = relation_stats.at(rel_id.value());
    const auto & col_stats = relation_stat.column_stats;
    if (auto it = col_stats.find(column_name); it != col_stats.end() && it->second.num_distinct_values > 0)
        return it->second.num_distinct_values;
    return relation_stat.estimated_rows.value_or(0);
}

inline double computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge, 1.0);
    auto & selectivity = it->second;
    if (!inserted)
        return selectivity;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();

    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return 1.0;

    UInt64 lhs_ndv = getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName());
    UInt64 rhs_ndv = getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName());
    UInt64 max_ndv = std::max(lhs_ndv, rhs_ndv);
    if (max_ndv > 0)
        selectivity = std::min(selectivity, 1.0 / static_cast<double>(max_ndv));
    return selectivity;
}

inline double computeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges)
{
    double selectivity = 1.0;
    for (const auto & edge : edges)
        selectivity = std::min(selectivity, computeSelectivity(query_graph, dp_table, expression_selectivity, *edge));
    return selectivity;
}

/// Rows a join cannot exceed, from the bounds of its inputs: an inner or cross join at most the
/// product, an outer join at most the product plus the preserved rows that may stay unmatched,
/// a semi or anti join at most its preserved input. Unknown when a needed input bound is unknown.
/// The product saturates at the maximum `UInt64` instead of wrapping; a saturated bound is still
/// a bound.
inline std::optional<UInt64> estimateJoinRowsUpperBound(
    std::optional<UInt64> left_max, std::optional<UInt64> right_max, JoinKind join_kind, JoinStrictness strictness)
{
    constexpr UInt64 max = std::numeric_limits<UInt64>::max();
    auto saturating_mul = [](UInt64 a, UInt64 b) { UInt64 r; return __builtin_mul_overflow(a, b, &r) ? max : r; };
    auto saturating_add = [](UInt64 a, UInt64 b) { UInt64 r; return __builtin_add_overflow(a, b, &r) ? max : r; };

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
        return isRight(join_kind) ? right_max : left_max;

    if (!left_max || !right_max)
        return {};

    UInt64 bound = saturating_mul(*left_max, *right_max);
    if (join_kind == JoinKind::Left || join_kind == JoinKind::Full)
        bound = saturating_add(bound, *left_max);
    if (join_kind == JoinKind::Right || join_kind == JoinKind::Full)
        bound = saturating_add(bound, *right_max);
    return bound;
}

/// Rows the cost of an entry counts: the estimate when there is one, otherwise the upper bound,
/// otherwise the graph's fallback (the largest known relation). A missing estimate is never
/// counted as one row, which would make the plan that contains it look cheap.
inline double searchRows(const DPJoinEntryPtr & entry, const QueryGraph & query_graph)
{
    if (entry->estimated_rows)
        return static_cast<double>(*entry->estimated_rows);
    if (entry->max_rows)
        return static_cast<double>(*entry->max_rows);
    return static_cast<double>(query_graph.unknown_rows_fallback.value_or(1));
}

/// Single source of truth for join cardinality estimation. For outer joins the result is
/// floored by the number of rows from the preserved side(s), since those are always emitted
/// (NULL-padded when there is no match): LEFT keeps all left rows, RIGHT all right rows, FULL
/// at least the larger side.
///
/// Semi/anti joins are filters on their preserved side (LEFT preserves the left input, RIGHT the
/// right), so they never expand and must NOT be floored at the preserved side's row count. A
/// semijoin keeps the fraction of preserved rows that have >= 1 match; an antijoin keeps the rest.
/// Estimating them like outer joins (row count >= preserved side) is what makes the optimizer
/// refuse to push a selective semi/anti join down.
inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind,
    JoinStrictness strictness = JoinStrictness::All)
{
    /// An input known to be empty decides the result without the other input: an inner, cross or
    /// semi join is empty; an anti join and an outer join keep the preserved side.
    const bool left_empty = left_rows && *left_rows == 0;
    const bool right_empty = right_rows && *right_rows == 0;
    if (left_empty || right_empty)
    {
        const bool preserves_left = join_kind == JoinKind::Left || join_kind == JoinKind::Full
            || (strictness == JoinStrictness::Anti && !isRight(join_kind));
        const bool preserves_right = join_kind == JoinKind::Right || join_kind == JoinKind::Full
            || (strictness == JoinStrictness::Anti && isRight(join_kind));
        if (strictness == JoinStrictness::Anti)
            return isRight(join_kind) ? (left_empty ? right_rows : std::optional<UInt64>(0)) : (right_empty ? left_rows : std::optional<UInt64>(0));
        if (left_empty && right_empty)
            return 0;
        if (left_empty)
            return preserves_right ? right_rows : std::optional<UInt64>(0);
        return preserves_left ? left_rows : std::optional<UInt64>(0);
    }

    if (!left_rows || !right_rows)
        return {};

    double lhs = static_cast<double>(*left_rows);
    double rhs = static_cast<double>(*right_rows);

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        /// Preserved side is the left input for LEFT (and Inner/Cross, defensively), the right
        /// input for RIGHT; the other side is only probed for existence.
        const bool preserve_left = !isRight(join_kind);
        const double preserved = preserve_left ? lhs : rhs;
        const double other = preserve_left ? rhs : lhs;
        /// Expected fraction of preserved rows with at least one match. `selectivity` is ~1/ndv,
        /// so `selectivity * other` approximates matches per preserved row; cap at 1.
        const double match_fraction = std::min(1.0, selectivity * other);
        const double kept = (strictness == JoinStrictness::Semi)
            ? preserved * match_fraction
            : preserved * (1.0 - match_fraction);
        const double semi_rows = std::max(kept, 1.0);
        if (semi_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
            return std::numeric_limits<UInt64>::max();
        return static_cast<UInt64>(semi_rows);
    }

    double joined_rows = std::max(selectivity * lhs * rhs, 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    /// Every row of both sides appears at least once, matched or padded, so the result is at least
    /// the larger side. The sum of both sides is not a lower bound: matched rows appear once.
    if (join_kind == JoinKind::Full)
        joined_rows = std::max(joined_rows, std::max(lhs, rhs));

    /// Use >= to avoid undefined behavior when joined_rows is very close to max UInt64
    /// Due to floating point precision, a value slightly less than max when compared
    /// as double could still overflow when cast to UInt64
    if (joined_rows >= static_cast<double>(std::numeric_limits<UInt64>::max()))
        return std::numeric_limits<UInt64>::max();
    if (joined_rows < 1)
        return 1;
    return static_cast<UInt64>(joined_rows);
}

inline std::optional<UInt64> estimateJoinCardinality(
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    double selectivity,
    JoinKind join_kind = JoinKind::Inner)
{
    return estimateJoinCardinality(left->estimated_rows, right->estimated_rows, selectivity, join_kind);
}

inline double computeJoinCost(const QueryGraph & query_graph, const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, double selectivity)
{
    return left->cost + right->cost + selectivity * searchRows(left, query_graph) * searchRows(right, query_graph);
}

}
