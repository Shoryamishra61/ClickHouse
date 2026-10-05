#pragma once

#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Interpreters/ActionsDAG.h>

#include <algorithm>
#include <limits>

namespace DB
{

using PlanMemo = std::unordered_map<BitSet, DPJoinEntryPtr>;
/// What one join edge contributes to the selectivity of a join, computed once per edge.
struct EdgeSelectivity
{
    /// One over the larger key NDV; nothing for a non-equality edge, or when neither side has an
    /// NDV nor a row count to stand in for it.
    std::optional<double> selectivity;
    /// The equivalence classes of the two sides when they are plain input columns. The class term
    /// of the algorithms counts an edge between two class members, so the edge itself is skipped.
    const void * left_class = nullptr;
    const void * right_class = nullptr;
};
using SelectivityCache = std::unordered_map<JoinActionRef, EdgeSelectivity>;

/// NDV of a column as the join order optimizer knows it: from the relation's statistics, or from
/// the DP entry of a joined relation set (narrowed through joins). A column without an NDV (no
/// entry, or a zero NDV that only carries other column facts) counts its relation's rows as its
/// NDV: the key is taken as unique, the guess DuckDB and Orca make too. Zero when the rows are
/// unknown as well.
inline UInt64 getColumnStats(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    const BitSet & rels,
    const String & column_name)
{
    const auto & relation_stats = query_graph.relation_stats;
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        if (auto it = dp_table.find(rels); it != dp_table.end())
        {
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

inline const EdgeSelectivity & computeEdgeSelectivity(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const JoinActionRef & edge)
{
    auto [it, inserted] = expression_selectivity.try_emplace(edge);
    auto & result = it->second;
    if (!inserted)
        return result;

    auto [op, lhs, rhs] = edge.asBinaryPredicate();
    if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
        return result;

    result.selectivity = QueryPlanOptimizations::equalitySelectivity(
        getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName()),
        getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName()));

    auto input_class = [&](const JoinActionRef & ref) -> const void *
    {
        auto resolved = ref.resolveAliases();
        if (resolved.getNode()->type != ActionsDAG::ActionType::INPUT || !resolved.getSourceRelations().getSingleBit())
            return nullptr;
        return query_graph.column_equivalences.getClass(resolved).get();
    };
    result.left_class = input_class(lhs);
    result.right_class = input_class(rhs);
    return result;
}

/// Adds the selectivities of the equality edges to `selectivities`, one per distinct pair of
/// equivalence classes (the most selective when several edges relate the same pair), and skips
/// the edges between two class members, which the class term of the caller counts.
inline void collectEdgeSelectivities(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges,
    std::vector<double> & selectivities)
{
    std::vector<std::pair<const void *, const void *>> pairs;
    for (const auto * edge : edges)
    {
        const auto & edge_selectivity = computeEdgeSelectivity(query_graph, dp_table, expression_selectivity, *edge);
        if (!edge_selectivity.selectivity)
            continue;
        if (edge_selectivity.left_class && edge_selectivity.right_class)
            continue;

        /// An edge with a side outside every class is a pair of its own.
        std::pair<const void *, const void *> pair{edge_selectivity.left_class, edge_selectivity.right_class};
        if (!pair.first || !pair.second)
            pair = {edge, nullptr};
        auto found = std::find(pairs.begin(), pairs.end(), pair);
        if (found == pairs.end())
        {
            pairs.push_back(pair);
            selectivities.push_back(*edge_selectivity.selectivity);
        }
        else
        {
            auto & existing = selectivities[found - pairs.begin()];
            existing = std::min(existing, *edge_selectivity.selectivity);
        }
    }
}

/// Rows a join cannot exceed, from the bounds of its inputs: an inner or cross join at most the
/// product; an outer join at most one row per preserved row times the matches it can have, at
/// least one, plus every row of the other side for a full join; a semi or anti join at most its
/// preserved input. Unknown when a needed input bound is unknown. The arithmetic saturates at the
/// maximum `UInt64` instead of wrapping; a saturated bound is still a bound.
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

    switch (join_kind)
    {
        case JoinKind::Left:
            return saturating_mul(*left_max, std::max<UInt64>(1, *right_max));
        case JoinKind::Right:
            return saturating_mul(*right_max, std::max<UInt64>(1, *left_max));
        case JoinKind::Full:
            return saturating_add(saturating_mul(*left_max, std::max<UInt64>(1, *right_max)), *right_max);
        default:
            return saturating_mul(*left_max, *right_max);
    }
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
