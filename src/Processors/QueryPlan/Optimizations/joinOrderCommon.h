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
    /// The key NDVs of the two sides of the predicate (zero = unknown), the key's domain, and the
    /// relations of the left side, to tell which side of a join split each belongs to.
    UInt64 lhs_distinct_values = 0;
    UInt64 rhs_distinct_values = 0;
    UInt64 domain_distinct_values = 0;
    BitSet lhs_relations;
};
using SelectivityCache = std::unordered_map<JoinActionRef, EdgeSelectivity>;

/// The equality predicates of a join combined: the most selective key decides the selectivity,
/// and the smallest fraction of each side's rows whose key values the other side also has decides
/// what a semi join keeps and an anti join drops. No predicates: everything matches.
struct JoinKeyEstimate
{
    double selectivity = 1.0;
    double left_match_fraction = 1.0;
    double right_match_fraction = 1.0;

    /// A key pair by its two NDVs (zero = unknown) and the key's domain; nothing when both are unknown.
    void add(UInt64 left_distinct_values, UInt64 right_distinct_values, UInt64 domain_distinct_values)
    {
        if (std::max(left_distinct_values, right_distinct_values) == 0)
            return;
        selectivity = std::min(selectivity, 1.0 / static_cast<double>(std::max(left_distinct_values, right_distinct_values)));
        left_match_fraction = std::min(left_match_fraction,
            QueryPlanOptimizations::keyContainment(left_distinct_values, right_distinct_values, domain_distinct_values).value_or(1.0));
        right_match_fraction = std::min(right_match_fraction,
            QueryPlanOptimizations::keyContainment(right_distinct_values, left_distinct_values, domain_distinct_values).value_or(1.0));
    }
};

/// NDV of a column as the join order optimizer knows it: from the relation's statistics, or from
/// the DP entry of a joined relation set (narrowed through joins). A column without an NDV counts
/// its relation's rows, see `keyDistinctValuesOrRows`. Zero when the rows are unknown as well.
inline UInt64 getColumnStats(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    const BitSet & rels,
    const String & column_name)
{
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
    return QueryPlanOptimizations::keyDistinctValuesOrRows(query_graph.relation_stats.at(rel_id.value()), column_name);
}

/// Domain of a column as the join order optimizer knows it; zero when unknown.
inline UInt64 getColumnDomain(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    const BitSet & rels,
    const String & column_name)
{
    auto rel_id = rels.getSingleBit();
    if (!rel_id.has_value())
    {
        if (auto it = dp_table.find(rels); it != dp_table.end())
            if (auto col_it = it->second->column_stats.find(column_name); col_it != it->second->column_stats.end())
                return col_it->second.domain_distinct_values;
        return 0;
    }
    return QueryPlanOptimizations::keyDomain(query_graph.relation_stats.at(rel_id.value()), column_name);
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

    result.lhs_distinct_values = getColumnStats(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName());
    result.rhs_distinct_values = getColumnStats(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName());
    result.domain_distinct_values = std::max(
        getColumnDomain(query_graph, dp_table, lhs.getSourceRelations(), lhs.getColumnName()),
        getColumnDomain(query_graph, dp_table, rhs.getSourceRelations(), rhs.getColumnName()));
    result.lhs_relations = lhs.getSourceRelations();
    result.selectivity = QueryPlanOptimizations::equalitySelectivity(result.lhs_distinct_values, result.rhs_distinct_values);
    return result;
}

/// Adds the equality edges between `left` and `right` to `keys`.
inline void collectEdgeKeys(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges,
    const BitSet & left,
    JoinKeyEstimate & keys)
{
    for (const auto * edge : edges)
    {
        const auto & edge_selectivity = computeEdgeSelectivity(query_graph, dp_table, expression_selectivity, *edge);
        if (!edge_selectivity.selectivity)
            continue;
        const bool lhs_on_left = isSubsetOf(edge_selectivity.lhs_relations, left);
        const UInt64 left_distinct_values = lhs_on_left ? edge_selectivity.lhs_distinct_values : edge_selectivity.rhs_distinct_values;
        const UInt64 right_distinct_values = lhs_on_left ? edge_selectivity.rhs_distinct_values : edge_selectivity.lhs_distinct_values;
        keys.add(left_distinct_values, right_distinct_values, edge_selectivity.domain_distinct_values);
    }
}

/// The input whose rows a join emits at most once each: the preserved side of a semi or anti
/// join, the side that takes one match per row in an `ANY` or `ASOF` join (the left side, or
/// the right side of a `RIGHT` join and of a right-any join). None for the other strictnesses.
inline std::optional<bool> singleMatchSideIsLeft(JoinKind join_kind, JoinStrictness strictness)
{
    switch (strictness)
    {
        case JoinStrictness::Semi:
        case JoinStrictness::Anti:
        case JoinStrictness::Any:
        case JoinStrictness::Asof:
            return !isRight(join_kind);
        case JoinStrictness::RightAny:
            return join_kind == JoinKind::Left;
        default:
            return {};
    }
}

/// Rows a join cannot exceed, from the bounds of its inputs: a paste join the shorter side; a semi,
/// anti, any or asof join the side it emits at most once (empty for a semi join and an inner any
/// or asof join when the other side is); an inner or cross join the product, empty when a side is;
/// an outer join one row per preserved row times the matches it can have, at least one, plus every
/// row of the other side for a full join. Unknown when a needed input bound is; the arithmetic
/// saturates instead of wrapping, and a saturated bound is still a bound.
inline std::optional<UInt64> estimateJoinRowsUpperBound(
    std::optional<UInt64> left_max, std::optional<UInt64> right_max, JoinKind join_kind, JoinStrictness strictness)
{
    const bool left_empty = left_max && *left_max == 0;
    const bool right_empty = right_max && *right_max == 0;

    if (join_kind == JoinKind::Paste)
    {
        if (left_max && right_max)
            return std::min(*left_max, *right_max);
        return left_max ? left_max : right_max;
    }

    if (const auto single_side_is_left = singleMatchSideIsLeft(join_kind, strictness))
    {
        const auto & single_max = *single_side_is_left ? left_max : right_max;
        const bool other_empty = *single_side_is_left ? right_empty : left_empty;
        /// Only an anti join and an outer any or asof join keep rows the other side does not match.
        const bool keeps_unmatched = strictness == JoinStrictness::Anti
            || (strictness != JoinStrictness::Semi && join_kind != JoinKind::Inner);
        if (other_empty && !keeps_unmatched)
            return 0;
        return single_max;
    }

    /// An empty side decides on its own: an inner or cross join is empty, an outer join keeps the
    /// other side when that is the preserved one.
    if (left_empty || right_empty)
    {
        if (join_kind == JoinKind::Left)
            return left_empty ? std::optional<UInt64>(0) : left_max;
        if (join_kind == JoinKind::Right)
            return right_empty ? std::optional<UInt64>(0) : right_max;
        if (join_kind == JoinKind::Full)
            return left_empty ? right_max : left_max;
        return 0;
    }

    if (!left_max || !right_max)
        return {};

    switch (join_kind)
    {
        case JoinKind::Left:
            return saturatingMul(*left_max, std::max<UInt64>(1, *right_max));
        case JoinKind::Right:
            return saturatingMul(*right_max, std::max<UInt64>(1, *left_max));
        case JoinKind::Full:
            return saturatingAdd(saturatingMul(*left_max, std::max<UInt64>(1, *right_max)), *right_max);
        default:
            return saturatingMul(*left_max, *right_max);
    }
}

/// A relation read through a prepared join storage (a `Join` engine table, a dictionary). It is
/// probed by its key instead of scanned, so its size does not multiply the join, and its physical
/// join step accepts the key equalities only. One row as its search value joins it early, while
/// the other side still relates to it through the key alone.
inline bool isPreparedStorageRelation(const BitSet & relations, const QueryGraph & query_graph)
{
    auto relation = relations.getSingleBit();
    return relation && query_graph.prepared_storage_relations.test(*relation);
}

/// Rows the cost of an entry counts: the estimate when there is one, otherwise the upper bound,
/// otherwise the graph's fallback (the largest known relation). A missing estimate is never
/// counted as one row, which would make the plan that contains it look cheap; the exception is a
/// prepared storage relation, see `isPreparedStorageRelation`.
inline double searchRows(const DPJoinEntryPtr & entry, const QueryGraph & query_graph)
{
    if (entry->estimated_rows)
        return static_cast<double>(*entry->estimated_rows);
    if (entry->max_rows)
        return static_cast<double>(*entry->max_rows);
    if (isPreparedStorageRelation(entry->relations, query_graph))
        return 1;
    return static_cast<double>(query_graph.unknown_rows_fallback.value_or(1));
}

/// Join cardinality for every planner. An inner join is `selectivity * left * right`, at least one
/// row. An outer join is at least its preserved side(s): every preserved row is emitted, matched
/// or padded. A semi or anti join is a filter on its preserved side: the fraction of its rows
/// with a match, or without one; `preserved_match_fraction` gives that fraction from the key
/// values, without it the other side's rows per preserved key stand in. A paste join pairs rows by
/// position: the shorter side. An `ANY` or `ASOF` join emits each row of one side at most once:
/// exactly that side for an outer join, at most that side for an inner join. An empty side
/// decides the result alone, as the bound does.
inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind,
    JoinStrictness strictness = JoinStrictness::All,
    std::optional<double> preserved_match_fraction = {})
{
    const bool left_empty = left_rows && *left_rows == 0;
    const bool right_empty = right_rows && *right_rows == 0;
    if (join_kind == JoinKind::Paste || left_empty || right_empty)
        return estimateJoinRowsUpperBound(left_rows, right_rows, join_kind, strictness);

    if (!left_rows || !right_rows)
        return {};

    double lhs = static_cast<double>(*left_rows);
    double rhs = static_cast<double>(*right_rows);

    if (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny || strictness == JoinStrictness::Asof)
    {
        const bool single_side_is_left = *singleMatchSideIsLeft(join_kind, strictness);
        const double single_side = single_side_is_left ? lhs : rhs;
        /// An outer join emits every row of that side once; an inner join only the matched ones.
        if (join_kind != JoinKind::Inner)
            return roundToRowCount(single_side);
        return roundToRowCount(std::max(std::min(selectivity * lhs * rhs, single_side), 1.0));
    }

    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
    {
        /// Preserved side is the left input for LEFT (and Inner/Cross, defensively), the right
        /// input for RIGHT; the other side is only probed for existence.
        const bool preserve_left = !isRight(join_kind);
        const double preserved = preserve_left ? lhs : rhs;
        const double other = preserve_left ? rhs : lhs;
        /// Expected fraction of preserved rows with at least one match. Without the key containment
        /// `selectivity * other` approximates matches per preserved row; cap at 1.
        const double match_fraction = preserved_match_fraction.value_or(std::min(1.0, selectivity * other));
        const double kept = (strictness == JoinStrictness::Semi)
            ? preserved * match_fraction
            : preserved * (1.0 - match_fraction);
        return roundToRowCount(std::max(kept, 1.0));
    }

    double joined_rows = std::max(selectivity * lhs * rhs, 1.0);

    if (join_kind == JoinKind::Left)
        joined_rows = std::max(joined_rows, lhs);
    if (join_kind == JoinKind::Right)
        joined_rows = std::max(joined_rows, rhs);
    /// Every row of both sides appears at least once, matched or padded, so the result is at least
    /// the larger side. The sum of both sides is not a lower bound: matched rows appear once.
    if (join_kind == JoinKind::Full)
        joined_rows = std::max({joined_rows, lhs, rhs});

    return roundToRowCount(std::max(joined_rows, 1.0));
}

inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    const JoinKeyEstimate & keys,
    JoinKind join_kind,
    JoinStrictness strictness)
{
    const double preserved_match_fraction = isRight(join_kind) ? keys.right_match_fraction : keys.left_match_fraction;
    return estimateJoinCardinality(left_rows, right_rows, keys.selectivity, join_kind, strictness, preserved_match_fraction);
}

inline std::optional<UInt64> estimateJoinCardinality(
    const DPJoinEntryPtr & left,
    const DPJoinEntryPtr & right,
    const JoinKeyEstimate & keys,
    JoinKind join_kind,
    JoinStrictness strictness)
{
    return estimateJoinCardinality(left->estimated_rows, right->estimated_rows, keys, join_kind, strictness);
}

inline double computeJoinCost(const QueryGraph & query_graph, const DPJoinEntryPtr & left, const DPJoinEntryPtr & right, double selectivity)
{
    return left->cost + right->cost + selectivity * searchRows(left, query_graph) * searchRows(right, query_graph);
}

/// The estimate of a join of two relations, by the shared formulas: the key selectivity and
/// containment from the relations' column statistics, the rows and the bound by the join kind
/// and strictness, and the output column statistics with the key NDVs narrowed by the join.
struct JoinEstimate
{
    std::optional<UInt64> rows;
    std::optional<UInt64> max_rows;
    double selectivity = 1.0;
    std::unordered_map<String, ColumnStats> column_stats;
};

inline JoinEstimate estimateJoin(const RelationStats & left, const RelationStats & right, const JoinOperator & join_operator)
{
    auto left_entry = std::make_shared<DPJoinEntry>(0, left);
    auto right_entry = std::make_shared<DPJoinEntry>(1, right);

    JoinKeyEstimate keys;
    for (const auto & predicate : join_operator.expression)
    {
        auto [op, lhs, rhs] = predicate.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals && op != JoinConditionOperator::NullSafeEquals)
            continue;
        if (lhs.fromRight() && rhs.fromLeft())
            std::swap(lhs, rhs);
        if (!lhs.fromLeft() || !rhs.fromRight())
            continue;
        keys.add(
            QueryPlanOptimizations::keyDistinctValuesOrRows(left, lhs.getColumnName()),
            QueryPlanOptimizations::keyDistinctValuesOrRows(right, rhs.getColumnName()),
            std::max(QueryPlanOptimizations::keyDomain(left, lhs.getColumnName()), QueryPlanOptimizations::keyDomain(right, rhs.getColumnName())));
    }

    const auto rows = estimateJoinCardinality(left_entry, right_entry, keys, join_operator.kind, join_operator.strictness);
    DPJoinEntry joined(left_entry, right_entry, /*cost*/ 0.0, keys.selectivity, rows, join_operator);
    return JoinEstimate{
        .rows = joined.estimated_rows,
        .max_rows = joined.max_rows,
        .selectivity = keys.selectivity,
        .column_stats = std::move(joined.column_stats)};
}

}
