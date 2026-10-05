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
    /// The equivalence classes of the two sides when they are plain input columns. The class term
    /// of the algorithms counts an edge between two class members, so the edge itself is skipped.
    const void * left_class = nullptr;
    const void * right_class = nullptr;
};
using SelectivityCache = std::unordered_map<JoinActionRef, EdgeSelectivity>;

/// The equality predicates between two relation sets, reduced per key to a selectivity and to the
/// fraction of each side's key values that the other side also has.
struct JoinKeyFactors
{
    std::vector<double> selectivities;
    std::vector<double> left_in_right;
    std::vector<double> right_in_left;

    void add(UInt64 left_distinct_values, UInt64 right_distinct_values, UInt64 domain_distinct_values)
    {
        selectivities.push_back(1.0 / static_cast<double>(std::max(left_distinct_values, right_distinct_values)));
        left_in_right.push_back(QueryPlanOptimizations::keyContainment(left_distinct_values, right_distinct_values, domain_distinct_values).value_or(1.0));
        right_in_left.push_back(QueryPlanOptimizations::keyContainment(right_distinct_values, left_distinct_values, domain_distinct_values).value_or(1.0));
    }
};

/// The equality predicates of a join combined: their selectivity, and the fraction of each side's
/// rows whose key values the other side also has (what a semi join keeps and an anti join drops).
struct JoinKeyEstimate
{
    double selectivity = 1.0;
    double left_match_fraction = 1.0;
    double right_match_fraction = 1.0;

    static JoinKeyEstimate combine(JoinKeyFactors factors, bool exponential_backoff)
    {
        return JoinKeyEstimate{
            .selectivity = QueryPlanOptimizations::combineKeySelectivities(std::move(factors.selectivities), exponential_backoff),
            .left_match_fraction = QueryPlanOptimizations::combineKeySelectivities(std::move(factors.left_in_right), exponential_backoff),
            .right_match_fraction = QueryPlanOptimizations::combineKeySelectivities(std::move(factors.right_in_left), exponential_backoff)};
    }
};

/// NDV of a column as the join order optimizer knows it: from the relation's statistics, or from
/// the DP entry of a joined relation set (narrowed through joins). A column without an NDV (no
/// entry, or a zero NDV that only carries other column facts) counts its relation's rows as its
/// NDV: the key is taken as unique on both sides, so a join on two such keys estimates the smaller
/// input (DuckDB and Orca guess a key-to-foreign-key join there, the larger input). Zero when the
/// rows are unknown as well.
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
    const auto & col_stats = query_graph.relation_stats.at(rel_id.value()).column_stats;
    if (auto it = col_stats.find(column_name); it != col_stats.end())
        return it->second.domain_distinct_values;
    return 0;
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

/// Adds the equality edges between `left` and `right` to `factors`, one factor per edge, and
/// skips the edges between two class members, which the class term of the caller counts.
inline void collectEdgeFactors(
    const QueryGraph & query_graph,
    const PlanMemo & dp_table,
    SelectivityCache & expression_selectivity,
    const std::vector<JoinActionRef *> & edges,
    const BitSet & left,
    JoinKeyFactors & factors)
{
    for (const auto * edge : edges)
    {
        const auto & edge_selectivity = computeEdgeSelectivity(query_graph, dp_table, expression_selectivity, *edge);
        if (!edge_selectivity.selectivity)
            continue;
        if (edge_selectivity.left_class && edge_selectivity.right_class)
            continue;

        const bool lhs_on_left = isSubsetOf(edge_selectivity.lhs_relations, left);
        const UInt64 left_distinct_values = lhs_on_left ? edge_selectivity.lhs_distinct_values : edge_selectivity.rhs_distinct_values;
        const UInt64 right_distinct_values = lhs_on_left ? edge_selectivity.rhs_distinct_values : edge_selectivity.lhs_distinct_values;
        factors.add(left_distinct_values, right_distinct_values, edge_selectivity.domain_distinct_values);
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
    constexpr UInt64 max = std::numeric_limits<UInt64>::max();
    auto saturating_mul = [](UInt64 a, UInt64 b) { UInt64 r; return __builtin_mul_overflow(a, b, &r) ? max : r; };
    auto saturating_add = [](UInt64 a, UInt64 b) { UInt64 r; return __builtin_add_overflow(a, b, &r) ? max : r; };
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

    if (join_kind == JoinKind::Inner || join_kind == JoinKind::Cross || join_kind == JoinKind::Comma)
        if (left_empty || right_empty)
            return 0;

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

/// Single source of truth for join cardinality estimation, for every planner. For outer joins the
/// result is floored by the number of rows from the preserved side(s), since those are always
/// emitted (NULL-padded when there is no match): LEFT keeps all left rows, RIGHT all right rows,
/// FULL at least the larger side.
///
/// Semi/anti joins are filters on their preserved side (LEFT preserves the left input, RIGHT the
/// right), so they never expand and must NOT be floored at the preserved side's row count. A
/// semijoin keeps the fraction of preserved rows that have >= 1 match; an antijoin keeps the rest.
/// Estimating them like outer joins (row count >= preserved side) is what makes the optimizer
/// refuse to push a selective semi/anti join down.
/// `preserved_match_fraction` is the fraction of the preserved side's rows whose key values the
/// other side has, for a semi or anti join; without it the other side's rows per preserved key
/// stand in, which overstates the matches when the other side repeats its keys.
///
/// A paste join pairs rows by position: the shorter side. An `ANY` or `ASOF` join emits each row
/// of one side at most once: exactly that side for an outer join, at most that side for an inner
/// join.
inline std::optional<UInt64> estimateJoinCardinality(
    std::optional<UInt64> left_rows,
    std::optional<UInt64> right_rows,
    double selectivity,
    JoinKind join_kind,
    JoinStrictness strictness = JoinStrictness::All,
    std::optional<double> preserved_match_fraction = {})
{
    /// A paste join has no keys: its result follows the inputs alone, as the bound does.
    if (join_kind == JoinKind::Paste)
        return estimateJoinRowsUpperBound(left_rows, right_rows, join_kind, strictness);

    /// An input known to be empty decides the result without the other input: an inner, cross or
    /// semi join is empty; an anti join and an outer join keep the preserved side.
    const bool left_empty = left_rows && *left_rows == 0;
    const bool right_empty = right_rows && *right_rows == 0;
    if (left_empty || right_empty)
    {
        if (strictness == JoinStrictness::Semi)
            return 0;
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

    if (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny || strictness == JoinStrictness::Asof)
    {
        const bool single_side_is_left = *singleMatchSideIsLeft(join_kind, strictness);
        const double single_side = single_side_is_left ? lhs : rhs;
        /// An outer join emits every row of that side once; an inner join only the matched ones.
        if (join_kind != JoinKind::Inner)
            return static_cast<UInt64>(single_side);
        return static_cast<UInt64>(std::max(std::min(selectivity * lhs * rhs, single_side), 1.0));
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

}
