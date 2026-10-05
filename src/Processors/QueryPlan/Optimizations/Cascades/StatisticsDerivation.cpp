#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerDefaults.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/IStorage.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool allow_statistics_optimize;
}

void StatisticsDerivation::deriveStatistics(GroupId group_id)
{
    auto group = memo.getGroup(group_id);

    if (group->statistics.has_value())
        return;

    /// Pick the first logical expression to derive statistics from
    /// (all logical expressions in a group represent the same logical result)
    if (group->logical_expressions.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Group #{} has no logical expressions to derive statistics from", group_id);

    auto expression = group->logical_expressions.front();
    const IQueryPlanStep * plan_step = expression->getQueryPlanStep();

    /// Ensure all input groups have statistics first (bottom-up derivation)
    for (const auto & input : expression->inputs)
    {
        auto input_group = memo.getGroup(input.group_id);
        if (!input_group->statistics.has_value())
            deriveStatistics(input.group_id);
    }

    /// Returns the statistics of the expression's input #index; throws if the expression has
    /// fewer inputs than its step type implies.
    auto input_statistics = [&](size_t index) -> const ExpressionStatistics &
    {
        if (index >= expression->inputs.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression '{}' has {} inputs, statistics derivation needs input #{}",
                expression->getName(), expression->inputs.size(), index);
        return *memo.getGroup(expression->inputs[index].group_id)->statistics;
    };

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(plan_step))
    {
        group->statistics = deriveJoinStatistics(*join_step, input_statistics(0), input_statistics(1));
    }
    else if (const auto * read_step = typeid_cast<const ReadFromMergeTree *>(plan_step))
    {
        group->statistics = deriveReadStatistics(*read_step);
    }
    else if (const auto * filter_step = typeid_cast<const FilterStep *>(plan_step))
    {
        group->statistics = deriveFilterStatistics(*filter_step, input_statistics(0));
    }
    else if (const auto * expression_step = typeid_cast<const ExpressionStep *>(plan_step))
    {
        group->statistics = deriveExpressionStatistics(*expression_step, input_statistics(0));
    }
    else if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(plan_step))
    {
        group->statistics = deriveAggregatingStatistics(*aggregating_step, input_statistics(0));
    }
    else if (const auto * sorting_step = typeid_cast<const SortingStep *>(plan_step))
    {
        group->statistics = deriveSortingStatistics(*sorting_step, input_statistics(0));
    }
    else if (const auto * limit_step = typeid_cast<const LimitStep *>(plan_step))
    {
        group->statistics = deriveLimitStatistics(*limit_step, input_statistics(0));
    }
    else if (const auto * distinct_step = typeid_cast<const DistinctStep *>(plan_step); distinct_step && !distinct_step->isPreliminary())
    {
        group->statistics = deriveDistinctStatistics(*distinct_step, input_statistics(0));
    }
    else if (const auto * union_step = typeid_cast<const UnionStep *>(plan_step))
    {
        group->statistics = deriveUnionStatistics(*union_step, [&](size_t index) -> const ExpressionStatistics & { return input_statistics(index); }, expression->inputs.size());
    }
    else if (const auto * intersect_or_except_step = typeid_cast<const IntersectOrExceptStep *>(plan_step))
    {
        /// The output reuses the first input's header and so its column statistics. Only
        /// `INTERSECT ALL` is bounded by the smallest input; the `DISTINCT` variants keep every
        /// matching duplicate of the first input at this step (a separate `Distinct` above
        /// deduplicates), and `EXCEPT` keeps at most the first input.
        ExpressionStatistics result = input_statistics(0);
        const auto op = intersect_or_except_step->getOperator();
        if (op == IntersectOrExceptStep::Operator::INTERSECT_ALL
            || op == IntersectOrExceptStep::Operator::INTERSECT_DISTINCT)
        {
            /// Every output row also matches a row of each other input, so a column's NDV is
            /// bounded by every input's NDV at the same position. Without this the `Distinct`
            /// above would estimate from the first input's NDVs alone.
            const auto & input_headers = intersect_or_except_step->getInputHeaders();
            for (size_t input_index = 1; input_index < expression->inputs.size(); ++input_index)
            {
                const auto & other = input_statistics(input_index);
                if (op == IntersectOrExceptStep::Operator::INTERSECT_ALL)
                {
                    result.estimated_row_count = std::min(result.estimated_row_count, other.estimated_row_count);
                    result.max_row_count = std::min(result.max_row_count, other.max_row_count);
                }
                /// The distinct output rows cannot exceed any input, with or without NDVs.
                result.estimated_distinct_bound = std::min(
                    {result.estimated_distinct_bound, other.estimated_row_count, other.estimated_distinct_bound});
                for (size_t position = 0; position < input_headers.at(0)->columns(); ++position)
                {
                    auto output_column = result.column_statistics.find(input_headers.at(0)->getByPosition(position).name);
                    if (output_column == result.column_statistics.end())
                        continue;
                    auto other_column = other.column_statistics.find(input_headers.at(input_index)->getByPosition(position).name);
                    if (other_column == other.column_statistics.end())
                        continue;
                    output_column->second.num_distinct_values
                        = std::min(output_column->second.num_distinct_values, other_column->second.num_distinct_values);
                }
            }
            /// Without the clamp a row-count reduction could leave a column NDV above the row count.
            for (auto & [column_name, column_stats] : result.column_statistics)
                column_stats.num_distinct_values = std::min(column_stats.num_distinct_values,
                    static_cast<UInt64>(std::max(result.estimated_row_count, 1.0)));
        }
        result.min_row_count = 0;
        group->statistics = std::move(result);
    }
    else if (!expression->inputs.empty())
    {
        /// By default take statistics from the first input
        group->statistics = input_statistics(0);
    }
    else
    {
        /// A source the estimator does not know: no estimate, and no bound. The search value is
        /// the largest known leaf of the query, so the unknown source is never costed as empty.
        ExpressionStatistics unknown;
        unknown.rows_unknown = true;
        unknown.estimated_row_count = largestKnownLeafRowCount().value_or(CascadesDefaults::DEFAULT_UNKNOWN_READ_ROWS);
        group->statistics = std::move(unknown);
    }

    /// A result derived from an input without an estimate has none either, unless an input is proven
    /// empty, which decides the result on its own.
    bool input_proven_empty = false;
    bool input_unknown = false;
    for (const auto & input : expression->inputs)
    {
        const auto & input_stats = *memo.getGroup(input.group_id)->statistics;
        input_proven_empty |= input_stats.max_row_count == 0;
        input_unknown |= input_stats.rows_unknown;
    }
    if (input_unknown && !input_proven_empty)
        group->statistics->rows_unknown = true;

    LOG_TEST(log, "Derived statistics for group #{}:\n{}",
        group_id, group->statistics->dump());
}

std::optional<Float64> StatisticsDerivation::largestKnownLeafRowCount() const
{
    std::optional<Float64> largest;
    for (GroupId group_id = 0; group_id < memo.getGroupCount(); ++group_id)
    {
        const auto group = memo.getGroup(group_id);
        if (!group->statistics || group->statistics->rows_unknown || group->logical_expressions.empty()
            || !group->logical_expressions.front()->inputs.empty())
            continue;
        if (!largest || group->statistics->estimated_row_count > *largest)
            largest = group->statistics->estimated_row_count;
    }
    return largest;
}

Float64 clampJoinRowCount(JoinKind kind, JoinStrictness strictness, Float64 base, Float64 left, Float64 right)
{
    /// Paste is position-wise (no keys): one output row per aligned pair.
    if (kind == JoinKind::Paste)
        return std::min(left, right);
    /// Cross/comma product keeps the multiplicative estimate.
    if (kind == JoinKind::Cross || kind == JoinKind::Comma)
        return base;

    /// A full join keeps unmatched rows from both sides, so it is at least the larger side regardless
    /// of strictness. (Checked before the semi/any bounds, which apply only to one preserved side.)
    if (kind == JoinKind::Full)
        return std::max({base, left, right});

    const Float64 preserved = (kind == JoinKind::Right) ? right : left;

    /// Semi/anti filter the preserved side, so the output cannot exceed it.
    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
        return std::min(base, preserved);

    if (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny || strictness == JoinStrictness::Asof)
    {
        /// Outer any/asof keep every preserved-side row (one match each).
        if (kind == JoinKind::Left || kind == JoinKind::Right)
            return preserved;
        /// Inner asof keeps one nearest match per left row, so it is bounded by the left side.
        if (strictness == JoinStrictness::Asof)
            return std::min(base, left);
        /// Inner any emits at most one row per matching key, so it cannot exceed either side.
        return std::min({base, left, right});
    }

    /// Strictness All/Unspecified: an outer join keeps every preserved-side row, so the output is at
    /// least the preserved side.
    if (kind == JoinKind::Left)
        return std::max(base, left);
    if (kind == JoinKind::Right)
        return std::max(base, right);
    return base;
}

Float64 clampJoinMaxRowCount(JoinKind kind, JoinStrictness strictness, Float64 product, Float64 left, Float64 right)
{
    /// Position-wise: exactly the shorter side.
    if (kind == JoinKind::Paste)
        return std::min(left, right);
    /// A full join can emit every row from both sides when the keys are disjoint.
    if (kind == JoinKind::Full)
        return std::max(product, left + right);

    const Float64 preserved = (kind == JoinKind::Right) ? right : left;
    const bool reduces_to_preserved =
        strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti
        || strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny
        || strictness == JoinStrictness::Asof;

    /// Semi/anti/any keep at most one row per preserved-side row (anti can keep the whole preserved
    /// side even when the other side is empty), so the preserved side is the upper bound.
    if (reduces_to_preserved && (kind == JoinKind::Left || kind == JoinKind::Right))
        return preserved;

    /// An outer join with matches can multiply up to the product but never drops below the preserved
    /// side (unmatched rows are kept).
    if (kind == JoinKind::Left)
        return std::max(product, left);
    if (kind == JoinKind::Right)
        return std::max(product, right);
    /// Inner (any strictness), cross, comma: the product is a valid upper bound.
    return product;
}

ExpressionStatistics StatisticsDerivation::deriveJoinStatistics(
    const JoinStepLogical & join_step,
    const ExpressionStatistics & left_statistics,
    const ExpressionStatistics & right_statistics)
{
    ExpressionStatistics statistics;
    statistics.min_row_count = 0;
    statistics.max_row_count = left_statistics.max_row_count * right_statistics.max_row_count;

    statistics.column_statistics.insert(left_statistics.column_statistics.begin(), left_statistics.column_statistics.end());
    statistics.column_statistics.insert(right_statistics.column_statistics.begin(), right_statistics.column_statistics.end());

    /// One selectivity per distinct pair of key classes: a predicate between columns that other
    /// predicates already relate (through the inputs' equivalences) restricts nothing more. The
    /// class of a column is named by its first member.
    auto class_representative = [](const EquivalenceClasses<String> & classes, const String & column) -> String
    {
        auto equivalence_class = classes.getClass(column);
        return equivalence_class && !equivalence_class->empty() ? equivalence_class->front() : column;
    };
    std::vector<std::pair<String, String>> key_pairs;
    std::vector<double> key_selectivities;

    /// Equality key pairs, for the output column equivalences.
    std::vector<std::pair<String, String>> equi_pairs;
    const auto & join_operator = join_step.getJoinOperator();

    for (const auto & predicate_expression : join_operator.expression)
    {
        const auto & predicate = predicate_expression.asBinaryPredicate();
        auto left_column_actions = get<1>(predicate);
        auto right_column_actions = get<2>(predicate);

        if (get<0>(predicate) != JoinConditionOperator::Equals || !left_column_actions || !right_column_actions)
        {
            /// TODO: add support for non-equality operators
            LOG_TEST(log, "Skipping predicate '{}'", predicate_expression.dump());
            continue;
        }

        if (left_column_actions.fromRight() && right_column_actions.fromLeft())
            std::swap(left_column_actions, right_column_actions);
        const auto & left_column = left_column_actions.getColumnName();
        const auto & right_column = right_column_actions.getColumnName();

        equi_pairs.emplace_back(left_column, right_column);

        auto left_column_statistics = left_statistics.column_statistics.find(left_column);
        auto right_column_statistics = right_statistics.column_statistics.find(right_column);

        /// A key without an NDV (no entry, or an entry with a zero NDV that only carries the column's
        /// width) counts its relation's rows as its NDV for the selectivity, as the join order
        /// optimizer does in `getColumnStats`; both planners must give the same join the same
        /// selectivity. The result keeps such a key without an NDV: the stand-in written as an NDV
        /// would make an aggregation above the join estimate one group per row.
        auto known_distinct_values = [](const auto & found, const auto & end, const ExpressionStatistics & side) -> std::optional<UInt64>
        {
            if (found != end && found->second.num_distinct_values > 0)
                return std::min(found->second.num_distinct_values, UInt64(side.estimated_row_count));
            return std::nullopt;
        };
        const auto left_known_distinct_values = known_distinct_values(left_column_statistics, left_statistics.column_statistics.end(), left_statistics);
        const auto right_known_distinct_values = known_distinct_values(right_column_statistics, right_statistics.column_statistics.end(), right_statistics);

        const auto predicate_selectivity = QueryPlanOptimizations::equalitySelectivity(
            left_known_distinct_values.value_or(UInt64(left_statistics.estimated_row_count)),
            right_known_distinct_values.value_or(UInt64(right_statistics.estimated_row_count)));

        /// The shared update then narrows only sides whose rows can be filtered by this join; a side
        /// without an NDV takes the other side's NDV when the join bounds it, else stays without one.
        statistics.column_statistics[left_column].num_distinct_values = left_known_distinct_values.value_or(0);
        statistics.column_statistics[right_column].num_distinct_values = right_known_distinct_values.value_or(0);
        QueryPlanOptimizations::updateJoinKeyDistinctCounts(
            statistics.column_statistics.at(left_column),
            statistics.column_statistics.at(right_column),
            join_operator.kind,
            join_operator.strictness);

        if (!predicate_selectivity)
        {
            LOG_TEST(log, "Predicate '{} = {}' has no NDV and no rows on either side", left_column, right_column);
            continue;
        }
        LOG_TEST(log, "Predicate '{} = {}' selectivity: 1 / {}", left_column, right_column, 1.0 / *predicate_selectivity);

        std::pair<String, String> key_pair{
            class_representative(left_statistics.equivalences, left_column),
            class_representative(right_statistics.equivalences, right_column)};
        auto found = std::find(key_pairs.begin(), key_pairs.end(), key_pair);
        if (found == key_pairs.end())
        {
            key_pairs.push_back(key_pair);
            key_selectivities.push_back(*predicate_selectivity);
        }
        else
        {
            auto & existing = key_selectivities[found - key_pairs.begin()];
            existing = std::min(existing, *predicate_selectivity);
        }
    }

    const Float64 join_selectivity = QueryPlanOptimizations::combineKeySelectivities(std::move(key_selectivities), join_selectivity_exponential_backoff);

    /// The multiplicative value is the search value when no estimate exists.
    const Float64 search_value = left_statistics.estimated_row_count * right_statistics.estimated_row_count * join_selectivity;
    const bool inputs_known = !left_statistics.rows_unknown && !right_statistics.rows_unknown;

    const auto join_order_estimate = join_step.isEstimatedByJoinOrder() ? join_step.getResultRowsEstimation() : std::nullopt;
    if (join_order_estimate)
    {
        /// The join order optimizer decided this join from the same inputs, and its estimate is the one
        /// estimate of this result.
        statistics.estimated_row_count = Float64(*join_order_estimate);
    }
    else if (inputs_known)
    {
        /// The join order optimizer did not see this join, or could not estimate an input this derivation
        /// can (its estimator knows fewer operators, `Distinct` for one). The shared formula applies.
        statistics.estimated_row_count = Float64(estimateJoinCardinality(
            UInt64(left_statistics.estimated_row_count), UInt64(right_statistics.estimated_row_count),
            join_selectivity, join_operator.kind, join_operator.strictness).value());
    }
    else
    {
        /// An input is unknown, so the result is unknown. The join order optimizer's bound is the search
        /// value when it computed one.
        statistics.rows_unknown = true;
        const auto upper_bound = join_step.isEstimatedByJoinOrder() ? join_step.getResultRowsUpperBound() : std::nullopt;
        statistics.estimated_row_count = upper_bound ? Float64(*upper_bound) : search_value;
    }

    /// Constrain the inner-product estimate to the join semantics (outer joins keep the preserved side,
    /// semi/anti/any bound it). Applied after the join-order hint so a hint cannot exceed a semantic
    /// upper bound (e.g. a semi join above its preserved-side row count).
    statistics.estimated_row_count = clampJoinRowCount(join_operator.kind, join_operator.strictness,
        statistics.estimated_row_count, left_statistics.estimated_row_count, right_statistics.estimated_row_count);
    statistics.max_row_count = clampJoinMaxRowCount(join_operator.kind, join_operator.strictness,
        statistics.max_row_count, left_statistics.max_row_count, right_statistics.max_row_count);

    /// Column equivalences: both inputs' classes survive (the sides do not share column names).
    /// An inner join also makes its equality keys equal on every output row, so each key pair
    /// links the two classes; other kinds keep unmatched rows, where the equality does not hold.
    statistics.equivalences = left_statistics.equivalences;
    statistics.equivalences.merge(right_statistics.equivalences);
    if (join_operator.kind == JoinKind::Inner)
        for (const auto & [left_column, right_column] : equi_pairs)
            statistics.equivalences.add(left_column, right_column);

    /// Width comes from the actual join output columns; summing both inputs double-counts join keys and
    /// can include columns the join does not emit. Use the inputs' known column sizes where available:
    /// with type defaults alone a short `String` would count as 64 bytes, and a cheap post-join shuffle
    /// could look costlier than shuffling the whole pre-join input.
    statistics.estimated_bytes_per_row = estimateRowWidth(*join_step.getOutputHeader(), statistics.column_statistics);

    for (auto & column_statistics : statistics.column_statistics)
        if (Float64(column_statistics.second.num_distinct_values) > statistics.estimated_row_count)
            column_statistics.second.num_distinct_values = UInt64(statistics.estimated_row_count);

    if (statistics.estimated_row_count < 0.01)
    {
        LOG_TEST(log, "Possibly incorrect estimation result: {}\nleft stats: {}\nright stats: {}\njoin_selectivity: {}",
            statistics.dump(), left_statistics.dump(), right_statistics.dump(), join_selectivity);
    }

    return statistics;
}

ExpressionStatistics StatisticsDerivation::deriveReadStatistics(const ReadFromMergeTree & read_step)
{
    ExpressionStatistics statistics;
    const auto & table_name = read_step.getStorageID().getTableName();

    statistics.min_row_count = 0;
    statistics.max_row_count = Float64(read_step.getStorageSnapshot()->storage.totalRows(read_step.getContext()).value_or(std::numeric_limits<UInt64>::max()));

    ReadFromMergeTree::AnalysisResultPtr analyzed_result = read_step.getAnalyzedResult();
    analyzed_result = analyzed_result ? analyzed_result : read_step.selectRangesToRead();
    if (analyzed_result)
    {
        statistics.estimated_row_count = Float64(analyzed_result->selected_rows);
        statistics.max_row_count = Float64(analyzed_result->selected_rows);
    }
    else
    {
        /// Nothing was analyzed, so there is no estimate. The table size bounds the read when the
        /// storage knows it; otherwise the largest known leaf of the query is the search value.
        statistics.rows_unknown = true;
        statistics.estimated_row_count = statistics.max_row_count < Float64(std::numeric_limits<UInt64>::max())
            ? statistics.max_row_count
            : largestKnownLeafRowCount().value_or(CascadesDefaults::DEFAULT_UNKNOWN_READ_ROWS);
    }

    const Float64 physical_selected_rows = analyzed_result ? Float64(analyzed_result->selected_rows) : 0;

    if (read_step.getContext()->getSettingsRef()[Setting::allow_statistics_optimize])
    {
        /// TODO: Move this to IOptimizerStatistics implementation
        if (auto estimator = read_step.getConditionSelectivityEstimator(read_step.getAllColumnNames()))
        {
            auto prewhere_info = read_step.getPrewhereInfo();
            const ActionsDAG::Node * prewhere_node = prewhere_info
                ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
                : nullptr;
            auto relation_profile = estimator->estimateRelationProfile(nullptr, nullptr, prewhere_node);

            /// Index analysis already bounds the read: it cannot emit more than `selected_rows`.
            /// Without a `PREWHERE` the profile carries no filter, its row count is only the
            /// statistics' total, so the index-analysis estimate stays.
            if (prewhere_node)
                statistics.estimated_row_count = analyzed_result
                    ? std::min(Float64(relation_profile.rows), Float64(analyzed_result->selected_rows))
                    : Float64(relation_profile.rows);
            for (const auto & [column_name, column_stats] : relation_profile.column_stats)
                statistics.column_statistics[column_name].num_distinct_values = column_stats.num_distinct_values;
            /// The profile carries no byte sizes; leaving the default 1 byte per row would make wide
            /// tables look nearly free to move over the network.
            fillReadColumnWidths(statistics, read_step, table_name);
            statistics.estimated_bytes_per_row = estimateReadBytesPerRow(read_step, statistics);
            fillPhysicalReadBytes(statistics, physical_selected_rows);

            LOG_TEST(log, "Estimate statistics for table {}: {}", table_name, statistics.dump());
            return statistics;
        }
    }

    for (const auto & column_name : read_step.getAllColumnNames())
    {
        auto column_ndv = statistics_lookup.getNumberOfDistinctValues(table_name, column_name);
        if (column_ndv)
            statistics.column_statistics[column_name].num_distinct_values = column_ndv.value();
    }

    auto cardinality_hint = statistics_lookup.getCardinality(table_name);
    if (cardinality_hint)
    {
        statistics.estimated_row_count = std::min<Float64>(statistics.estimated_row_count, Float64(*cardinality_hint));
        statistics.rows_unknown = false;
    }

    fillReadColumnWidths(statistics, read_step, table_name);
    statistics.estimated_bytes_per_row = estimateReadBytesPerRow(read_step, statistics);
    fillPhysicalReadBytes(statistics, physical_selected_rows);

    return statistics;
}

void StatisticsDerivation::fillReadColumnWidths(ExpressionStatistics & statistics, const ReadFromMergeTree & read_step, const String & table_name)
{
    /// A table-level width hint marks the parts as stand-ins, so it beats their real sizes.
    auto avg_row_bytes_hint = statistics_lookup.getAvgRowBytes(table_name);
    auto storage_widths = avg_row_bytes_hint
        ? estimateReadColumnWidthsScaledToRow(read_step, *avg_row_bytes_hint)
        : estimateReadColumnWidths(read_step);
    for (const auto & [column_name, width] : storage_widths)
        statistics.column_statistics[column_name].avg_bytes = width;

    /// A per-column hint overrides the derived width.
    for (const auto & column_name : read_step.getAllColumnNames())
    {
        auto hint = statistics_lookup.getAvgColumnBytes(table_name, column_name);
        if (hint)
            statistics.column_statistics[column_name].avg_bytes = *hint;
    }
}

/// Output names that carry an input column through unchanged: `INPUT`/`ALIAS` chains only.
/// Value equality between renamed columns survives; a computed expression changes the values,
/// so it must not keep an equivalence.
static std::unordered_map<String, Names> identityOutputNames(const ActionsDAG & actions)
{
    std::unordered_map<String, Names> input_to_outputs;
    for (const auto * output : actions.getOutputs())
    {
        const auto * node = output;
        while (node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children.front();
        if (node->type == ActionsDAG::ActionType::INPUT)
            input_to_outputs[node->result_name].push_back(output->result_name);
    }
    return input_to_outputs;
}

static EquivalenceClasses<String> remapEquivalences(
    const EquivalenceClasses<String> & equivalences, const ActionsDAG & actions)
{
    auto input_to_outputs = identityOutputNames(actions);
    EquivalenceClasses<String> result;
    std::unordered_set<const void *> visited_classes;
    for (const auto & [member, class_ptr] : equivalences.getMemberToClassMap())
    {
        if (!class_ptr || !visited_classes.insert(class_ptr.get()).second)
            continue;
        Names renamed;
        for (const auto & class_member : *class_ptr)
        {
            auto it = input_to_outputs.find(class_member);
            if (it != input_to_outputs.end())
                renamed.insert(renamed.end(), it->second.begin(), it->second.end());
        }
        /// A class with fewer than two surviving members says nothing, and `add` skips it.
        for (size_t i = 1; i < renamed.size(); ++i)
            result.add(renamed[0], renamed[i]);
    }
    return result;
}

namespace
{

const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

}

/// The TRUE fraction of a filter expression, estimated from the input column NDVs and
/// equivalence classes. The input of a standalone filter is an arbitrary subplan (e.g. an
/// aggregation for `HAVING`), so there are no table statistics here, and the column statistics
/// carry no value ranges: an equality uses 1/NDV, everything else uses the default factors.
Float64 estimatePredicateSelectivity(const ActionsDAG::Node * node, const ExpressionStatistics & input_statistics)
{
    node = skipAliases(node);
    /// A constant filter column: the planner removes an always-false filter, so assume true.
    if (node->type == ActionsDAG::ActionType::COLUMN)
        return 1.0;
    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return CascadesDefaults::DEFAULT_UNKNOWN_SELECTIVITY;

    const String & name = node->function_base->getName();

    if (name == "and")
    {
        Float64 selectivity = 1.0;
        for (const auto * child : node->children)
            selectivity *= estimatePredicateSelectivity(child, input_statistics);
        return selectivity;
    }
    if (name == "or")
    {
        Float64 none_passes = 1.0;
        for (const auto * child : node->children)
            none_passes *= 1.0 - estimatePredicateSelectivity(child, input_statistics);
        return 1.0 - none_passes;
    }
    if (name == "not" && node->children.size() == 1)
        return 1.0 - estimatePredicateSelectivity(node->children.front(), input_statistics);

    /// A runtime join filter repeats the join selectivity, which the join estimate carries.
    if (name == "__applyFilter")
        return 1.0;
    if (name == "like" || name == "ilike")
        return CascadesDefaults::DEFAULT_LIKE_SELECTIVITY;
    if (name == "notLike" || name == "notILike")
        return 1.0 - CascadesDefaults::DEFAULT_LIKE_SELECTIVITY;
    if (name == "isNull")
        return CascadesDefaults::DEFAULT_EQUALITY_SELECTIVITY;
    if (name == "isNotNull")
        return 1.0 - CascadesDefaults::DEFAULT_EQUALITY_SELECTIVITY;

    const bool is_equals = name == "equals";
    const bool is_not_equals = name == "notEquals";
    const bool is_range = name == "less" || name == "greater" || name == "lessOrEquals" || name == "greaterOrEquals";
    if ((is_equals || is_not_equals || is_range) && node->children.size() == 2)
    {
        if (is_range)
            return CascadesDefaults::DEFAULT_RANGE_SELECTIVITY;

        const auto * left = skipAliases(node->children[0]);
        const auto * right = skipAliases(node->children[1]);
        const bool left_is_constant = left->column && isColumnConst(*left->column);
        const bool right_is_constant = right->column && isColumnConst(*right->column);

        auto column_ndv = [&](const ActionsDAG::Node * side) -> UInt64
        {
            auto it = input_statistics.column_statistics.find(side->result_name);
            return it != input_statistics.column_statistics.end() ? it->second.num_distinct_values : 0;
        };

        Float64 equal_selectivity = CascadesDefaults::DEFAULT_EQUALITY_SELECTIVITY;
        if (!left_is_constant && !right_is_constant)
        {
            /// Two columns. An equality the plan below already enforces (e.g. the keys of an
            /// inner join under this filter) holds on every row; otherwise the join-equality
            /// formula 1 / max(NDV) applies.
            auto left_class = input_statistics.equivalences.getClass(left->result_name);
            if (left_class && left_class == input_statistics.equivalences.getClass(right->result_name))
                equal_selectivity = 1.0;
            else if (UInt64 max_ndv = std::max(column_ndv(left), column_ndv(right)))
                equal_selectivity = 1.0 / Float64(max_ndv);
        }
        else
        {
            const auto * column_side = left_is_constant ? right : left;
            if (UInt64 ndv = column_ndv(column_side))
                equal_selectivity = 1.0 / Float64(ndv);
        }
        return is_equals ? equal_selectivity : 1.0 - equal_selectivity;
    }

    return CascadesDefaults::DEFAULT_UNKNOWN_SELECTIVITY;
}


ExpressionStatistics StatisticsDerivation::deriveFilterStatistics(const FilterStep & filter_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, filter_step.getExpression());
    result_statistics.equivalences = remapEquivalences(input_statistics.equivalences, filter_step.getExpression());

    const ActionsDAG::Node * filter_node = nullptr;
    for (const auto & dag_node : filter_step.getExpression().getNodes())
    {
        if (dag_node.result_name == filter_step.getFilterColumnName())
        {
            filter_node = &dag_node;
            break;
        }
    }

    if (filter_node)
    {
        const Float64 selectivity = estimatePredicateSelectivity(filter_node, input_statistics);
        result_statistics.estimated_row_count *= selectivity;
        result_statistics.min_row_count = 0;
        /// A column cannot have more distinct values than there are rows.
        for (auto & [column_name, column_stats] : result_statistics.column_statistics)
            if (Float64(column_stats.num_distinct_values) > result_statistics.estimated_row_count)
                column_stats.num_distinct_values = UInt64(result_statistics.estimated_row_count);
        LOG_TEST(getLogger("StatisticsDerivation"), "Filter '{}' selectivity: {}", filter_step.getFilterColumnName(), selectivity);
    }

    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, expression_step.getExpression());
    result_statistics.equivalences = remapEquivalences(input_statistics.equivalences, expression_step.getExpression());
    /// Keep the input row width: most projections pass columns through, and the input width may carry
    /// storage-derived or hinted byte sizes that a header-based type-default estimate would discard.
    /// TODO: recompute only for added/dropped columns (needs per-column widths); an arrayJoin also
    /// grows the row count (preserves_number_of_rows is false), which we do not estimate yet.
    return result_statistics;
}

/// Fraction of the input rows taken as distinct when no key has statistics: the search value of a
/// group count the statistics do not determine.
static constexpr Float64 DEFAULT_DISTINCT_VALUES_RATIO = 0.1;

/// A `Float64` row count as an integer; the unbounded value stays the largest integer.
static UInt64 toRowCount(Float64 rows)
{
    if (!(rows < Float64(std::numeric_limits<UInt64>::max())))
        return std::numeric_limits<UInt64>::max();
    return rows <= 0 ? 0 : UInt64(rows);
}

/// NDV of a group key in the input, capped by the input's bound; nothing without statistics (an
/// entry with a zero NDV only carries the column's width).
static std::optional<UInt64> keyDistinctValues(const String & column, const ExpressionStatistics & input_statistics)
{
    auto column_stats = input_statistics.column_statistics.find(column);
    if (column_stats == input_statistics.column_statistics.end() || column_stats->second.num_distinct_values == 0)
        return std::nullopt;
    return std::min(column_stats->second.num_distinct_values, toRowCount(input_statistics.max_row_count));
}

struct GroupCount
{
    Float64 estimated_rows;
    Float64 max_rows;
    /// No key has an NDV: `estimated_rows` is the default fraction of the input, a search value.
    bool unknown;
};

/// Distinct value combinations of the columns by the shared group count formula (the largest key
/// NDV, bounded by the product): the output rows of an aggregation on the columns and of a
/// `DISTINCT` over them.
static GroupCount estimateGroupCount(const Names & columns, const ExpressionStatistics & input_statistics, bool damped_product)
{
    std::vector<UInt64> key_distinct_values;
    for (const auto & column : columns)
        key_distinct_values.push_back(keyDistinctValues(column, input_statistics).value_or(0));
    const auto shared = QueryPlanOptimizations::estimateGroupCount(
        key_distinct_values, toRowCount(input_statistics.estimated_row_count), toRowCount(input_statistics.max_row_count), damped_product);

    GroupCount result;
    result.unknown = !shared.estimated_rows;
    result.estimated_rows = shared.estimated_rows
        ? Float64(*shared.estimated_rows)
        : std::min(DEFAULT_DISTINCT_VALUES_RATIO * input_statistics.estimated_row_count, input_statistics.max_row_count);
    result.max_rows = shared.max_rows ? Float64(*shared.max_rows) : input_statistics.max_row_count;
    return result;
}

ExpressionStatistics StatisticsDerivation::deriveAggregatingStatistics(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    ExpressionStatistics aggregation_statistics;
    /// A key without an NDV stays without one in the output.
    for (const auto & key : aggregator_params.keys)
        aggregation_statistics.column_statistics[key].num_distinct_values = keyDistinctValues(key, input_statistics).value_or(0);

    aggregation_statistics.min_row_count = 0;
    const auto groups = estimateGroupCount(aggregator_params.keys, input_statistics, group_count_damped_product);
    aggregation_statistics.estimated_row_count = groups.estimated_rows;
    aggregation_statistics.max_row_count = groups.max_rows;
    aggregation_statistics.rows_unknown = groups.unknown;
    /// Group-by keys pass through with their input value sizes.
    for (auto & [column_name, column_stats] : aggregation_statistics.column_statistics)
    {
        auto input_column_statistics = input_statistics.column_statistics.find(column_name);
        if (input_column_statistics != input_statistics.column_statistics.end())
            column_stats.avg_bytes = input_column_statistics->second.avg_bytes;
    }
    /// Aggregation changes the schema (group-by keys + aggregate states), recompute from output
    /// header with the keys' known value sizes.
    aggregation_statistics.estimated_bytes_per_row = estimateRowWidth(*aggregating_step.getOutputHeader(), aggregation_statistics.column_statistics);

    /// Group keys pass through with their values, so their equivalences survive; the column
    /// statistics at this point hold exactly the group keys.
    std::unordered_set<const void *> visited_classes;
    for (const auto & [member, class_ptr] : input_statistics.equivalences.getMemberToClassMap())
    {
        if (!class_ptr || !visited_classes.insert(class_ptr.get()).second)
            continue;
        Names kept;
        for (const auto & class_member : *class_ptr)
            if (aggregation_statistics.column_statistics.contains(class_member))
                kept.push_back(class_member);
        for (size_t i = 1; i < kept.size(); ++i)
            aggregation_statistics.equivalences.add(kept[0], kept[i]);
    }

    return aggregation_statistics;
}

static void trimStatisticsByLimit(ExpressionStatistics & statistics, UInt64 limit)
{
    statistics.estimated_row_count = std::min(statistics.estimated_row_count, Float64(limit));
    statistics.max_row_count = std::min(statistics.max_row_count, Float64(limit));
    for (auto & column_statistics : statistics.column_statistics)
        if (Float64(column_statistics.second.num_distinct_values) > statistics.estimated_row_count)
            column_statistics.second.num_distinct_values = UInt64(statistics.estimated_row_count);
}

ExpressionStatistics StatisticsDerivation::deriveSortingStatistics(const SortingStep & sorting_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    /// If there is no LIMIT, then sorting does not change statistics
    if (sorting_step.getLimit())
    {
        trimStatisticsByLimit(result_statistics, sorting_step.getLimit());
    }
    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveLimitStatistics(const LimitStep & limit_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    /// Without a `LIMIT` value the step does not change statistics
    if (limit_step.getLimit())
    {
        trimStatisticsByLimit(result_statistics, limit_step.getLimit());
    }
    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveUnionStatistics(
    const UnionStep & union_step, const std::function<const ExpressionStatistics &(size_t)> & input_statistics, size_t input_count)
{
    /// `UNION ALL`: rows and bounds add up. The output takes the first input's column names; a
    /// column's NDV is at most the sum over the inputs, unknown when an input's is. Equal values
    /// on every row of one input say nothing about the union, so no equivalence survives.
    ExpressionStatistics result = input_statistics(0);
    result.equivalences = {};
    const auto & input_headers = union_step.getInputHeaders();
    for (size_t input_index = 1; input_index < input_count; ++input_index)
    {
        const auto & other = input_statistics(input_index);
        result.estimated_row_count += other.estimated_row_count;
        result.min_row_count += other.min_row_count;
        result.max_row_count += other.max_row_count;
        result.estimated_distinct_bound += other.estimated_distinct_bound;
        for (size_t position = 0; position < input_headers.at(0)->columns(); ++position)
        {
            auto output_column = result.column_statistics.find(input_headers.at(0)->getByPosition(position).name);
            if (output_column == result.column_statistics.end())
                continue;
            auto other_column = other.column_statistics.find(input_headers.at(input_index)->getByPosition(position).name);
            const UInt64 other_distinct_values = other_column == other.column_statistics.end() ? 0 : other_column->second.num_distinct_values;
            output_column->second.num_distinct_values = output_column->second.num_distinct_values == 0 || other_distinct_values == 0
                ? 0
                : output_column->second.num_distinct_values + other_distinct_values;
        }
    }
    return result;
}

ExpressionStatistics StatisticsDerivation::deriveDistinctStatistics(const DistinctStep & distinct_step, const ExpressionStatistics & input_statistics)
{
    /// One output row per distinct value combination.
    ExpressionStatistics result = input_statistics;
    const auto groups = estimateGroupCount(distinct_step.getColumnNames(), input_statistics, group_count_damped_product);
    result.estimated_row_count = std::min(groups.estimated_rows, input_statistics.estimated_distinct_bound);
    result.max_row_count = groups.max_rows;
    /// Without a key NDV the distinct rows are unknown, unless the input bounds its distinct rows
    /// below the search value: that bound is then the estimate.
    result.rows_unknown = groups.unknown && !(input_statistics.estimated_distinct_bound < groups.estimated_rows);
    result.min_row_count = input_statistics.min_row_count > 0 ? 1 : 0;
    /// Every output row is distinct.
    result.estimated_distinct_bound = result.estimated_row_count;
    /// Without the clamp the row-count reduction could leave a column NDV above the row count.
    for (auto & [column_name, column_stats] : result.column_statistics)
        column_stats.num_distinct_values = std::min(column_stats.num_distinct_values,
            static_cast<UInt64>(std::max(result.estimated_row_count, 1.0)));
    return result;
}

Float64 StatisticsDerivation::estimateReadBytesPerRow(const ReadFromMergeTree & read_step, const ExpressionStatistics & statistics)
{
    /// Priority: table-level hint > per-column widths (hinted or storage-derived) > type-based estimate
    auto avg_row_bytes_hint = statistics_lookup.getAvgRowBytes(read_step.getStorageID().getTableName());
    if (avg_row_bytes_hint)
        return *avg_row_bytes_hint;

    return estimateRowWidth(*read_step.getOutputHeader(), statistics.column_statistics);
}

}
