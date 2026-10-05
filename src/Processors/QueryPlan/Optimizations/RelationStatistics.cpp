#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

namespace DB::QueryPlanOptimizations
{

GroupCountEstimate estimateGroupCount(
    const std::vector<UInt64> & key_distinct_values, std::optional<UInt64> rows, std::optional<UInt64> max_rows, bool damped_product)
{
    /// No keys: one group.
    if (key_distinct_values.empty())
        return GroupCountEstimate{.estimated_rows = 1, .max_rows = 1};

    std::vector<UInt64> known;
    std::optional<UInt64> product = 1;
    for (UInt64 distinct_values : key_distinct_values)
    {
        if (distinct_values == 0)
        {
            product.reset();
            continue;
        }
        known.push_back(distinct_values);
        if (product)
        {
            UInt64 multiplied = 0;
            product = __builtin_mul_overflow(*product, distinct_values, &multiplied) ? std::numeric_limits<UInt64>::max() : multiplied;
        }
    }

    GroupCountEstimate result;
    if (!known.empty())
    {
        std::sort(known.begin(), known.end(), std::greater<>());
        if (damped_product)
        {
            double damped = 1.0;
            double exponent = 1.0;
            for (UInt64 distinct_values : known)
            {
                damped *= std::pow(static_cast<double>(distinct_values), exponent);
                exponent /= 2;
            }
            result.estimated_rows = damped >= static_cast<double>(std::numeric_limits<UInt64>::max())
                ? std::numeric_limits<UInt64>::max()
                : static_cast<UInt64>(std::llround(damped));
        }
        else
        {
            result.estimated_rows = known.front();
        }
    }
    if (result.estimated_rows && rows)
        result.estimated_rows = std::min(*result.estimated_rows, *rows);
    result.max_rows = max_rows;
    if (product && (!result.max_rows || *product < *result.max_rows))
        result.max_rows = product;
    return result;
}

std::optional<double> equalitySelectivity(UInt64 left_distinct_values, UInt64 right_distinct_values)
{
    const UInt64 larger = std::max(left_distinct_values, right_distinct_values);
    if (larger == 0)
        return std::nullopt;
    return 1.0 / static_cast<double>(larger);
}

double combineKeySelectivities(std::vector<double> selectivities, bool exponential_backoff)
{
    if (selectivities.empty())
        return 1.0;
    std::sort(selectivities.begin(), selectivities.end());
    if (!exponential_backoff)
        return selectivities.front();

    double result = 1.0;
    double exponent = 1.0;
    for (size_t i = 0; i < selectivities.size() && i < 4; ++i)
    {
        result *= std::pow(selectivities[i], exponent);
        exponent /= 2;
    }
    return result;
}

void updateJoinKeyDistinctCounts(
    ColumnStats & left_stats,
    ColumnStats & right_stats,
    JoinKind kind,
    JoinStrictness strictness)
{
    bool update_left = false;
    bool update_right = false;
    if (strictness == JoinStrictness::Semi)
    {
        /// Only the output side is filtered to matching keys; the other side is not in the output.
        update_left = kind == JoinKind::Left;
        update_right = kind == JoinKind::Right;
    }
    else if (strictness != JoinStrictness::Anti)
    {
        /// An outer join preserves the named side, so only the non-preserved side can lose key values.
        update_left = kind == JoinKind::Inner || kind == JoinKind::Right
            || kind == JoinKind::Cross || kind == JoinKind::Comma;
        update_right = kind == JoinKind::Inner || kind == JoinKind::Left
            || kind == JoinKind::Cross || kind == JoinKind::Comma;
    }

    /// A zero NDV is an unknown NDV: it does not narrow the other side, and a side the join filters
    /// takes the other side's NDV as its bound.
    const UInt64 left_distinct_values = left_stats.num_distinct_values;
    const UInt64 right_distinct_values = right_stats.num_distinct_values;
    UInt64 minimum = std::min(left_distinct_values, right_distinct_values);
    if (left_distinct_values == 0)
        minimum = right_distinct_values;
    else if (right_distinct_values == 0)
        minimum = left_distinct_values;
    if (update_left)
        left_stats.num_distinct_values = minimum;
    if (update_right)
        right_stats.num_distinct_values = minimum;
}

void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions)
{
    /// Column statistics are usually absent; do not pay for a full lineage walk of the
    /// `ActionsDAG` when there is nothing to remap.
    if (mapped.empty())
        return;

    std::unordered_map<String, ColumnStats> original;
    original.swap(mapped);

    const auto lineage = traceActionsDAGLineage(actions);
    const auto & inputs = actions.getInputs();
    const auto & outputs = actions.getOutputs();
    for (const auto & output_lineage : lineage)
    {
        if (!output_lineage.input)
            continue;

        const auto stats_it = original.find(inputs[output_lineage.input->input_position]->result_name);
        if (stats_it == original.end())
            continue;

        ColumnStats stats = stats_it->second;
        /// Add the offset, guarding against overflow when the source NDV is near the maximum.
        if (stats.num_distinct_values <= std::numeric_limits<UInt64>::max() - output_lineage.input->ndv_delta)
            stats.num_distinct_values += output_lineage.input->ndv_delta;
        /// A hop that changes the type (e.g. `toString(k)`) changes the value bytes, so drop the
        /// width to unknown.
        if (!output_lineage.input->preserves_width)
            stats.avg_bytes = 0;
        /// The value range and NULL set survive only lineage known to pass the value through
        /// unchanged; unlike NDV, they do not survive a generic deterministic function (e.g. `negate(k)`)
        /// or a `CAST`, which may rewrite NULL rows into real values.
        if (output_lineage.input->kind == ActionsDAGLineageKind::DistinctValuesBound)
        {
            stats.min_value.reset();
            stats.max_value.reset();
            stats.null_fraction.reset();
        }
        mapped[outputs[output_lineage.output_position]->result_name] = stats;
    }
}

}
