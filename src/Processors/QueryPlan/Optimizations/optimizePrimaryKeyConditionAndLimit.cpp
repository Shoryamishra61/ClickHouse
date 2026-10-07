#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/ObjectFilterStep.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <DataTypes/DataTypeArray.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/array/emptyArrayToSingle.h>
#include <Interpreters/PreparedSets.h>

#include <list>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// the array under emptyArrayToSingle in node, or nullptr
const ActionsDAG::Node * getPaddedArray(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    if (node->type != ActionsDAG::ActionType::FUNCTION || node->function_base->getName() != "emptyArrayToSingle")
        return nullptr;
    const auto * array = node->children.front();
    return array->result_type->equals(*node->result_type) ? array : nullptr;
}

/// index analysis builds these sets anyway, the probe needs them for IN (subquery)
void buildSets(const ActionsDAG & dag, const ContextPtr & context)
{
    for (const auto & node : dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::COLUMN || !node.column)
            continue;
        const auto * column_set = checkAndGetColumn<const ColumnSet>(&node.column->getDataColumn());
        if (auto future_set = column_set ? column_set->getData() : nullptr; future_set && !future_set->get())
            future_set->buildOrderedSetInplace(context);
    }
}

}

void optimizePrimaryKeyConditionAndLimit(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilterBase *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    /// Collect ExpressionStep DAGs encountered while walking up the plan.
    /// When a filter references columns produced by expressions (e.g., ALIAS
    /// columns computed in "Compute alias columns" step, or renamed in
    /// "Change column names to column identifiers" step), we compose the
    /// filter through these expression DAGs so that column references are
    /// resolved to physical columns. This is essential for correct index
    /// analysis when plan optimizations like mergeExpressions have not
    /// merged these steps into the filter.
    std::vector<const ActionsDAG *> expression_dags;
    /// the same, but a LEFT join or a join over emptyArrayToSingle(x) joins the array itself
    std::vector<const ActionsDAG *> inner_dags;
    /// A list, `expression_dags` keeps pointers into it.
    std::list<ActionsDAG> array_join_dags;

    /// joins that emit a default element for an empty array
    struct PaddingJoin
    {
        size_t first_dag_above;
        Block padded;
    };
    std::vector<PaddingJoin> padding_joins;

    auto push = [&](const ActionsDAG * dag, const ActionsDAG * inner_dag = nullptr)
    {
        expression_dags.push_back(dag);
        inner_dags.push_back(inner_dag ? inner_dag : dag);
    };

    /// Resolve the filter's columns down to the source through the steps below it.
    auto compose = [&](ActionsDAG filter_dag, const std::vector<const ActionsDAG *> & dags, size_t from = 0)
    {
        for (size_t i = dags.size(); i > from; --i)
            filter_dag = ActionsDAG::merge(dags[i - 1]->clone(), std::move(filter_dag));
        return filter_dag;
    };

    const auto * source_with_context = dynamic_cast<const SourceStepWithFilter *>(source_step_with_filter);

    /// a filter that is false on the default element can treat these joins as plain ones
    auto add_filter = [&](const ActionsDAG & filter_dag, const String & filter_column_name)
    {
        bool rejects_padding = std::ranges::all_of(padding_joins, [&](const PaddingJoin & join)
        {
            auto probe = compose(filter_dag.clone(), expression_dags, join.first_dag_above);
            if (source_with_context)
                buildSets(probe, source_with_context->getContext());
            return filterResultForNotMatchedRows(probe, filter_column_name, join.padded) == FilterResult::FALSE;
        });
        source_step_with_filter->addFilter(
            compose(filter_dag.clone(), rejects_padding ? inner_dags : expression_dags), filter_column_name);
    };

    /// in inner_dags, the DAG below that computes name as emptyArrayToSingle(x) returns x instead
    auto unpad_below = [&](String name)
    {
        for (size_t i = expression_dags.size(); i > 0; --i)
        {
            const auto * output = expression_dags[i - 1]->tryFindInOutputs(name);
            if (!output)
                continue;
            while (output->type == ActionsDAG::ActionType::ALIAS)
                output = output->children.front();
            if (output->type == ActionsDAG::ActionType::INPUT)
            {
                name = output->result_name;
                continue;
            }
            if (!getPaddedArray(output))
                return false;

            auto & dag = array_join_dags.emplace_back(inner_dags[i - 1]->clone());
            for (auto & dag_output : dag.getOutputs())
                if (dag_output->result_name == name)
                    dag_output = &dag.addAlias(*getPaddedArray(dag_output), name);
            inner_dags[i - 1] = &dag;
            return true;
        }
        return false;
    };

    const auto & storage_prewhere_info = source_step_with_filter->getPrewhereInfo();
    const auto & storage_row_level_filter = source_step_with_filter->getRowLevelFilter();
    if (storage_row_level_filter)
        source_step_with_filter->addFilter(storage_row_level_filter->actions.clone(), storage_row_level_filter->column_name);
    if (storage_prewhere_info)
    {
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);
        /// Prewhere and filters may also compute columns that the steps above refer to.
        push(&storage_prewhere_info->prewhere_actions);
    }

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            add_filter(filter_step->getExpression(), filter_step->getFilterColumnName());
            push(&filter_step->getExpression());
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(iter->node->step.get()))
        {
            /// A LIMIT above an ARRAY JOIN says nothing about the source row count.
            if (array_join_dags.empty())
            {
                if (auto rows_to_read = limit_step->getLimitWithOffset())
                    source_step_with_filter->setLimit(*rows_to_read);
            }
            break;
        }
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(iter->node->step.get()))
        {
            /// `arrayJoin` in an `ExpressionStep` above the source changes row cardinality.
            /// Propagating the outer `LIMIT` past such a step is unsound: the source would
            /// be told to generate at most N rows, and `arrayJoin` would then expand only
            /// those (possibly producing fewer than N output rows when arrays are empty,
            /// or wrong rows when arrays expand). Composing filters through `arrayJoin`
            /// expressions is unsound for the same reason. Stop walking here and skip both
            /// filter composition and limit propagation. See issue #82279 and the sibling
            /// guards in `liftUpFunctions`, `optimizeLazyMaterialization`, `optimizeTopK`,
            /// `topKThroughJoin`, and `pushLimitByIntoSort`.
            if (expression_step->getExpression().hasArrayJoin())
                break;
            push(&expression_step->getExpression());
            continue;
        }
        else if (auto * array_join_step = typeid_cast<ArrayJoinStep *>(iter->node->step.get()))
        {
            /// A plain ARRAY JOIN emits real elements only, so a condition on the element is one on the array too.
            /// Unaligned joins pad short arrays with defaults, so theirs is not.
            const auto & input_header = *array_join_step->getInputHeaders().front();
            const auto & columns = array_join_step->getColumns();
            if (array_join_step->isUnaligned()
                || (array_join_step->isLeft()
                    && !std::ranges::all_of(columns, [&](const auto & name) { return isArray(input_header.getByName(name).type); })))
                break;

            auto & dag = array_join_dags.emplace_back(input_header.getNamesAndTypesList());
            auto & inner_dag = array_join_dags.emplace_back(input_header.getNamesAndTypesList());
            Block padded;
            for (const auto & name : columns)
            {
                /// LEFT ARRAY JOIN is arrayJoin(emptyArrayToSingle(col))
                const auto * array = &dag.findInOutputs(name);
                if (array_join_step->isLeft())
                    array = &dag.addFunction(std::make_shared<FunctionToOverloadResolverAdaptor>(FunctionEmptyArrayToSingle::createImpl()), {array}, {});
                if (array_join_step->isLeft() || unpad_below(name))
                    padded.insert(array_join_step->getOutputHeader()->getByName(name));

                /// The atom must be `arrayJoin(col)`, not something named like the array column; the alias just keeps the name.
                dag.addOrReplaceInOutputs(dag.addAlias(dag.addArrayJoin(*array, {}), name));
                inner_dag.addOrReplaceInOutputs(inner_dag.addAlias(inner_dag.addArrayJoin(inner_dag.findInOutputs(name), {}), name));
            }
            push(&dag, &inner_dag);
            if (padded.columns())
                padding_joins.push_back({expression_dags.size(), std::move(padded)});

            /// The fused element filter is just a filter right above the join.
            if (const auto & element_filter = array_join_step->getElementFilter())
                add_filter(*element_filter, array_join_step->getElementFilterColumnName());
        }
        else if (auto * object_filter_step = typeid_cast<ObjectFilterStep *>(iter->node->step.get()))
        {
            /// Not composed, so above an ARRAY JOIN its names would mean the elements.
            if (!array_join_dags.empty())
                break;
            source_step_with_filter->addFilter(object_filter_step->getExpression().clone(), object_filter_step->getFilterColumnName());
        }
        else
        {
            break;
        }
    }

    source_step_with_filter->applyFilters();
}

}
