#pragma once

#include <base/types.h>
#include <optional>

namespace DB
{

/// Defined in `RelationEstimateInfo.h`; declared opaque here so that the plan steps and processors
/// carrying a `CostEstimationInfo` do not depend on that header. Zero is `NoSource`.
enum class RowEstimateSource : UInt8;

/// The estimate an optimizer attached to a plan step, whichever planner decided the step.
/// `EXPLAIN estimates = 1` prints it, and the processors created from the step record it in
/// `system.processors_profile_log` next to the rows they produced.
struct CostEstimationInfo
{
    /// Absent when the optimizer had no estimate for the step.
    std::optional<Float64> rows;
    /// Absent when the optimizer has no cost for the step; the unit is that optimizer's own.
    std::optional<Float64> cost;
    RowEstimateSource source{};
};

}
