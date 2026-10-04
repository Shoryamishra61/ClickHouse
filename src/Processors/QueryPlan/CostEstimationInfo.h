#pragma once

#include <base/types.h>
#include <optional>

namespace DB
{

/// Defined in `RelationEstimateInfo.h` together with its name and tag helpers. Declared opaque here
/// so that every plan step and processor, which carry a `CostEstimationInfo`, does not depend on
/// that header: a new estimate source then rebuilds the optimizer, not the whole tree.
enum class RowEstimateSource : UInt8;

/// The estimate an optimizer attached to a plan step, whichever planner decided the step.
/// `EXPLAIN estimates = 1` prints it, and the processors created from the step record it in
/// `system.processors_profile_log` next to the rows they produced.
struct CostEstimationInfo
{
    /// Absent when the optimizer had no estimate for the step. A missing estimate stays missing
    /// here; it is never replaced by a default.
    std::optional<Float64> rows;
    /// Absent when the optimizer has no cost for the step; the unit is that optimizer's own.
    std::optional<Float64> cost;
    /// Zero is `RowEstimateSource::NoSource`: the origin was not tracked.
    RowEstimateSource source{};
    /// Some input of the estimate was a default rather than a measurement.
    bool imprecise = false;
};

}
