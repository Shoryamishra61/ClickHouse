#pragma once

#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>

#include <optional>

namespace DB
{

/// Half-open, handed out only once both bounds are found.
struct FrameBounds
{
    RowNumber start;
    RowNumber end;
};

class Frame
{
public:
    Frame(const WindowTransformParams & params_, const SlidingBlocks & blocks_);

    void enterPartition(RowNumber partition_start);
    std::optional<FrameBounds> advance(const RowPoint & current, const PartitionBounds & partition);

private:
    const WindowTransformParams & params;
    const SlidingBlocks & blocks;
    RowPoint start;
    RowPoint end;
};

}
