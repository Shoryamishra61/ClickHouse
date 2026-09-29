#pragma once

#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>


namespace DB
{

/// Half-open. Until `fully_visible` is set, the bounds are where the search stands.
struct FrameBounds
{
    RowNumber start;
    RowNumber end;
    bool fully_visible = false;
};

class Frame
{
public:
    Frame(const WindowTransformParams & params_, const SlidingBlocks & blocks_);

    void enterPartition(RowNumber partition_start);
    void advance(const RowPoint & current, const PartitionBounds & partition);

    FrameBounds bounds() const;

private:
    const WindowTransformParams & params;
    const SlidingBlocks & blocks;
    RowPoint start;
    RowPoint end;
    bool fully_visible = false;
};

}
