#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

namespace DB
{

/// Connects the sources reading from MergeTreeReadPoolInOrderSliced to the merge that reads in order.
/// Input i is source i; output l is lane l, one part, whose chunks come out in mark order as if a
/// single source read that part alone; in reverse order, from the last mark down.
///
/// The router is a port adapter: the pool owns all state and makes all decisions. The executor runs a
/// processor only through its ports, so this is the processor that is run when a source has a chunk or
/// the merge asks for a lane, and that sets the sources' inputs needed or not. Chunks are placed by the
/// slice tag they carry; which source read them does not matter. A source runs only while its input is
/// needed, and the input is set needed only while the pool has slices for it, so idle sources cost
/// nothing and no part is read before the pool asks for it.
class MergeTreeInOrderSliceRouter final : public IProcessor
{
public:
    MergeTreeInOrderSliceRouter(SharedHeader header, std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_);

    String getName() const override { return "MergeTreeInOrderSliceRouter"; }
    Status prepare() override;

private:
    /// Called once every lane is finished; ends the sources.
    Status finish();

    const std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool;

    std::vector<InputPort *> source_inputs;
    std::vector<OutputPort *> lane_outputs;
    /// Whether each input is set needed; InputPort does not tell.
    std::vector<bool> input_needed;
};

}
