#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

namespace DB
{

/// The processor between the sources of MergeTreeReadPoolInOrderSliced and the merge that reads in order.
/// Input i is source i; output l is lane l, one part, whose chunks come out in mark order as if one
/// source read that part alone (in reverse order, from the last mark down).
///
/// It keeps no state of its own. Every chunk a source emits goes to the pool, which buffers it in its
/// slice, and every lane output is served from the pool: the lane's next rows, or a virtual row announcing
/// the key the lane's next rows start at when none are ready. What the router decides is only when a
/// source runs: it sets as many parked inputs needed as the pool has slices waiting, and parks a source
/// again when it reports that it found nothing, so sources with nothing to read cost nothing.
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
    /// The pool is finished and the idle sources were woken up to end their streams.
    bool finishing = false;
};

}
