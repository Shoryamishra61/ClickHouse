#pragma once

#include <Processors/IProcessor.h>
#include <Storages/MergeTree/MergeTreeReadPoolInOrderSliced.h>

#include <deque>
#include <map>
#include <optional>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Connects the sources reading from MergeTreeReadPoolInOrderSliced to the merge that reads in order.
/// Input i is source i; output l is lane l, one part, whose chunks come out in mark order as if a
/// single source read that part alone; in reverse order, from the last mark down.
///
/// A lane the merge asks for gets the next chunk of its first issued slice or, when no rows are ready,
/// a virtual row announcing the primary key its next rows start at: the first mark of the slice in
/// flight, else the lane's next unread mark. So the merge goes on with the other lanes until it reaches
/// that key instead of waiting for a lane whose rows are filtered out until it is read to its end. A key
/// is announced once; a lane that was announced and is asked again is one the merge waits for.
///
/// The router decides what is read and when. A source runs only while its input port is needed, and
/// the port is set needed only after a slice was assigned to it, so idle sources cost nothing and no
/// part is read before the router asks for it. Slices are issued only when the merge asks for data or
/// waits for a lane, by two rules:
/// - the lane the merge waits for gets its next slice whenever it has none issued, so the merge is never
///   blocked on a lane nobody reads; with it, every lane whose next key comes before that slice's end
///   gets its next slice too, since the merge reaches those lanes before it is done with the slice;
/// - the lanes the merge needs next, in the order of the pool's queue, get slices while the marks issued
///   so far stay within the read-ahead budget. The budget is zero until a slice comes back with most of
///   its rows filtered out. Then it covers the rest of the ramp of slice sizes, so the ramp is read in one
///   round instead of one slice after another, and once as many slices have missed as the ramp has steps,
///   reading and not merging is the bottleneck for sure and every source gets a slice.
/// A slice counts as issued until the merge has taken its last row, so the budget bounds the rows held in
/// the router as well as the sources reading on behalf of the merge. A slice read ahead that comes back
/// without rows is dropped at once, but its marks stay in the budget until the merge asks again: otherwise,
/// while the merge waits for one slow slice, the sources would read a lane that yields nothing to its end.
/// The empty slices of the lane the merge waits for are not held: that lane is read on until it yields.
class MergeTreeInOrderSliceRouter final : public IProcessor
{
public:
    MergeTreeInOrderSliceRouter(
        SharedHeader header,
        std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_,
        ExpressionActionsPtr virtual_row_conversions_);

    String getName() const override { return "MergeTreeInOrderSliceRouter"; }
    Status prepare() override;

private:
    struct SliceBuffer
    {
        std::deque<Chunk> chunks;
        size_t marks = 0;
        /// The mark the rows of the slice start at in reading order, announced while the slice is in flight.
        size_t boundary_mark = 0;
        bool finished = false;
        /// Some rows of the slice passed the reader's filter.
        bool had_rows = false;
    };

    using SliceBuffers = std::map<size_t, SliceBuffer>;

    struct Lane
    {
        /// Issued slices by their first mark: in flight, or finished with rows the merge has not taken yet.
        SliceBuffers slices;
        /// The primary key announced to the merge last; empty before the first announcement.
        Block announced_key;
        /// The merge waits for this lane right now and nothing is ready for it.
        bool wants_data = false;
        bool finished = false;
    };

    struct Assignment
    {
        size_t lane;
        size_t first_mark;
        size_t rows_in_marks;
        size_t rows_read = 0;
    };

    void consumeInput(size_t source);
    /// The issued slice the lane's rows leave through: the first in reading order, if any.
    SliceBuffers::iterator headSlice(Lane & lane) const;
    /// Serves the lane if its output can take a chunk. Returns whether the merge asked for the lane anew:
    /// the output can take a chunk and the lane was not waiting already.
    bool pushToLane(size_t lane);
    /// Announces the key at the mark of the lane unless it is the key announced last; returns whether it did.
    bool announce(size_t lane, size_t mark);
    void finishLane(size_t lane);
    void dropSlice(size_t lane, SliceBuffers::iterator slice);
    size_t readAheadMarks() const;
    std::optional<size_t> pickIdleSource(size_t lane) const;
    void assignSlice(size_t source, size_t lane);
    void scheduleSlices(bool merge_asked);
    /// Called once every lane is finished; ends the sources.
    Status finish();

    const std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool;
    const ExpressionActionsPtr virtual_row_conversions;

    std::vector<InputPort *> source_inputs;
    std::vector<OutputPort *> lane_outputs;
    std::vector<Lane> lanes;
    std::vector<std::optional<Assignment>> assignments;
    size_t num_finished_lanes = 0;
    /// Marks of the slices in the lanes' buffers: assigned and not yet taken by the merge in full.
    size_t issued_marks = 0;
    /// Marks of the slices read ahead and dropped without rows since the merge last asked.
    size_t fruitless_marks = 0;
    /// Slices that ended with most of their rows filtered out.
    size_t misses = 0;
    /// Slices a lane reads before its slices reach full size (1, 2, 4, ... marks), and their marks in total.
    size_t ramp_slices = 0;
    size_t ramp_marks = 0;
};

}
