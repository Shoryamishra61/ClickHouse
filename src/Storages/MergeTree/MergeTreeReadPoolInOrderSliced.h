#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Storages/MergeTree/MergeTreeSliceInfo.h>

#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <unordered_map>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

/// Read pool for reading in the order of the primary key with more parallelism than one thread per part.
///
/// Every part is a lane. Slices are cut from the front of the lane's unread marks (the back in reverse
/// order), and a slice is one MergeTreeReadTask. Lanes with unread marks are queued by the primary key at
/// their next unread mark, so the head of the queue is the slice the merge needs next, whichever part it
/// belongs to. The slices of a lane start with one mark and double up to `min_marks_for_concurrent_read`:
/// the first rows of a part arrive after one granule, and a lane the merge stays on is read in large slices.
///
/// The pool owns all state and makes all decisions; MergeTreeInOrderSliceRouter only moves chunks between
/// its ports and the pool. `schedule` cuts slices into a FIFO when the merge asks for data. Sources take
/// them in `getTask`, read them, and tag every chunk with the slice (MergeTreeSliceDataInfo). The router
/// deposits the chunks here, where they are buffered per slice, and `serve` hands a lane's rows out from
/// its first slice in reading order, so the lane is in mark order whichever sources read it. When no rows
/// are ready, `serve` announces the key the lane's next rows start at, from the index, so the merge goes
/// on with other lanes until it reaches that key instead of waiting for a lane whose rows are filtered
/// out. A source reports the slices it finished with a marker (MergeTreeSliceMarkerInfo); a slice is
/// complete only then, because its last chunk may still be in the port when the source asks for the next.
///
/// Readers follow the lane, not the source: a source that switches lanes leaves its readers parked in the
/// lane for whichever source reads it next, and `getTask` prefers a queued slice of the lane the source
/// read last. Readers are created for the marks of one slice, like the readers of the other pools are
/// created for one task, so their buffers are sized by the slice. When the slices of a lane have grown
/// well past the size its readers were made for, new readers replace them.
class MergeTreeReadPoolInOrderSliced : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolInOrderSliced(
        RangesInDataParts parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const IndexReadTasks & index_read_tasks_,
        const StorageSnapshotPtr & storage_snapshot_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_,
        size_t num_sources_,
        const Block & primary_key_header_,
        ExpressionActionsPtr virtual_row_conversions_,
        bool read_in_reverse_order_);

    String getName() const override { return "ReadPoolInOrderSliced"; }
    bool preservesOrderOfRanges() const override { return true; }
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

    size_t numSources() const { return num_sources; }
    size_t numLanes() const { return num_lanes; }

    /// Source side.

    /// The next slice from the FIFO, preferring one of the lane the source read last. Nothing if the FIFO
    /// is empty: the source then reports idle and is parked until the router has a slice for it.
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

    /// Slices that ended in `getTask` of the source without being read: emptied by the refiner, or of a
    /// lane the merge finished. The source reports them in its next marker, with the task it finished.
    std::vector<MergeTreeSliceTag> takeSlicesEndedUnread(size_t source);

    /// No slice is going to be cut anymore; a source that finds no task ends its stream.
    bool isFinished() const;

    /// Router side.

    /// A chunk with data from a source goes to the buffer of the slice in its tag. Chunks of a finished
    /// lane are dropped; a tag of no issued slice is a logical error.
    void deposit(Chunk chunk);

    /// The source's marker: the listed slices are complete, no more chunks of them are coming.
    void completeSlices(const MergeTreeSliceMarkerInfo & marker);

    struct Served
    {
        /// Rows or an announcement; empty when nothing is ready and the merge has to wait.
        Chunk chunk = {};
        bool finished = false;
    };

    /// Called when the lane's output can take a chunk, which is how the merge asks for the lane.
    Served serve(size_t lane, const Block & output_header);

    /// Cuts slices into the FIFO (see the rules in the .cpp); returns the number of slices waiting there.
    size_t schedule();

    size_t fifoSize() const;
    bool allLanesFinished() const;

    /// Slices cut or being read: a source is busy with something, or will be as soon as it looks.
    bool hasSlicesInFlight() const;

    /// The lane is not going to be read anymore: its unread marks, buffers, queued slices and parked
    /// readers go away.
    void finishLane(size_t lane);

    void finish();

    /// Primary key values at the mark of the lane, one row; empty if the index has no value there.
    Block keyAtMark(size_t lane, size_t mark) const;

    /// Keys that are not known (empty blocks) go last.
    static int compareKeys(const Block & lhs, const Block & rhs, bool reverse = false);

private:
    /// Readers and the number of marks of the slice they were created for, which sized their buffers.
    struct SizedReaders
    {
        MergeTreeReadTask::Readers readers;
        size_t marks;
    };

    /// A slice from the cut until the merge consumed it.
    struct Slice
    {
        /// Marks to read; moved out when a source takes the slice.
        MarkRanges ranges = {};
        /// Marks when cut, counted in `issued_marks` until the slice is consumed or dropped.
        size_t marks = 0;
        /// Rows in those marks before any filtering, to tell a slice whose rows were mostly filtered out.
        size_t rows_in_marks = 0;
        size_t rows_received = 0;
        /// The mark the rows of the slice start at in reading order; announced while the slice is in flight.
        size_t boundary_mark = 0;
        std::deque<Chunk> chunks = {};
        bool taken = false;
        bool complete = false;
        bool had_rows = false;
    };

    using Slices = std::map<size_t, Slice>;

    struct Lane
    {
        MarkRanges unread;
        /// Slices of a lane start small and grow, so the first rows of a part arrive quickly.
        size_t slices_cut = 0;
        /// The ranges refiner was applied to the whole lane.
        bool refined = false;
        /// Readers of sources that moved on to other lanes.
        std::vector<SizedReaders> parked_readers = {};
        /// Issued slices by first mark.
        Slices slices = {};
        /// The primary key announced to the merge last; empty before the first announcement.
        Block announced_key = {};
        /// The merge waits for this lane right now and nothing is ready for it.
        bool wants_data = false;
        bool finished = false;
    };

    /// A lane with unread marks, ordered by the primary key at its next unread mark in reading order.
    struct QueuedLane
    {
        Block key;
        size_t lane;
    };

    struct QueuedLaneLess
    {
        bool reverse;
        bool operator()(const QueuedLane & lhs, const QueuedLane & rhs) const;
    };

    using LaneQueue = std::set<QueuedLane, QueuedLaneLess>;

    struct QueuedSlice
    {
        size_t lane;
        size_t first_mark;
    };

    size_t laneOf(const MergeTreeSliceTag & tag) const;

    size_t nextSliceMarks(size_t lane) const TSA_REQUIRES(mutex);
    void enqueueLane(size_t lane) TSA_REQUIRES(mutex);
    void dequeueLane(size_t lane) TSA_REQUIRES(mutex);
    std::optional<size_t> nextLane() const TSA_REQUIRES(mutex);
    std::optional<size_t> nextLaneBefore(size_t lane) const TSA_REQUIRES(mutex);
    std::optional<size_t> nextUnreadMark(const Lane & lane) const TSA_REQUIRES(mutex);
    Slices::iterator headSlice(Lane & lane) const TSA_REQUIRES(mutex);
    void cutSlice(size_t lane) TSA_REQUIRES(mutex);
    void dropSlice(size_t lane, Slices::iterator slice) TSA_REQUIRES(mutex);
    void finishLaneUnlocked(size_t lane) TSA_REQUIRES(mutex);
    Chunk announce(size_t lane, size_t mark, const Block & output_header) TSA_REQUIRES(mutex);
    size_t readAheadMarks() const TSA_REQUIRES(mutex);
    size_t idleSources() const TSA_REQUIRES(mutex);

    const RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    const size_t num_sources;
    const size_t num_lanes;
    const size_t max_slice_marks;
    /// Marks of the slices a lane reads before its slices reach full size (1, 2, 4, ... marks).
    size_t ramp_marks = 0;
    const Block primary_key_header;
    const ExpressionActionsPtr virtual_row_conversions;
    const bool reverse;
    std::unordered_map<size_t, size_t> lane_by_part_index;

    mutable std::mutex mutex;
    std::vector<Lane> lanes TSA_GUARDED_BY(mutex);
    LaneQueue queue TSA_GUARDED_BY(mutex);
    /// Where each lane with unread marks sits in the queue.
    std::vector<std::optional<LaneQueue::iterator>> queue_position TSA_GUARDED_BY(mutex);
    /// Slices cut and not yet taken by a source, in the order they were cut.
    std::deque<QueuedSlice> fifo TSA_GUARDED_BY(mutex);
    /// The lane of the last task each source got, i.e. the lane its current readers belong to, and the
    /// marks those readers were created for.
    std::vector<std::optional<size_t>> last_task_lane TSA_GUARDED_BY(mutex);
    std::vector<size_t> last_readers_marks TSA_GUARDED_BY(mutex);
    std::vector<std::vector<MergeTreeSliceTag>> ended_unread TSA_GUARDED_BY(mutex);
    /// Slices taken by sources and not yet complete.
    size_t taken_slices TSA_GUARDED_BY(mutex) = 0;
    size_t num_finished_lanes TSA_GUARDED_BY(mutex) = 0;
    /// Marks of the slices cut and not yet consumed by the merge in full.
    size_t issued_marks TSA_GUARDED_BY(mutex) = 0;
    /// Marks of the slices read ahead and dropped without rows since the merge last asked.
    size_t fruitless_marks TSA_GUARDED_BY(mutex) = 0;
    /// Marks of the slices the merge has consumed: taken in full, or dropped without rows.
    size_t consumed_marks TSA_GUARDED_BY(mutex) = 0;
    /// A slice ended with most of its rows filtered out: reading, not merging, is the bottleneck.
    bool has_miss TSA_GUARDED_BY(mutex) = false;
    /// The merge asked for a lane anew since the last `schedule`.
    bool merge_asked TSA_GUARDED_BY(mutex) = false;
    bool finished TSA_GUARDED_BY(mutex) = false;
};

}
