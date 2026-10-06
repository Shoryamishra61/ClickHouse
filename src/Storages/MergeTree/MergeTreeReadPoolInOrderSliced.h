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
/// belongs to. The slices of a lane start with one granule and double up to `min_marks_for_concurrent_read`:
/// the first rows of a part arrive after one granule, and a lane the merge stays on is read in large
/// slices.
///
/// The pool owns all state and makes all decisions; MergeTreeInOrderSliceRouter only moves chunks between
/// its ports and the pool. `schedule` cuts slices into a FIFO when the merge asks for data, and names the
/// parked sources to wake for them. Sources take them in `getTask`, read them, and tag every chunk with
/// the slice (MergeTreeSliceInfo). The router hands
/// the chunks to `receive`, which buffers them per slice, and `serve` gives a lane's rows out from its
/// first slice in reading order, so the lane is in mark order whichever sources read it. When no rows are
/// ready, `serve` announces the key the lane's next rows start at, from the index, so the merge goes on
/// with other lanes until it reaches that key instead of waiting for a lane whose rows are filtered out.
/// A source reports the slice it finished in the chunk it emits after asking for the next one; a slice is
/// complete only then, because its last chunk may still be in the port when the source asks.
///
/// Several sources read one lane at a time when its slices carry enough data to be worth a request each:
/// that is the parallelism within a part the pool exists for. How much a slice carries is what the
/// finished slices of the query read (the sources report it with their markers): a filter may drop every
/// row at its first, cheap column and never read the rest, or let every row through. Slices below one
/// compressed block are read by one source, slice after slice, through the readers it holds: readers
/// follow the lane, not the source, `getTask` prefers a queued slice of the lane the source read last, and
/// `schedule` wakes the parked source that holds a lane's readers. Readers are created for the marks of
/// one slice, like the readers of the other pools are created for one task, so their buffers are sized by
/// the slice.
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
    bool mayHaveMoreTasks() const override;

    /// Router side.

    /// A chunk from a source. Its rows go to the buffer of their slice; the slice it names as ended is
    /// complete, no more chunks of it are coming. Chunks of a finished lane are dropped.
    void receive(Chunk chunk);

    struct Served
    {
        /// Rows or an announcement; empty when nothing is ready and the merge has to wait.
        Chunk chunk = {};
        bool finished = false;
    };

    /// Called when the lane's output can take a chunk, which is how the merge asks for the lane.
    Served serve(size_t lane, const Block & output_header);

    /// Cuts slices into the FIFO (see the rules in the .cpp), at most as many as there are parked sources
    /// to take them, and returns the parked sources to wake for the slices waiting there: for each, the one
    /// that read the slice's lane last if it is among them, since it holds the lane's readers.
    std::vector<size_t> schedule(const std::vector<size_t> & parked_sources);
    size_t fifoSize() const;

    /// The lane is not going to be read anymore: its unread marks, buffers, queued slices and parked
    /// readers go away.
    void finishLane(size_t lane);

    /// No slice is going to be cut anymore; a source that finds no task ends its stream.
    void finish();

    /// Primary key values at the mark of the lane, one row; empty if the index has no value there.
    Block keyAtMark(size_t lane, size_t mark) const;

    /// Keys that are not known (empty blocks) go last.
    static int compareKeys(const Block & lhs, const Block & rhs, bool reverse = false);

private:
    /// A slice from the cut until the merge consumed it.
    struct Slice
    {
        /// Marks to read; moved out when a source takes the slice.
        MarkRanges ranges = {};
        /// Marks when cut, counted in `issued_marks` until the slice is consumed or dropped.
        size_t marks = 0;
        size_t rows_received = 0;
        /// The mark the rows of the slice start at in reading order; announced while the slice is in flight.
        size_t boundary_mark = 0;
        std::deque<Chunk> chunks = {};
        bool complete = false;
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
        std::vector<MergeTreeReadTask::Readers> parked_readers = {};
        /// Issued slices by first mark.
        Slices slices = {};
        /// Marks of the slices with rows the merge took in full since it last asked for the lane. They
        /// become consumed when it asks again: then it went through them and wanted more.
        size_t taken_marks = 0;
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
    /// Whether the lane may get a slice while it has slices in flight: its next slice must be worth a
    /// request of its own.
    bool canShare(size_t lane) const TSA_REQUIRES(mutex);
    void enqueueLane(size_t lane) TSA_REQUIRES(mutex);
    void dequeueLane(size_t lane) TSA_REQUIRES(mutex);
    std::optional<size_t> nextLane() const TSA_REQUIRES(mutex);
    std::optional<size_t> nextLaneBefore(size_t lane) const TSA_REQUIRES(mutex);
    /// A lane with slices in flight lies after the position in key order.
    bool inFlightAfter(LaneQueue::const_iterator position) const TSA_REQUIRES(mutex);
    std::optional<size_t> nextUnreadMark(const Lane & lane) const TSA_REQUIRES(mutex);
    Slices::iterator headSlice(Lane & lane) const TSA_REQUIRES(mutex);
    void cutSlice(size_t lane) TSA_REQUIRES(mutex);
    void completeSlice(const MergeTreeSliceTag & tag, size_t bytes_read) TSA_REQUIRES(mutex);
    void dropSlice(size_t lane, Slices::iterator slice) TSA_REQUIRES(mutex);
    void finishLaneUnlocked(size_t lane) TSA_REQUIRES(mutex);
    Chunk announce(size_t lane, size_t mark, const Block & output_header) TSA_REQUIRES(mutex);
    size_t readAheadMarks() const TSA_REQUIRES(mutex);

    const RuntimeDataflowStatisticsCacheUpdaterPtr updater;
    const size_t num_sources;
    const size_t num_lanes;
    const size_t max_slice_marks;
    /// Bytes a slice reads below which it is not worth a request of its own.
    const size_t min_slice_bytes_to_share;
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
    /// The lane of the last task each source got, i.e. the lane its current readers belong to.
    std::vector<std::optional<size_t>> last_task_lane TSA_GUARDED_BY(mutex);
    /// Marks of the slices cut and not yet consumed by the merge in full.
    size_t issued_marks TSA_GUARDED_BY(mutex) = 0;
    /// Marks of the slices the merge went through and asked past, and of the slices that came back
    /// without any rows. Both are the evidence the read-ahead depth follows, see readAheadMarks.
    size_t consumed_marks TSA_GUARDED_BY(mutex) = 0;
    size_t empty_marks TSA_GUARDED_BY(mutex) = 0;
    /// Bytes the finished slices read and their marks: what a mark costs to read in this query, see canShare.
    size_t read_bytes TSA_GUARDED_BY(mutex) = 0;
    size_t read_marks TSA_GUARDED_BY(mutex) = 0;
    /// The merge asked for a lane anew since the last `schedule`.
    bool merge_asked TSA_GUARDED_BY(mutex) = false;
    /// Some lane was read to its end and the merge asked for more: the query is not answered within the
    /// lanes it started on, see `schedule`.
    bool query_spans_lanes TSA_GUARDED_BY(mutex) = false;
    bool finished TSA_GUARDED_BY(mutex) = false;
};

}
