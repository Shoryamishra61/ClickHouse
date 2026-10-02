#include <Storages/MergeTree/MergeTreeInOrderSliceRouter.h>

#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Processors/Port.h>
#include <Storages/MergeTree/MergeTreeSliceEndInfo.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeInOrderSliceRouter::MergeTreeInOrderSliceRouter(
    SharedHeader header,
    std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_,
    ExpressionActionsPtr virtual_row_conversions_)
    : IProcessor(InputPorts(pool_->numSources(), header), OutputPorts(pool_->numLanes(), header))
    , pool(std::move(pool_))
    , virtual_row_conversions(std::move(virtual_row_conversions_))
    , lanes(pool->numLanes())
    , assignments(pool->numSources())
{
    for (auto & input : inputs)
        source_inputs.push_back(&input);
    for (auto & output : outputs)
        lane_outputs.push_back(&output);

    size_t ramp_slices = 0;
    while ((size_t(1) << ramp_slices) < pool->maxSliceMarks())
        ++ramp_slices;
    ramp_marks = (size_t(1) << ramp_slices) - 1;
}

IProcessor::Status MergeTreeInOrderSliceRouter::prepare()
{
    for (size_t source = 0; source < source_inputs.size(); ++source)
        if (source_inputs[source]->hasData())
            consumeInput(source);

    bool merge_asked = false;
    for (size_t lane = 0; lane < lanes.size(); ++lane)
        merge_asked |= pushToLane(lane);

    /// An ask is the merge's progress: what was read ahead and found empty since the last one no longer
    /// counts against the budget.
    if (merge_asked)
        fruitless_marks = 0;

    if (num_finished_lanes == lanes.size())
        return finish();

    /// A source ends its stream on its own only when reading was cancelled for a partial result. The
    /// rows of its slice are not coming, so no lane can be completed in order anymore: end them all,
    /// the way a cancelled source ends its stream.
    for (const auto & input : inputs)
    {
        if (input.isFinished())
        {
            for (auto & output : outputs)
                output.finish();
            for (auto & other : inputs)
                other.close();
            return Status::Finished;
        }
    }

    scheduleSlices(merge_asked);

    for (const auto & assignment : assignments)
        if (assignment)
            return Status::NeedData;
    return Status::PortFull;
}

IProcessor::Status MergeTreeInOrderSliceRouter::finish()
{
    /// A source still reading a slice is cut short: no lane wants its rows anymore.
    for (const auto & assignment : assignments)
    {
        if (assignment)
        {
            for (auto & input : inputs)
                input.close();
            return Status::Finished;
        }
    }

    /// Idle sources end their streams themselves once the pool has nothing more for them, so that
    /// they finish the way every source does (onFinish: statistics and logs).
    pool->finish();
    bool all_sources_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;
        all_sources_finished = false;
        input.setNeeded();
    }
    return all_sources_finished ? Status::Finished : Status::NeedData;
}

void MergeTreeInOrderSliceRouter::consumeInput(size_t source)
{
    auto & input = *source_inputs[source];
    Chunk chunk = input.pull();
    const bool slice_ended = chunk.getChunkInfos().get<MergeTreeSliceEndInfo>() != nullptr;

    auto & assignment = assignments[source];
    if (!assignment)
    {
        /// A source without a slice can only report that it has nothing to read.
        if (!slice_ended)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got data from source {} that has no slice assigned", source);
        input.setNotNeeded();
        return;
    }

    /// The source asked the pool before this slice was assigned and found nothing: the report is stale,
    /// the source stays needed to pick the slice up.
    if (slice_ended && pool->hasPendingSlice(source))
        return;

    auto & lane = lanes[assignment->lane];

    /// The merge finished the lane while the slice was being read: nothing waits for its rows.
    if (lane.finished)
    {
        if (slice_ended)
        {
            input.setNotNeeded();
            assignment.reset();
        }
        return;
    }

    auto slice = lane.slices.find(assignment->first_mark);
    if (slice == lane.slices.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of lane {} is not issued", assignment->first_mark, assignment->lane);

    if (!slice_ended)
    {
        assignment->rows_read += chunk.getNumRows();
        /// A chunk without rows carries nothing the merge can use, unless it is a virtual row of the source.
        if (chunk.getNumRows() == 0 && !isVirtualRow(chunk))
            return;
        if (chunk.getNumRows() > 0)
            slice->second.had_rows = true;
        slice->second.chunks.push_back(std::move(chunk));
        return;
    }

    slice->second.finished = true;
    if (slice->second.chunks.empty())
        dropSlice(assignment->lane, slice);
    input.setNotNeeded();

    /// Most rows of the slice were filtered out: reading is the bottleneck, not merging.
    if (assignment->rows_read * 4 < assignment->rows_in_marks)
        has_miss = true;

    assignment.reset();
}

void MergeTreeInOrderSliceRouter::dropSlice(size_t lane_idx, SliceBuffers::iterator slice)
{
    issued_marks -= slice->second.marks;
    consumed_marks += slice->second.marks;
    /// Read ahead and found empty: held against the budget until the merge asks again. The lane the merge
    /// waits for is what it needs next, so its empty slices are not held.
    if (!slice->second.had_rows && !lanes[lane_idx].wants_data)
        fruitless_marks += slice->second.marks;
    lanes[lane_idx].slices.erase(slice);
}

void MergeTreeInOrderSliceRouter::finishLane(size_t lane_idx)
{
    auto & lane = lanes[lane_idx];
    lane.finished = true;
    ++num_finished_lanes;
    for (const auto & [first_mark, slice] : lane.slices)
        issued_marks -= slice.marks;
    lane.slices.clear();
    pool->finishLane(lane_idx);
}

bool MergeTreeInOrderSliceRouter::pushToLane(size_t lane_idx)
{
    auto & lane = lanes[lane_idx];
    auto & output = *lane_outputs[lane_idx];
    const bool was_waiting = lane.wants_data;
    lane.wants_data = false;
    if (lane.finished)
        return false;

    if (output.isFinished())
    {
        finishLane(lane_idx);
        return false;
    }

    if (!output.canPush())
        return false;

    /// Rows leave a lane through its first issued slice only, so the lane stays in reading order.
    auto head = headSlice(lane);
    if (head != lane.slices.end() && !head->second.chunks.empty())
    {
        Chunk chunk = std::move(head->second.chunks.front());
        head->second.chunks.pop_front();
        if (head->second.finished && head->second.chunks.empty())
            dropSlice(lane_idx, head);
        output.push(std::move(chunk));
        return !was_waiting;
    }

    /// Nothing is ready: the rows of the slice in flight start at its boundary mark, else the lane's rows
    /// start at its next unread mark.
    std::optional<size_t> next_mark;
    if (head != lane.slices.end())
        next_mark = head->second.boundary_mark;
    else
        next_mark = pool->nextUnreadMark(lane_idx);

    if (!next_mark)
    {
        output.finish();
        finishLane(lane_idx);
        return false;
    }

    if (!announce(lane_idx, *next_mark))
        lane.wants_data = true;
    return !was_waiting;
}

MergeTreeInOrderSliceRouter::SliceBuffers::iterator MergeTreeInOrderSliceRouter::headSlice(Lane & lane) const
{
    if (lane.slices.empty())
        return lane.slices.end();
    return pool->readsInReverseOrder() ? std::prev(lane.slices.end()) : lane.slices.begin();
}

bool MergeTreeInOrderSliceRouter::announce(size_t lane_idx, size_t mark)
{
    if (!virtual_row_conversions)
        return false;

    auto & lane = lanes[lane_idx];
    Block key = pool->keyAtMark(lane_idx, mark);
    if (key.columns() == 0 || MergeTreeReadPoolInOrderSliced::compareKeys(key, lane.announced_key) == 0)
        return false;

    const auto & header = outputs.front().getHeader();
    Chunk chunk(header.cloneEmptyColumns(), 0);
    chunk.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(/*part_level=*/ 0, key, virtual_row_conversions));
    lane_outputs[lane_idx]->push(std::move(chunk));
    lane.announced_key = std::move(key);
    return true;
}

size_t MergeTreeInOrderSliceRouter::readAheadMarks() const
{
    /// On remote storage every round of slices is a round trip, so the rest of the ramp is read in one round
    /// rather than slice by slice. From there the depth follows the merge's progress.
    if (!has_miss)
        return 0;
    return std::min(assignments.size() * pool->maxSliceMarks(), std::max(ramp_marks, 4 * consumed_marks));
}

std::optional<size_t> MergeTreeInOrderSliceRouter::pickIdleSource(size_t lane) const
{
    /// A source that read the lane last still holds its readers, so it continues the lane for free.
    std::optional<size_t> idle;
    for (size_t source = 0; source < assignments.size(); ++source)
    {
        if (assignments[source])
            continue;
        if (pool->lastTaskLane(source) == lane)
            return source;
        if (!idle)
            idle = source;
    }
    return idle;
}

void MergeTreeInOrderSliceRouter::assignSlice(size_t source, size_t lane_idx)
{
    auto description = pool->assignSlice(source, lane_idx);

    auto [it, inserted] = lanes[lane_idx].slices.try_emplace(description.first_mark);
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Slice starting at mark {} of lane {} was assigned twice", description.first_mark, lane_idx);

    it->second.marks = description.marks;
    it->second.boundary_mark = pool->readsInReverseOrder() ? description.end_mark : description.first_mark;
    issued_marks += description.marks;
    assignments[source] = Assignment{.lane = lane_idx, .first_mark = description.first_mark, .rows_in_marks = description.rows};
    source_inputs[source]->setNeeded();
}

void MergeTreeInOrderSliceRouter::scheduleSlices(bool merge_asked)
{
    /// The lane the merge is blocked on is read whatever the read-ahead depth: those rows are never waste.
    bool merge_waits = false;
    for (size_t lane = 0; lane < lanes.size(); ++lane)
    {
        if (!lanes[lane].wants_data)
            continue;

        merge_waits = true;
        if (!lanes[lane].slices.empty() || !pool->laneHasUnreadMarks(lane))
            continue;

        auto source = pickIdleSource(lane);
        if (!source)
            return;
        assignSlice(*source, lane);

        /// Lanes whose next key lies within the slice just issued are consumed before that slice is done:
        /// reading them now costs no more rows than waiting for the merge to ask for each of them in turn.
        while (auto before = pool->nextLaneBefore(lane))
        {
            source = pickIdleSource(*before);
            if (!source)
                return;
            assignSlice(*source, *before);
        }
    }

    /// Read-ahead only on the merge's demand: a merge that stops after the chunk it just took (a LIMIT)
    /// must not trigger reads it never needs.
    if (!merge_asked && !merge_waits)
        return;

    /// Read ahead in the order the merge is going to need the data, never past the budget.
    const size_t budget = readAheadMarks();
    while (auto lane = pool->nextLane())
    {
        if (issued_marks + fruitless_marks + pool->nextSliceMarks(*lane) > budget)
            return;

        auto source = pickIdleSource(*lane);
        if (!source)
            return;
        assignSlice(*source, *lane);
    }
}

}
