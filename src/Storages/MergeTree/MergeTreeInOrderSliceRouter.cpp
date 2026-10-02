#include <Storages/MergeTree/MergeTreeInOrderSliceRouter.h>

#include <Storages/MergeTree/MergeTreeSliceInfo.h>

namespace DB
{

MergeTreeInOrderSliceRouter::MergeTreeInOrderSliceRouter(SharedHeader header, std::shared_ptr<MergeTreeReadPoolInOrderSliced> pool_)
    : IProcessor(InputPorts(pool_->numSources(), header), OutputPorts(pool_->numLanes(), header))
    , pool(std::move(pool_))
    , input_needed(pool->numSources(), false)
{
    for (auto & input : inputs)
        source_inputs.push_back(&input);
    for (auto & output : outputs)
        lane_outputs.push_back(&output);
}

IProcessor::Status MergeTreeInOrderSliceRouter::prepare()
{
    for (size_t source = 0; source < source_inputs.size(); ++source)
    {
        auto & input = *source_inputs[source];
        if (!input.hasData())
            continue;

        Chunk chunk = input.pull();
        if (auto marker = chunk.getChunkInfos().get<MergeTreeSliceMarkerInfo>())
        {
            pool->completeSlices(*marker);
            /// An idle source is parked, unless slices were cut since it looked: then it stays needed
            /// and takes one when it asks again.
            if (marker->idle && pool->fifoSize() == 0)
            {
                input.setNotNeeded();
                input_needed[source] = false;
            }
        }
        else
            pool->deposit(std::move(chunk));
    }

    for (size_t lane = 0; lane < lane_outputs.size(); ++lane)
    {
        auto & output = *lane_outputs[lane];
        if (output.isFinished())
        {
            pool->finishLane(lane);
            continue;
        }
        if (!output.canPush())
            continue;

        auto served = pool->serve(lane, output.getHeader());
        if (served.finished)
            output.finish();
        else if (served.chunk)
            output.push(std::move(served.chunk));
    }

    if (pool->allLanesFinished())
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

    /// One parked source per slice waiting in the FIFO; sources still reading take theirs when done.
    size_t queued = pool->schedule();
    for (size_t source = 0; source < source_inputs.size() && queued > 0; ++source)
    {
        if (input_needed[source])
            continue;
        source_inputs[source]->setNeeded();
        input_needed[source] = true;
        --queued;
    }

    for (bool needed : input_needed)
        if (needed)
            return Status::NeedData;
    return Status::PortFull;
}

IProcessor::Status MergeTreeInOrderSliceRouter::finish()
{
    /// Sources still reading or about to read are cut short: no lane wants their rows anymore.
    if (pool->hasSlicesInFlight())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
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

}
