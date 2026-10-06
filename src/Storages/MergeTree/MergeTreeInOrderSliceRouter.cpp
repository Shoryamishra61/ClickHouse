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
        const auto info = chunk.getChunkInfos().get<MergeTreeSliceInfo>();
        const bool idle = info && info->idle;
        pool->receive(std::move(chunk));

        /// An idle source is parked, unless slices were cut since it looked: then it stays needed and
        /// takes one when it asks again.
        if (idle && pool->fifoSize() == 0)
        {
            input.setNotNeeded();
            input_needed[source] = false;
        }
    }

    bool all_lanes_finished = true;
    for (size_t lane = 0; lane < lane_outputs.size(); ++lane)
    {
        auto & output = *lane_outputs[lane];
        if (output.isFinished())
        {
            pool->finishLane(lane);
            continue;
        }
        if (output.canPush())
        {
            auto served = pool->serve(lane, output.getHeader());
            if (served.finished)
                output.finish();
            else if (served.chunk)
                output.push(std::move(served.chunk));
        }
        all_lanes_finished &= output.isFinished();
    }

    if (all_lanes_finished)
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

    /// The pool names a parked source per slice waiting in its FIFO; sources still reading take theirs
    /// when done.
    std::vector<size_t> parked;
    for (size_t source = 0; source < source_inputs.size(); ++source)
        if (!input_needed[source])
            parked.push_back(source);
    for (size_t source : pool->schedule(parked))
    {
        source_inputs[source]->setNeeded();
        input_needed[source] = true;
    }

    for (bool needed : input_needed)
        if (needed)
            return Status::NeedData;
    return Status::PortFull;
}

IProcessor::Status MergeTreeInOrderSliceRouter::finish()
{
    if (!finishing)
    {
        /// Sources still reading are cut short: no lane wants their rows anymore.
        for (bool needed : input_needed)
        {
            if (needed)
            {
                for (auto & input : inputs)
                    input.close();
                return Status::Finished;
            }
        }

        /// Idle sources end their streams themselves once the pool has nothing more for them, so that
        /// they finish the way every source does (onFinish: statistics and logs).
        finishing = true;
        pool->finish();
        for (auto & input : inputs)
            if (!input.isFinished())
                input.setNeeded();
    }

    for (const auto & input : inputs)
        if (!input.isFinished())
            return Status::NeedData;
    return Status::Finished;
}

}
