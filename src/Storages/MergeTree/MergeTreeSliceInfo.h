#pragma once

#include <Processors/Chunk.h>

#include <vector>

namespace DB
{

/// Identifies a slice of MergeTreeReadPoolInOrderSliced: the part and the first mark of the slice.
/// Slices of a part never overlap, so the first mark is unique within the part.
struct MergeTreeSliceTag
{
    size_t part_index_in_query = 0;
    size_t first_mark = 0;
};

/// On every chunk with data a source emits while reading from MergeTreeReadPoolInOrderSliced. The
/// chunk goes to the buffer of that slice; the info is removed there and never reaches the merge.
class MergeTreeSliceDataInfo : public ChunkInfoCloneable<MergeTreeSliceDataInfo>
{
public:
    explicit MergeTreeSliceDataInfo(MergeTreeSliceTag slice_) : slice(slice_) {}
    MergeTreeSliceDataInfo(const MergeTreeSliceDataInfo &) = default;

    MergeTreeSliceTag slice;
};

/// On an empty chunk a source emits right after it asked the pool for its next slice: the slices that
/// ended since its previous marker (the task it finished, and slices the refiner emptied before they were
/// read), and whether the source got nothing to read. The chunk never reaches the merge.
class MergeTreeSliceMarkerInfo : public ChunkInfoCloneable<MergeTreeSliceMarkerInfo>
{
public:
    MergeTreeSliceMarkerInfo(std::vector<MergeTreeSliceTag> ended_, bool idle_) : ended(std::move(ended_)), idle(idle_) {}
    MergeTreeSliceMarkerInfo(const MergeTreeSliceMarkerInfo &) = default;

    std::vector<MergeTreeSliceTag> ended;
    bool idle;
};

}
