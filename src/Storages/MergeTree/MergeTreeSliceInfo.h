#pragma once

#include <Processors/Chunk.h>

#include <optional>

namespace DB
{

/// Identifies a slice of MergeTreeReadPoolInOrderSliced: the part and the first mark of the slice.
/// Slices of a part never overlap, so the first mark is unique within the part.
struct MergeTreeSliceTag
{
    size_t part_index_in_query = 0;
    size_t first_mark = 0;
};

/// On every chunk a source emits while reading from MergeTreeReadPoolInOrderSliced. A chunk with rows
/// (or a virtual row) carries the slice its rows belong to. The empty chunk a source emits right after
/// it asked the pool for its next slice carries the slice of the task it finished, if any, with the
/// bytes it read, and whether it got nothing to read. The pool takes the info off; nothing of it reaches
/// the merge.
class MergeTreeSliceInfo : public ChunkInfoCloneable<MergeTreeSliceInfo>
{
public:
    MergeTreeSliceInfo() = default;
    MergeTreeSliceInfo(const MergeTreeSliceInfo &) = default;

    std::optional<MergeTreeSliceTag> slice;
    std::optional<MergeTreeSliceTag> ended;
    size_t ended_bytes = 0;
    bool idle = false;
};

}
