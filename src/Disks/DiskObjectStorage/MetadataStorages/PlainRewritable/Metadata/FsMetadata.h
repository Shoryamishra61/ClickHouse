#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <Common/CurrentMetrics.h>
#include <Common/MultiVersion.h>

#include <base/defines.h>

namespace DB
{

class BlobLinkCounts;

/// The committed state of the plain-rewritable metadata: the tree of directories and files, and the numbers of links to the blobs.
class FsMetadata
{
public:
    FsMetadata(CurrentMetrics::Metric metric_directories_name, CurrentMetrics::Metric metric_files_name);

    void applySnapshot(std::shared_ptr<FsSnapshot> snapshot);
    void applyLayout(std::unordered_map<std::string, DirectoryRemoteInfo> remote_layout);

    std::shared_ptr<FsSnapshot> takeReadWriteSnapshot() const;
    std::shared_ptr<const FsSnapshot> takeReadOnlySnapshot() const;
    /// The committed tree together with its version, taken atomically.
    std::pair<std::shared_ptr<const FsSnapshot>, UInt64> takeReadOnlySnapshotWithVersion() const;

    /// The version is incremented every time a new tree is applied.
    UInt64 getVersion() const;

private:
    mutable std::mutex mutex;
    const std::shared_ptr<BlobLinkCounts> blob_link_counts;
    std::shared_ptr<FsSnapshot> latest_snapshot TSA_GUARDED_BY(mutex);
    UInt64 version TSA_GUARDED_BY(mutex) = 0;
    mutable CurrentMetrics::Increment remote_layout_directories_count TSA_GUARDED_BY(mutex);
    mutable CurrentMetrics::Increment remote_layout_files_count TSA_GUARDED_BY(mutex);
};

}
