#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PlainRewritableSnapshotFile.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PrefixPath.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/UncommittedState.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Disks/WriteMode.h>
#include <Core/BackgroundSchedulePool.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_set>
#include <vector>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/SharedThreadPools.h>
#include <Poco/Timestamp.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>
#include <Common/thread_local_rng.h>
#include <Common/threadPoolCallbackRunner.h>

#if USE_AZURE_BLOB_STORAGE
#    include <azure/storage/common/storage_exception.hpp>
#endif

namespace ProfileEvents
{
    extern const Event DiskPlainRewritableLegacyLayoutDiskCount;
    extern const Event DiskPlainRewritableSnapshotRead;
    extern const Event DiskPlainRewritableSnapshotWritten;
    extern const Event DiskPlainRewritableSnapshotWriteFailed;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace FailPoints
{
    extern const char plain_rewritable_object_storage_azure_not_found_on_init[];
}

namespace
{

fs::path normalizeDirectoryPath(const fs::path & path)
{
    return path / "";
}

std::optional<std::string> getBlobKeyIfExists(const FsSnapshot & snapshot, const NormalizedPath & path)
{
    const auto directory = snapshot.getDirectoryRemoteInfo(path.parent_path());
    if (!directory)
        return std::nullopt;

    const auto it = directory->files.find(path.filename());
    if (it == directory->files.end())
        return std::nullopt;

    return getBlobKey(*directory, path.filename(), it->second);
}

/// The pages of one listing can only be fetched one after another, so enumerating a disk that holds
/// hundreds of thousands of objects takes hundreds of sequential requests, and no amount of threads
/// makes it faster. Directory names in this layout are random strings produced by `getRandomASCIIString`,
/// so grouping them by their first characters splits the key space into nearly equal parts that can be
/// listed in parallel. This alphabet must stay in sync with `getRandomASCIIString`; names outside of it
/// (written by another implementation) are still enumerated, by the two boundary shards below.
constexpr std::string_view DIRECTORY_NAME_ALPHABET = "abcdefghijklmnopqrstuvwxyz";

/// The point of the sharded listing is to replace many sequential pages with a few parallel ones,
/// so it only pays off once a plain listing would need many pages. Until then a plain listing costs
/// a single request while a sharded one costs one request per shard.
constexpr size_t MIN_DIRECTORIES_TO_LIST_IN_PARALLEL = 4096;

/// A listing returns at most `list_object_keys_size` (1000 by default) objects per request, so this
/// is how many directories a single shard should cover for its listing to fit in about one page.
/// It only controls how the work is split: a shard that turns out bigger is simply paginated.
constexpr size_t DIRECTORIES_PER_LISTING_SHARD = 64;

/// A longer prefix means more shards; 26^3 shards would be far more requests than pages to fetch.
constexpr size_t MAX_SHARD_PREFIX_LENGTH = 3;

/// A `__meta/{directory}/prefix.path` object, which is what makes a directory of the disk exist.
struct DirectoryObject
{
    std::string object_path;
    std::string remote_path;
    std::optional<ObjectMetadata> metadata;
    /// The entry of the base with the same `prefix.path`: the local path is taken from it without reading the object.
    const PlainRewritableRemoteLayout::value_type * base_entry = nullptr;
};

/// The outcome of loading one directory. `loaded` stays false for a directory that disappeared while
/// it was being read, which is not an error: it is simply absent from the resulting layout.
struct DirectoryLoadResult
{
    bool loaded = false;
    std::string local_path;
    DirectoryRemoteInfo info;
    /// The state of the directory in the base, if its `prefix.path` did not change.
    const DirectoryRemoteInfo * base_info = nullptr;
};

/// Split `names` into groups by their common prefix, so that listing every returned prefix in turn
/// enumerates exactly the same objects as listing the whole directory, but in `target_count` requests
/// that can run in parallel.
std::vector<std::string> makeListingShards(const std::vector<std::string> & names, size_t target_count)
{
    std::unordered_set<std::string> prefixes;
    for (size_t length = 1; length <= MAX_SHARD_PREFIX_LENGTH; ++length)
    {
        prefixes.clear();
        for (const auto & name : names)
            prefixes.emplace(name, 0, length);
        if (prefixes.size() >= target_count)
            break;
    }
    return {prefixes.begin(), prefixes.end()};
}

DirectoryRemoteInfo makeRootDirectoryInfo()
{
    return DirectoryRemoteInfo{PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, "fake_etag", 0, {}};
}

/// Only the root directory without files.
bool isEmptyLayout(const PlainRewritableRemoteLayout & remote_layout)
{
    if (remote_layout.empty())
        return true;
    if (remote_layout.size() > 1)
        return false;
    const auto & [path, info] = *remote_layout.begin();
    return path.empty() && info.files.empty();
}

/// How soon a failed snapshot write is retried, unless the configured delay is larger.
constexpr UInt64 SNAPSHOT_WRITE_RETRY_DELAY_MS = 1000;

}

PlainRewritableRemoteLayout MetadataStorageFromPlainRewritableObjectStorage::getCurrentLayout() const
{
    return getLayoutOf(*fs.takeReadOnlySnapshot());
}

PlainRewritableRemoteLayout MetadataStorageFromPlainRewritableObjectStorage::getLayoutOf(const FsSnapshot & tree)
{
    PlainRewritableRemoteLayout result;
    for (auto & [path, info] : tree.getSubtreeRemoteInfo(""))
    {
        /// Virtual directories (created only as parents of the real ones) are not stored anywhere.
        if (info)
            result.emplace(path, std::move(*info));
    }
    return result;
}

bool MetadataStorageFromPlainRewritableObjectStorage::isSnapshotWriter() const
{
    return snapshot_settings.enabled && !object_storage->isReadOnly();
}

MetadataStorageFromPlainRewritableObjectStorage::SnapshotFileContents MetadataStorageFromPlainRewritableObjectStorage::tryReadSnapshotFile(
    const LoggerPtr & log) const
{
    SnapshotFileContents result;
    const auto key = layout->constructSnapshotObjectKey();

    const auto metadata = object_storage->tryGetObjectMetadata(key, /*with_tags=*/ false);
    if (!metadata)
    {
        LOG_DEBUG(log, "There is no snapshot file '{}'", key);
        return result;
    }

    result.exists = true;

    try
    {
        auto read_settings = getReadSettings();
        read_settings.enable_filesystem_cache = false;

        auto in = object_storage->readObject(StoredObject(key, /*local_path*/ "", metadata->size_bytes), read_settings, metadata->size_bytes);
        result.layout = readPlainRewritableSnapshot(*in);
        LOG_DEBUG(log, "Read the snapshot file '{}' ({} bytes) with {} directories", key, metadata->size_bytes, result.layout->size());
    }
    catch (...)
    {
        /// The snapshot is only a copy of the state that can always be rebuilt from the object storage, so a file that
        /// cannot be read (e.g. written by a newer version in a newer format, or removed just now) does not make the disk unusable.
        tryLogCurrentException(log, fmt::format("Cannot read the snapshot file '{}', the state will be loaded by listing the object storage", key));
    }

    return result;
}

PlainRewritableRemoteLayout MetadataStorageFromPlainRewritableObjectStorage::listRemoteLayout(
    const PlainRewritableRemoteLayout * base, bool reuse_files, bool & differs_from_base, const LoggerPtr & log) const
{
    ThreadPool & pool = getIOThreadPool().get();

    auto settings = getReadSettings();
    settings.enable_filesystem_cache = false;
    settings.useForSmallRemoteRead(1024);  /// These files are small.

    /// This method can do both initial loading and incremental refresh of the metadata.
    ///
    /// We will list directories under __meta and compare it with the base (the current list in memory or the snapshot).
    /// Some directories may be new and some no longer exist in the storage.
    /// We want to update the state in memory without holding a lock,
    /// and we can do it while allowing certain race-conditions.

    /// So, we obtain a list, then apply changes by:
    /// 1. Deleting every directory in memory that no longer present in the storage;
    ///    This works correctly under the assumption that if a directory with a certain name was deleted it cannot appear again.
    ///    And this assumption is satisfied, because every name is a unique random value.
    /// 2. Checking the value of `prefix.path` for every new directory and adding it to the state in memory.
    ///    There is (?) a race condition, leading to the possibility to add a directory that was just deleted.
    ///    This race condition can be ignored for MergeTree tables.
    /// 3. Checking if the value of `prefix.path` changed for any already existing directory
    ///    and apply the corresponding rename.

    PlainRewritableRemoteLayout remote_layout;
    remote_layout[""] = makeRootDirectoryInfo();

    /// A directory whose `prefix.path` object has the same ETag as in the base has the same logical path, so `prefix.path`
    /// is not read again. The ETag of `prefix.path` does not change when files are added, removed or replaced inside the
    /// directory (e.g. `mutation_*.txt` of a table, or the metadata of a database), so the files of such a directory are
    /// taken from the base only if `reuse_files` is set, that is, when the base is the state in memory: it is as fresh as
    /// the previous refresh. A snapshot file can be older than any of the changes, so with a snapshot the files are listed,
    /// except for a directory in the explicit form (see `PrefixPath.h`): its files are the contents of `prefix.path`.
    std::unordered_map<std::string_view, const PlainRewritableRemoteLayout::value_type *> base_by_remote_path;
    if (base)
    {
        for (const auto & entry : *base)
        {
            if (!entry.first.empty())
                base_by_remote_path.emplace(entry.second.remote_path, &entry);
        }
    }
    size_t reused_directories = 0;

    /// Whether the disk was large enough for the listings to be split into parallel requests.
    bool list_in_parallel = false;
    bool files_are_prelisted = false;

    /// The files of every directory of the disk, listed up front, and the slot every directory is loaded
    /// into. Both are declared before the runner so that they outlive the tasks that write into them.
    std::unordered_map<std::string, std::unordered_map<std::string, FileRemoteInfo>> prelisted_files;
    std::mutex prelisted_files_mutex;
    std::vector<DirectoryLoadResult> results;

    /// Record the files of every directory whose name starts with `name_prefix`. Listing by prefix makes
    /// one request cover many directories, and lets the listing of the disk be split into parallel parts.
    auto list_files_shard = [this, &prelisted_files, &prelisted_files_mutex](const std::string & name_prefix)
    {
        const std::string metadata_directory_prefix = layout->constructMetadataDirectoryKey() + "/";

        std::unordered_map<std::string, std::unordered_map<std::string, FileRemoteInfo>> shard;
        for (auto iterator = object_storage->iterate(layout->constructFilesDirectoryKey(name_prefix), 0, /*with_tags=*/ false, std::nullopt);
             iterator->isValid(); iterator->next())
        {
            const auto remote_file = iterator->current();
            const auto unpacked_remote_file_path = layout->parseFileObjectKey(remote_file->getPath());
            if (!unpacked_remote_file_path.has_value())
                continue;

            const auto & [directory_remote_path, filename] = unpacked_remote_file_path.value();
            /// A listing by prefix also reaches the metadata directory when a directory name starts with
            /// the same characters; the objects there describe directories, they are not files of one.
            if (directory_remote_path == PlainRewritableLayout::METADATA_DIRECTORY_TOKEN
                || remote_file->getPath().starts_with(metadata_directory_prefix))
                continue;

            shard[directory_remote_path].emplace(filename, FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
                .blob_key = {},
            });
        }

        std::lock_guard guard(prelisted_files_mutex);
        for (auto & [directory_remote_path, files] : shard)
            prelisted_files[directory_remote_path].merge(files);
    };

    ThreadPoolCallbackRunnerLocal<void> runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
    try
    {
        /// Enumerate the directories of the disk, that is, the `__meta/{directory}/prefix.path` objects.
        std::vector<DirectoryObject> directories;

        const std::string metadata_directory_key = layout->constructMetadataDirectoryKey();
        const bool can_list_by_prefix = object_storage->supportsPrefixListing();

        auto collect_directory = [&](const RelativePathWithMetadataPtr & file)
        {
            auto remote_path = layout->parseDirectoryObjectKey(file->getPath());
            if (remote_path.has_value())
                directories.emplace_back(DirectoryObject{file->getPath(), std::move(remote_path.value()), file->metadata});
        };

        /// A plain listing costs a single request for a small disk, which is the common case, so start
        /// with one and only switch to the sharded listing once the disk turns out to be large enough
        /// for the sequential pages to dominate the load time. The few pages read here are then re-read
        /// by the sharded listing; that is cheaper than always paying for one request per shard.
        for (auto iterator = object_storage->iterate(metadata_directory_key, 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            if (can_list_by_prefix && directories.size() >= MIN_DIRECTORIES_TO_LIST_IN_PARALLEL)
            {
                list_in_parallel = true;
                break;
            }
            collect_directory(iterator->current());
        }

        if (list_in_parallel)
        {
            directories.clear();
            std::mutex directories_mutex;

            auto list_shard = [&](const std::string & prefix, std::optional<std::string> start_after, bool stop_at_alphabet)
            {
                std::vector<DirectoryObject> shard;
                for (auto iterator = object_storage->iterate(prefix, 0, /*with_tags=*/ false, start_after); iterator->isValid(); iterator->next())
                {
                    const auto file = iterator->current();
                    auto remote_path = layout->parseDirectoryObjectKey(file->getPath());
                    if (!remote_path.has_value())
                        continue;
                    /// The shard that covers everything sorting before the alphabet must not go on to read
                    /// the whole directory once it reaches the alphabet - that is what it is splitting up.
                    if (stop_at_alphabet && !remote_path->empty()
                        && static_cast<unsigned char>(remote_path->front()) >= static_cast<unsigned char>(DIRECTORY_NAME_ALPHABET.front()))
                        break;
                    shard.emplace_back(DirectoryObject{file->getPath(), std::move(remote_path.value()), file->metadata});
                }

                std::lock_guard guard(directories_mutex);
                directories.insert(directories.end(), std::make_move_iterator(shard.begin()), std::make_move_iterator(shard.end()));
            };

            ThreadPoolCallbackRunnerLocal<void> listing_runner(pool, ThreadName::PLAIN_REWRITABLE_META_LOAD);
            try
            {
                for (char c : DIRECTORY_NAME_ALPHABET)
                    listing_runner.enqueueAndKeepTrack([&, c] { list_shard(fmt::format("{}/{}", metadata_directory_key, c), std::nullopt, false); });

                /// Two more shards for the names that sort outside of the alphabet, so that a directory
                /// written by another implementation is never silently dropped from the listing.
                listing_runner.enqueueAndKeepTrack([&] { list_shard(metadata_directory_key, std::nullopt, true); });
                listing_runner.enqueueAndKeepTrack([&]
                {
                    const char after_alphabet = static_cast<char>(static_cast<unsigned char>(DIRECTORY_NAME_ALPHABET.back()) + 1);
                    list_shard(metadata_directory_key, fmt::format("{}/{}", metadata_directory_key, after_alphabet), false);
                });
            }
            catch (...)
            {
                listing_runner.waitForAllToFinish();
                throw;
            }
            listing_runner.waitForAllToFinishAndRethrowFirstError();
        }

        /// Directories whose `prefix.path` did not change since the base are taken from it without reading.
        /// Only a strong ETag proves that: a weak or missing one may stay the same after a rewrite (a rename).
        {
            std::vector<DirectoryObject> directories_to_load;
            for (auto & directory : directories)
            {
                if (const auto it = base_by_remote_path.find(directory.remote_path);
                    it != base_by_remote_path.end()
                    && directory.metadata->isEtagUsableAsCacheKey()
                    && it->second->second.etag == directory.metadata->etag)
                {
                    if (reuse_files)
                    {
                        remote_layout[it->second->first] = it->second->second;
                        ++reused_directories;
                        continue;
                    }
                    directory.base_entry = it->second;
                }
                directories_to_load.push_back(std::move(directory));
            }
            directories = std::move(directories_to_load);
        }

        /// Listing the files of every directory separately costs one request per directory and dominates
        /// the load time of a large disk. Now that the directory names are known, the same objects can be
        /// enumerated by a few listings of the whole disk running in parallel.
        /// A reconcile with a base usually re-reads only a few directories whose `prefix.path` changed,
        /// so listing the whole disk is done only when there are many directories left to load.
        files_are_prelisted = list_in_parallel && directories.size() >= MIN_DIRECTORIES_TO_LIST_IN_PARALLEL;
        std::vector<std::string> listing_shards;
        if (files_are_prelisted)
        {
            std::vector<std::string> directory_names;
            directory_names.reserve(directories.size());
            for (const auto & directory : directories)
                directory_names.push_back(directory.remote_path);

            listing_shards = makeListingShards(directory_names, directories.size() / DIRECTORIES_PER_LISTING_SHARD);
        }

        /// Reading the `prefix.path` objects is independent of listing the files, and neither of the two
        /// saturates the object storage on its own, so they are meant to run at the same time. The pool
        /// takes tasks in the order they were scheduled, so scheduling all the listings first would make
        /// them an earlier stage instead; the two kinds of task are interleaved to avoid that.
        results.resize(directories.size());
        size_t scheduled_shards = 0;
        const size_t shard_every = listing_shards.empty() ? 0 : std::max<size_t>(1, directories.size() / listing_shards.size());
        for (size_t i = 0; i < directories.size(); ++i)
        {
            if (scheduled_shards < listing_shards.size() && shard_every && i % shard_every == 0)
                runner.enqueueAndKeepTrack([&, name_prefix = listing_shards[scheduled_shards++]] { list_files_shard(name_prefix); });

            auto & directory = directories[i];

            /// Passing by reference:
            /// log: Created before runner, so it will be destroyed after
            /// settings: Same as log
            /// result: Same, and no two tasks are given the same slot
            /// In any case we have a try {} catch (...) around runner usage, so exceptions will call runner.waitForAllToFinish() first
            /// Thus the order of destruction of the variables is not important
            runner.enqueueAndKeepTrack([remote_path = std::move(directory.remote_path), object_path = std::move(directory.object_path), metadata = std::move(directory.metadata), base_entry = directory.base_entry, files_are_prelisted, &result = results[i], &log, &settings, this]
            {
                DB::setThreadName(ThreadName::PLAIN_REWRITABLE_META_LOAD);

                StoredObject object{object_path};
                String local_path;
                bool has_explicit_file_list = false;
                /// Assuming that local and the object storage clocks are synchronized.
                Poco::Timestamp last_modified = metadata->last_modified;
                std::unordered_map<std::string, FileRemoteInfo> files;

                try
                {
                    if (base_entry)
                    {
                        local_path = base_entry->first;
                        /// In the explicit form the list of files is the contents of `prefix.path`, so every change of the files
                        /// rewrites it, and the same ETag means the same files. In the implicit form the files are listed below.
                        has_explicit_file_list = base_entry->second.has_explicit_file_list;
                        if (has_explicit_file_list)
                            files = base_entry->second.files;
                    }
                    else if (metadata->size_bytes == 0)
                        LOG_TRACE(log, "The object with the key '{}' has size 0, skipping the read", object_path);
                    else
                    {
                        auto read_buf = object_storage->readObject(object, settings);
                        String contents;
                        readStringUntilEOF(contents, *read_buf);

                        auto prefix_path = parsePrefixPath(contents);
                        local_path = std::move(prefix_path.logical_path);
                        has_explicit_file_list = prefix_path.has_explicit_file_list;

                        /// In the explicit form, the list of files comes from the metadata object and the blobs are not listed.
                        for (auto & listed_file : prefix_path.files)
                        {
                            if (listed_file.blob_key == getDefaultBlobKey(remote_path, listed_file.name))
                                listed_file.blob_key.clear();

                            files.emplace(std::move(listed_file.name), FileRemoteInfo{
                                .bytes_size = listed_file.bytes_size,
                                .last_modified = last_modified.epochTime(),
                                .blob_key = std::move(listed_file.blob_key),
                            });
                        }
                    }

                    /// The root directory has a metadata object only in the explicit form, and it is stored under the reserved remote path.
                    if (remote_path == PlainRewritableLayout::ROOT_DIRECTORY_TOKEN)
                        local_path.clear();
                    else if (normalizePath(local_path).empty())
                    {
                        /// Only the reserved metadata object maps to the logical root, so an empty logical path here means that
                        /// the object does not describe a directory: it is either not written yet (`LocalObjectStorage` writes
                        /// to the final key directly, so an interrupted write can leave the object empty and visible),
                        /// or it is a leftover of a directory that has been removed. Loading it as the root would hide the real
                        /// root and send lookups under it to the prefix of this directory.
                        LOG_WARNING(log, "The object with the key '{}' does not contain the logical path of a directory, ignoring it", object_path);
                        return;
                    }

                    /// In the implicit form, the files of the directory are the blobs stored under its prefix.
                    /// When they were listed up front, the listing may still be running, so they are taken from
                    /// `prelisted_files` afterwards.
                    if (!has_explicit_file_list && !files_are_prelisted)
                    {
                        for (auto dir_iterator = object_storage->iterate(layout->constructFilesDirectoryKey(remote_path), 0, /*with_tags=*/ false, std::nullopt); dir_iterator->isValid(); dir_iterator->next())
                        {
                            const auto remote_file = dir_iterator->current();
                            const auto unpacked_remote_file_path = layout->parseFileObjectKey(remote_file->getPath());
                            if (!unpacked_remote_file_path.has_value())
                            {
                                LOG_WARNING(log, "Legacy layout is in use, ignoring '{}'", remote_file->getPath());
                                continue;
                            }

                            const auto & [directory_remote_path, filename] = unpacked_remote_file_path.value();
                            chassert(directory_remote_path == remote_path);

                            files.emplace(filename, FileRemoteInfo{
                                .bytes_size = remote_file->metadata->size_bytes,
                                .last_modified = remote_file->metadata->last_modified.epochTime(),
                                .blob_key = {},
                            });
                        }
                    }

#if USE_AZURE_BLOB_STORAGE
                    fiu_do_on(FailPoints::plain_rewritable_object_storage_azure_not_found_on_init, {
                        std::bernoulli_distribution fault(0.25);
                        if (fault(thread_local_rng))
                        {
                            LOG_TEST(log, "Fault injection");
                            throw Azure::Storage::StorageException::CreateFromResponse(std::make_unique<Azure::Core::Http::RawResponse>(
                                1, 0, Azure::Core::Http::HttpStatusCode::NotFound, "Fault injected"));
                        }
                    });
#endif
                }
#if USE_AWS_S3
                catch (const S3Exception & e)
                {
                    /// It is ok if a directory was removed just now.
                    if (e.getS3ErrorCode() == Aws::S3::S3Errors::NO_SUCH_KEY)
                        return;
                    throw;
                }
#endif
#if USE_AZURE_BLOB_STORAGE
                catch (const Azure::Storage::StorageException & e)
                {
                    if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound)
                        return;
                    throw;
                }
#endif
                catch (...)
                {
                    throw;
                }

                result = DirectoryLoadResult{
                    true,
                    std::move(local_path),
                    DirectoryRemoteInfo{remote_path, metadata->etag, last_modified.epochTime(), std::move(files), has_explicit_file_list},
                    base_entry ? &base_entry->second : nullptr};
            });
        }

        for (; scheduled_shards < listing_shards.size(); ++scheduled_shards)
            runner.enqueueAndKeepTrack([&, name_prefix = listing_shards[scheduled_shards]] { list_files_shard(name_prefix); });
    }
    catch (...)
    {
        runner.waitForAllToFinish();
        throw;
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    /// Everything has been read by now, so the directories and their files can be put together.
    for (auto & result : results)
    {
        if (!result.loaded)
            continue;

        /// A directory with the explicit file list takes its files from the metadata object, not from the listing.
        if (files_are_prelisted && !result.info.has_explicit_file_list)
        {
            if (auto it = prelisted_files.find(result.info.remote_path); it != prelisted_files.end())
                result.info.files = std::move(it->second);
        }

        if (result.base_info && result.base_info->files == result.info.files)
            ++reused_directories;

        remote_layout[std::move(result.local_path)] = std::move(result.info);
    }

    /// Root folder is a special case. Files are stored as /__root/{file-name}, unless the root has switched to the explicit file list.
    if (!remote_layout[""].has_explicit_file_list)
    {
        for (auto iterator = object_storage->iterate(layout->constructRootFilesDirectoryKey(), 0, /*with_tags=*/ false, std::nullopt); iterator->isValid(); iterator->next())
        {
            auto remote_file = iterator->current();
            remote_layout[""].files.emplace(remote_file->getFileName(), FileRemoteInfo{
                .bytes_size = remote_file->metadata->size_bytes,
                .last_modified = remote_file->metadata->last_modified.epochTime(),
                .blob_key = {},
            });
        }
    }

        LOG_DEBUG(log, "Listed metadata for {} directories (listed {}, files listed {}, {} directories unchanged since the base)",
        remote_layout.size(),
        list_in_parallel ? "by prefix shards" : "sequentially",
        files_are_prelisted ? "for the whole disk at once" : "per directory",
        reused_directories);

    /// Every directory of the base is still there unchanged, nothing was added, and the root is the same.
    differs_from_base = !base
        || reused_directories != base_by_remote_path.size()
        || remote_layout.size() != reused_directories + 1
        || !base->contains("")
        || base->at("") != remote_layout.at("");

    return remote_layout;
}

void MetadataStorageFromPlainRewritableObjectStorage::load(LoadMode mode)
{
    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");
    LOG_DEBUG(log, "Loading metadata");

    /// At startup, the state is obtained from the snapshot file (see `PlainRewritableSnapshotFile.h`) reconciled with
    /// the object storage, or, if there is no snapshot, by listing the object storage (see `listRemoteLayout`).
    ///
    /// The snapshot can lag behind the actual state: it is written after the changes, the server writing it could
    /// have crashed in between, the write is delayed by `write_delay_ms`, or it has failed (a failed write does not fail
    /// the commit). So the snapshot is never trusted alone: it is reconciled with the listing of the `__meta` directory
    /// (a request per thousand directories), which detects the directories that were created, removed or renamed after
    /// the snapshot was written; only those have their `prefix.path` read. The files of every directory are listed
    /// (by a few listings of the whole disk if it is large), because the files inside a directory can change without
    /// changing its `prefix.path`. The snapshot saves reading `prefix.path` of every directory, a request per directory.
    ///
    /// Later loads do not use the snapshot. A periodic refresh (of a read-only disk) reconciles the state in memory with
    /// the listing of `__meta` in the same way: it is at least as fresh as any snapshot it could read, so the files of
    /// the unchanged directories are kept. Other reloads of the disk (`SYSTEM RESTART DISK`,
    /// `SYSTEM CLEAR DISK METADATA CACHE`) list the object storage fully.

    const bool writer = isSnapshotWriter();
    const bool use_snapshot = snapshot_settings.enabled && mode == LoadMode::Initial;

    bool snapshot_file_exists = false;
    if (use_snapshot)
    {
        auto snapshot = tryReadSnapshotFile(log);
        snapshot_file_exists = snapshot.exists;

        if (snapshot.layout)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableSnapshotRead);

            bool differs = false;
            auto remote_layout = listRemoteLayout(&snapshot.layout.value(), /*reuse_files=*/ false, differs, log);
            LOG_DEBUG(log, "Loaded metadata for {} directories from the snapshot file{}",
                remote_layout.size(),
                differs ? ", the object storage had changes after the snapshot" : "");
            fs.applyLayout(std::move(remote_layout));
            previous_refresh.restart();

            if (writer && differs)
                onLayoutChanged();

            return;
        }
    }

    if (mode == LoadMode::Initial)
    {
        bool has_metadata = object_storage->existsOrHasAnyChild(layout->constructMetadataDirectoryKey());

        /// Use iteration to determine if the disk contains data.
        /// LocalObjectStorage creates an empty top-level directory even when no data is stored,
        /// unlike blob storage, which has no concept of directories, therefore existsOrHasAnyChild
        /// is not applicable.
        auto common_key_prefix = fs::path(object_storage->getCommonKeyPrefix()) / "";
        bool has_data = object_storage->isRemote() ? object_storage->existsOrHasAnyChild(common_key_prefix) : object_storage->iterate(common_key_prefix, 0, /*with_tags=*/ false, std::nullopt)->isValid();
        /// No metadata directory: legacy layout is likely in use.
        if (has_data && !has_metadata)
        {
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableLegacyLayoutDiskCount, 1);
            LOG_WARNING(log, "Legacy layout is likely used for disk '{}'", object_storage->getCommonKeyPrefix());
        }

        if (!has_data && !has_metadata)
        {
            LOG_DEBUG(log, "Loaded metadata (empty)");
            PlainRewritableRemoteLayout remote_layout;
            remote_layout[""] = makeRootDirectoryInfo();
            fs.applyLayout(std::move(remote_layout));
            return;
        }
    }

    std::optional<PlainRewritableRemoteLayout> base;
    if (mode == LoadMode::Incremental)
        base = getCurrentLayout();

    bool differs = false;
    auto remote_layout = listRemoteLayout(base ? &base.value() : nullptr, /*reuse_files=*/ true, differs, log);
    LOG_DEBUG(log, "Loaded metadata for {} directories", remote_layout.size());
    const bool empty = isEmptyLayout(remote_layout);
    fs.applyLayout(std::move(remote_layout));
    previous_refresh.restart();

    /// Publish the state for the next start and for the readers.
    /// A disk that never had anything gets no snapshot until the first change, to not write to a storage that is only looked at.
    if (writer && (mode != LoadMode::Initial || !empty || snapshot_file_exists))
        onLayoutChanged();
}

void MetadataStorageFromPlainRewritableObjectStorage::onLayoutChanged()
{
    if (!isSnapshotWriter())
        return;

    /// The change was already applied to `fs`, so its version is not greater than the current one.
    const UInt64 generation = fs.getVersion();
    snapshot_dirty = true;

    /// When everything was removed from the disk, the snapshot is removed right away regardless of the delay:
    /// this is a single cheap request, and nothing should be left behind in the object storage after the last `DROP`.
    if (snapshot_settings.write_delay_ms == 0 || fs.takeReadOnlySnapshot()->listDirectory("").empty())
        writeSnapshotIfDirty(generation);
    else
        snapshot_write_task->scheduleAfter(snapshot_settings.write_delay_ms, /*overwrite=*/ false);
}

void MetadataStorageFromPlainRewritableObjectStorage::writeSnapshotIfDirty(UInt64 required_generation)
{
    std::lock_guard lock(snapshot_write_mutex);

    /// Concurrent committers coalesce: if a write that finished while we were waiting for the lock already included
    /// our change, there is nothing to do. So at most one more write follows the one in progress.
    if (required_generation && snapshot_written_generation >= required_generation)
        return;

    /// The flag is cleared before taking the state: a change applied after this point sets it again and is written next time.
    if (!snapshot_dirty.exchange(false))
        return;

    LoggerPtr log = getLogger("MetadataStorageFromPlainObjectStorage");
    const auto key = layout->constructSnapshotObjectKey();

    std::unique_ptr<WriteBufferFromFileBase> out;
    try
    {
        /// The tree and its version are taken together, so the recorded generation is exactly the state that is written.
        auto [tree, generation] = fs.takeReadOnlySnapshotWithVersion();
        auto remote_layout = getLayoutOf(*tree);
        if (isEmptyLayout(remote_layout))
        {
            /// Everything was removed from the disk: do not leave the snapshot behind.
            object_storage->removeObjectIfExists(StoredObject(key));
            LOG_DEBUG(log, "Removed the snapshot file '{}' as the disk is empty", key);
        }
        else
        {
            out = object_storage->writeObject(StoredObject(key), WriteMode::Rewrite, /*object_attributes*/ std::nullopt, DBMS_DEFAULT_BUFFER_SIZE, getWriteSettings());
            writePlainRewritableSnapshot(remote_layout, *out);
            out->finalize();
            ProfileEvents::increment(ProfileEvents::DiskPlainRewritableSnapshotWritten);
            LOG_DEBUG(log, "Written the snapshot file '{}' ({} bytes) with {} directories", key, out->count(), remote_layout.size());
        }
        snapshot_written_generation = generation;
    }
    catch (...)
    {
        if (out)
            out->cancel();

        /// The state in memory is correct, only its copy in the object storage is stale, so a transaction commit
        /// must not fail because of this. The write is retried later.
        snapshot_dirty = true;
        ProfileEvents::increment(ProfileEvents::DiskPlainRewritableSnapshotWriteFailed);
        tryLogCurrentException(log, fmt::format("Cannot write the snapshot file '{}', will retry", key));
        snapshot_write_task->scheduleAfter(std::max(snapshot_settings.write_delay_ms, SNAPSHOT_WRITE_RETRY_DELAY_MS), /*overwrite=*/ false);
    }
}

void MetadataStorageFromPlainRewritableObjectStorage::snapshotWriteTask()
{
    writeSnapshotIfDirty();

    /// The changes that happened during the write are written after the next delay.
    if (snapshot_dirty && snapshot_settings.write_delay_ms > 0)
        snapshot_write_task->scheduleAfter(snapshot_settings.write_delay_ms, /*overwrite=*/ false);
}

MetadataStorageFromPlainRewritableObjectStorage::MetadataStorageFromPlainRewritableObjectStorage(
    ObjectStoragePtr object_storage_, String storage_path_prefix_, bool hard_links_enabled_, PlainRewritableSnapshotSettings snapshot_settings_)
    : object_storage(std::move(object_storage_))
    , metrics(createPlainRewritableMetrics(object_storage->getType()))
    , storage_path_prefix(std::move(storage_path_prefix_))
    , storage_path_full(fs::path(object_storage->getRootPrefix()) / storage_path_prefix)
    , hard_links_enabled(hard_links_enabled_)
    , snapshot_settings(snapshot_settings_)
    , fs(metrics->directory_map_size, metrics->file_count)
    , layout(std::make_shared<PlainRewritableLayout>(object_storage->getCommonKeyPrefix()))
{
    if (isSnapshotWriter())
    {
        auto context = Context::getGlobalContextInstance();
        if (!context)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The global context is required to write the snapshots of the plain_rewritable metadata");

        snapshot_write_task = context->getSchedulePool()->createTask(StorageID::createEmpty(), "PlainRewritableSnapshotWriter", [this] { snapshotWriteTask(); });
    }

    std::lock_guard lock(load_mutex);
    load(LoadMode::Initial);
}

MetadataTransactionPtr MetadataStorageFromPlainRewritableObjectStorage::createTransaction()
{
    return std::make_shared<MetadataStorageFromPlainRewritableObjectStorageTransaction>(*this);
}

void MetadataStorageFromPlainRewritableObjectStorage::dropCache()
{
    std::unique_lock reload_lock(load_mutex);
    std::unique_lock tx_lock(metadata_mutex);
    load(LoadMode::Full);
}

void MetadataStorageFromPlainRewritableObjectStorage::refresh(UInt64 not_sooner_than_milliseconds)
{
    if (!previous_refresh.compareAndRestart(0.001 * static_cast<double>(not_sooner_than_milliseconds)))
        return;

    std::unique_lock load_lock(load_mutex, std::defer_lock);
    if (load_lock.try_lock())
    {
        std::unique_lock metadata_lock(metadata_mutex);
        load(LoadMode::Incremental);
    }
}

void MetadataStorageFromPlainRewritableObjectStorage::shutdown()
{
    if (!snapshot_write_task)
        return;

    /// Stop the background writes and write the latest state synchronously: the next start loads it.
    snapshot_write_task->deactivate();
    writeSnapshotIfDirty();
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFile(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->existsFile(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsDirectory(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->existsDirectory(path);
}

bool MetadataStorageFromPlainRewritableObjectStorage::existsFileOrDirectory(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();
    return tree->existsFile(path) || tree->existsDirectory(path);
}

uint64_t MetadataStorageFromPlainRewritableObjectStorage::getFileSize(const std::string & path) const
{
    if (auto file_size = getFileSizeIfExists(path))
        return file_size.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<uint64_t> MetadataStorageFromPlainRewritableObjectStorage::getFileSizeIfExists(const std::string & path) const
{
    if (auto remote_info = fs.takeReadOnlySnapshot()->getFileRemoteInfo(path))
        return remote_info->bytes_size;

    return std::nullopt;
}

std::vector<std::string> MetadataStorageFromPlainRewritableObjectStorage::listDirectory(const std::string & path) const
{
    return fs.takeReadOnlySnapshot()->listDirectory(path);
}

DirectoryIteratorPtr MetadataStorageFromPlainRewritableObjectStorage::iterateDirectory(const std::string & path) const
{
    auto paths = listDirectory(path);

    /// Prepend path, since iterateDirectory() includes path, unlike listDirectory()
    std::for_each(paths.begin(), paths.end(), [&](auto & child) { child = fs::path(path) / child; });
    std::vector<fs::path> fs_paths(paths.begin(), paths.end());
    return std::make_unique<StaticDirectoryIterator>(std::move(fs_paths));
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorage::getStorageObjects(const std::string & path) const
{
    if (auto objects = getStorageObjectsIfExist(path))
        return std::move(objects.value());

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);
}

std::optional<StoredObjects> MetadataStorageFromPlainRewritableObjectStorage::getStorageObjectsIfExist(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        return std::nullopt;

    const auto file_it = directory_remote_info->files.find(normalized_path.filename());
    if (file_it == directory_remote_info->files.end())
        return std::nullopt;

    auto object_key = layout->constructBlobObjectKey(getBlobKey(*directory_remote_info, normalized_path.filename(), file_it->second));
    return StoredObjects{StoredObject(object_key, path, file_it->second.bytes_size)};
}

uint32_t MetadataStorageFromPlainRewritableObjectStorage::getHardlinkCount(const std::string & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    const auto normalized_path = normalizePath(path);
    const auto directory_remote_info = tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_remote_info)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);

    const auto file_it = directory_remote_info->files.find(normalized_path.filename());
    if (file_it == directory_remote_info->files.end())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} does not exist", path);

    /// As for the other metadata storages, this is the number of links besides the file itself.
    return tree->getBlobLinkCount(getBlobKey(*directory_remote_info, normalized_path.filename(), file_it->second)) - 1;
}

Poco::Timestamp MetadataStorageFromPlainRewritableObjectStorage::getLastModified(const std::string & path) const
{
    if (auto last_modified = getLastModifiedIfExists(path))
        return last_modified.value();

    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory {} does not exist", path);
}

std::optional<Poco::Timestamp> MetadataStorageFromPlainRewritableObjectStorage::getLastModifiedIfExists(const String & path) const
{
    const auto tree = fs.takeReadOnlySnapshot();

    if (tree->existsDirectory(path))
    {
        const auto remote_info = tree->getDirectoryRemoteInfo(path);

        if (remote_info)
            return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

        /// Let's return something in this case to unblock fs garbage cleanup.
        return Poco::Timestamp::fromEpochTime(0);
    }

    if (auto remote_info = tree->getFileRemoteInfo(path))
        return Poco::Timestamp::fromEpochTime(remote_info->last_modified);

    return std::nullopt;
}

MetadataStorageFromPlainRewritableObjectStorageTransaction::MetadataStorageFromPlainRewritableObjectStorageTransaction(MetadataStorageFromPlainRewritableObjectStorage & metadata_storage_)
    : metadata_storage(metadata_storage_)
    , commit_snapshot(metadata_storage.fs.takeReadWriteSnapshot())
    , uncommitted_state(metadata_storage.fs.takeReadWriteSnapshot())
{
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();

    /// 0. Add preconditions for transaction commit.
    operations.prependOperation(std::make_unique<MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation>(uncommitted_state.getTxPreconditions(), commit_snapshot));

    {
        std::unique_lock lock(metadata_storage.metadata_mutex);

        /// 1. Setup up-to-date fs into snapshot being used during commit.
        commit_snapshot->setRoot(metadata_storage.fs.takeReadWriteSnapshot()->getRoot());

        /// 2. Execute all operations on top of write set.
        operations.commit();

        /// 3. Exchange metadata with updated fs.
        metadata_storage.fs.applySnapshot(commit_snapshot);
    }

    operations.finalize();

    /// 4. Publish the new state in the snapshot file.
    metadata_storage.onLayoutChanged();
}

TransactionCommitOutcomeVariant MetadataStorageFromPlainRewritableObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & /*options*/)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Plain-Rewritable Metadata storage supports only commit");
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    /// The blob has been written to the key chosen by `generateObjectKeyForPath`; if the key was not generated
    /// by this transaction, the blob is expected at the default location.
    std::string blob_key;
    if (const auto it = generated_blob_keys.find(normalizePath(path).string()); it != generated_blob_keys.end())
        blob_key = it->second;

    /// The following operations of this transaction have to see the file: a hard link to it makes its blob shared,
    /// and then rewriting it has to pick a new blob instead of clobbering the shared one.
    uncommitted_state.recordCreatedFile(path, blob_key);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageWriteFileOperation>(
        path,
        objects.front(),
        std::move(blob_key),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectory(const std::string & path)
{
    if (normalizePath(path).empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    uncommitted_state.createDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/false,
        normalizeDirectoryPath(path),
        uncommitted_state.getDirectoryRemoteInfo(path)->remote_path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createDirectoryRecursive(const std::string & path)
{
    if (normalizePath(path).empty())
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainRewritableObjectStorageTransaction"), "Skipping creation of a directory '{}' with an empty normalized path", path);
        return;
    }

    uncommitted_state.createDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageCreateDirectoryOperation>(
        /*recursive=*/true,
        normalizeDirectoryPath(path),
        uncommitted_state.getDirectoryRemoteInfo(path)->remote_path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::markFallbackCopyMoved(const std::string & path_from, const std::string & path_to)
{
    /// The copy puts its blob at the default key of the original target, and the move carries it from there at commit.
    /// A rewrite of either path cannot supersede the copy anymore: the move needs it as the source, and the blob that
    /// the move or the copy produces at commit would overwrite the bytes that the caller writes before the commit.
    if (const auto it = fallback_copies.find(path_from); it != fallback_copies.end())
    {
        it->second.moved = true;
        fallback_copies[path_to] = FallbackCopy{.copy = it->second.copy, .moved = true};
    }
    else if (const auto it_to = fallback_copies.find(path_to); it_to != fallback_copies.end())
    {
        it_to->second.moved = true;
    }
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    uncommitted_state.moveDirectory(path_from, path_to);

    const auto directory_from = normalizePath(path_from).string();
    const auto directory_to = normalizePath(path_to).string();
    std::vector<std::pair<std::string, std::string>> moved_copies;
    for (const auto & [path, _] : fallback_copies)
        if (directory_from.empty() || path.starts_with(directory_from + "/"))
            moved_copies.emplace_back(path, (std::filesystem::path(directory_to) / path.substr(directory_from.empty() ? 0 : directory_from.size() + 1)).string());
    for (const auto & [moved_from, moved_to] : moved_copies)
        markFallbackCopyMoved(moved_from, moved_to);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(
        normalizeDirectoryPath(path_from),
        normalizeDirectoryPath(path_to),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::unlinkFile(const std::string & path, bool if_exists, bool /*should_remove_objects*/)
{
    const auto normalized_path = normalizePath(path);
    uncommitted_state.useDirectory(normalized_path.parent_path());

    /// Removing a file whose blob is shared switches the directory to the explicit file list.
    if (const auto blob_key = getBlobKeyIfExists(uncommitted_state.getSnapshot(), normalized_path);
        blob_key && uncommitted_state.getSnapshot().getBlobLinkCount(*blob_key) > 1)
        uncommitted_state.markDirectoryExplicit(normalized_path.parent_path());

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation>(
        path,
        if_exists,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeDirectory(const std::string & path)
{
    if (!normalizePath(path).empty())
        uncommitted_state.removeDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation>(
        normalizeDirectoryPath(path),
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & /*should_remove_objects*/)
{
    if (!normalizePath(path).empty())
        uncommitted_state.removeDirectory(path);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation>(
        path,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);
    uncommitted_state.useDirectory(normalized_path_from.parent_path());
    uncommitted_state.useDirectory(normalized_path_to.parent_path());

    /// A real hard link would make the metadata of the target directory unreadable by older servers, so it is opt-in.
    if (!metadata_storage.hard_links_enabled)
    {
        /// The copy is a new file with its own blob. In an implicit target directory the blob goes to the default location.
        /// A directory in the explicit form (written while hard links were enabled) may still have a blob at the default
        /// location that is linked from elsewhere after its file there was removed, so the copy gets a fresh random key,
        /// like any new file of such a directory (see `generateObjectKeyForPath`).
        std::string blob_key;
        if (const auto directory_to = uncommitted_state.getDirectoryRemoteInfo(normalized_path_to.parent_path());
            directory_to && directory_to->has_explicit_file_list)
            blob_key = getDefaultBlobKey(directory_to->remote_path, getRandomASCIIString(32));

        /// The following operations of this transaction have to see the copy, like any other created file: otherwise
        /// a rewrite of the target would plan as the creation of a missing file, and the copy would overwrite it at commit.
        uncommitted_state.recordCreatedFile(path_to, blob_key);

        auto copy = std::make_unique<MetadataStorageFromPlainObjectStorageCopyFileOperation>(
            path_from,
            path_to,
            std::move(blob_key),
            commit_snapshot,
            metadata_storage.object_storage,
            metadata_storage.layout,
            metadata_storage.metrics);
        fallback_copies[normalized_path_to.string()] = FallbackCopy{.copy = copy.get()};
        operations.addOperation(std::move(copy));
        return;
    }

    /// The target directory switches to the explicit file list and the blob becomes shared.
    uncommitted_state.recordHardLink(path_from, path_to);

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageHardLinkOperation>(
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::planFileMove(const NormalizedPath & path_from, const NormalizedPath & path_to)
{
    markFallbackCopyMoved(path_from.string(), path_to.string());

    uncommitted_state.useDirectory(path_from.parent_path());
    uncommitted_state.useDirectory(path_to.parent_path());

    const auto & snapshot = uncommitted_state.getSnapshot();
    const auto directory_from = snapshot.getDirectoryRemoteInfo(path_from.parent_path());
    const auto directory_to = snapshot.getDirectoryRemoteInfo(path_to.parent_path());
    const auto blob_key_from = getBlobKeyIfExists(snapshot, path_from);
    if (!directory_from || !directory_to || !blob_key_from)
        return;

    const bool metadata_only = isMetadataOnlyMove(snapshot, *directory_from, *directory_to, *blob_key_from, getBlobKeyIfExists(snapshot, path_to));
    if (metadata_only)
    {
        uncommitted_state.markDirectoryExplicit(path_from.parent_path());
        uncommitted_state.markDirectoryExplicit(path_to.parent_path());
    }

    /// The moved file has to be visible at its new path: a hard link to it makes its blob shared, and then rewriting it
    /// has to pick a new blob instead of clobbering the shared one.
    uncommitted_state.recordMovedFile(path_from, path_to, /*keeps_blob=*/metadata_only);
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    planFileMove(normalizePath(path_from), normalizePath(path_to));

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/false,
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

void MetadataStorageFromPlainRewritableObjectStorageTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    planFileMove(normalizePath(path_from), normalizePath(path_to));

    operations.addOperation(std::make_unique<MetadataStorageFromPlainObjectStorageMoveFileOperation>(
        /*replaceable=*/true,
        path_from,
        path_to,
        commit_snapshot,
        metadata_storage.object_storage,
        metadata_storage.layout,
        metadata_storage.metrics,
        removed_objects));
}

ObjectStorageKey MetadataStorageFromPlainRewritableObjectStorageTransaction::generateObjectKeyForPath(const std::string & path)
{
    const auto normalized_path = normalizePath(path);
    if (normalized_path.filename().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "File name is empty for path '{}'", path);

    /// The new blob replaces the copy that stood in for a hard link in this transaction, so the copy is redundant.
    /// It would also go to the same default key at commit (the target directory keeps the implicit form, where a file
    /// has one key) and overwrite the bytes that the caller writes to the key returned from here before the commit.
    if (const auto it = fallback_copies.find(normalized_path.string()); it != fallback_copies.end())
    {
        if (it->second.moved)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Cannot write the file '{}': the transaction has moved the copy of a file that stands in for a hard link "
                "(the disk has hard links disabled) to or from this path",
                path);

        it->second.copy->supersede();
        fallback_copies.erase(it);
    }

    const auto parent_path = normalized_path.parent_path();
    const auto parent_info = uncommitted_state.getDirectoryRemoteInfo(parent_path);

    if (!parent_info)
    {
        /// Validate during commit that directory will be created on S3.
        uncommitted_state.useMissingDirectory(parent_path);

        /// Materialize virtual parent.
        createDirectoryRecursive(parent_path);
    }
    else
    {
        /// Validate during commit that directory will not be recreated on S3.
        uncommitted_state.useDirectory(parent_path);
    }

    if (const auto directory_remote_info = uncommitted_state.getDirectoryRemoteInfo(parent_path))
    {
        const auto file_name = normalized_path.filename().string();

        /// In a directory with the explicit file list the blob names are random: a new file must not clobber the blob
        /// of a removed file that is still linked from elsewhere. The same applies to rewriting a file whose blob is shared.
        bool use_random_name = directory_remote_info->has_explicit_file_list;
        if (!use_random_name)
            if (const auto it = directory_remote_info->files.find(file_name); it != directory_remote_info->files.end())
                use_random_name = uncommitted_state.getSnapshot().getBlobLinkCount(getBlobKey(*directory_remote_info, file_name, it->second)) > 1;

        auto blob_key = getDefaultBlobKey(directory_remote_info->remote_path, use_random_name ? getRandomASCIIString(32) : file_name);
        auto object_key = ObjectStorageKey::createAsAbsolute(metadata_storage.layout->constructBlobObjectKey(blob_key));
        generated_blob_keys[normalized_path.string()] = std::move(blob_key);
        return object_key;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", parent_path.string());
}

StoredObjects MetadataStorageFromPlainRewritableObjectStorageTransaction::getSubmittedForRemovalBlobs()
{
    return removed_objects;
}

}
