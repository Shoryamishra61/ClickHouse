#pragma once

#include <Disks/CustomDiskRegistration.h>
#include <Common/Logger.h>
#include <Common/ThreadPool_fwd.h>
#include <base/defines.h>

#include <condition_variable>
#include <deque>
#include <map>
#include <mutex>
#include <set>

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// The registrations of the custom disks of the server, and the background thread that shuts down
/// the disks that are no longer used. `shutdown` of a disk may take a while (it waits for the
/// background operations of the disk), and the last registration may be destroyed by any thread
/// (a query dropping a table, a thread finishing a query that held the last reference to a dropped
/// table, ...), so it is not done in place.
class CustomDiskRegistry
{
public:
    CustomDiskRegistry();
    ~CustomDiskRegistry();

    /// The registration of the disk `name`. If there is none yet, creates it, and the new
    /// registration takes over `nested`.
    CustomDiskRegistrationPtr getOrCreate(const String & name, CustomDiskRegistrations nested);
    /// The registration of the disk `name`, or nullptr if there is none.
    CustomDiskRegistrationPtr tryGet(const String & name) const;
    /// Whether the last registration of the disk `name` is being destroyed. It is not if the disk
    /// has been registered again in the meantime, and then the disk has to stay.
    bool canRelease(const String & name) const;
    /// Removes the registration of the disk `name`, after `canRelease`. Until `scheduleShutdown` is
    /// done with the disk, `waitForShutdown` waits for it.
    void release(const String & name);

    /// Shuts down a released disk in the background, after the disks scheduled before it, if nothing
    /// else uses it by then. A disk wraps the disks nested in its definition, which are released after
    /// it, so they are scheduled after it as well. `disk` is nullptr if there was no disk to release.
    void scheduleShutdown(const String & name, DiskPtr disk);
    /// Waits until the disk `name` released earlier has been shut down. A new disk created with the
    /// same definition uses the same metadata and data, so it must not overlap with the old one.
    /// Throws if the old disk was not shut down because something still references it, and it is still alive.
    void waitForShutdown(const String & name);
    /// Whether the disk `name` has been released and is not gone yet: either not shut down yet, or still
    /// referenced after its shutdown was skipped.
    bool isShutdownPending(const String & name);

    /// Waits for the scheduled shutdowns. The disks released after that are shut down in place.
    void shutdown();

private:
    struct DiskToShutdown
    {
        String name;
        DiskPtr disk;
    };

    void run();
    void finishShutdown(DiskToShutdown disk_to_shutdown);
    void removePendingName(const String & name) TSA_REQUIRES(mutex);
    /// Whether a released disk `name`, not shut down because it was still referenced, is still alive.
    /// Forgets the ones that are gone.
    bool hasLingeringDisk(const String & name) TSA_REQUIRES(mutex);

    LoggerPtr log;

    mutable std::mutex mutex;
    std::map<String, std::weak_ptr<CustomDiskRegistration>> registrations TSA_GUARDED_BY(mutex);
    std::deque<DiskToShutdown> disks_to_shutdown TSA_GUARDED_BY(mutex);
    std::condition_variable disks_to_shutdown_cv;
    /// The names of the released disks that are not shut down yet.
    std::multiset<String> names_pending_shutdown TSA_GUARDED_BY(mutex);
    std::condition_variable shutdown_finished_cv;
    /// The released disks that were not shut down because something still referenced them. A new disk
    /// with the same name must not be created while the old one is alive.
    std::multimap<String, std::weak_ptr<IDisk>> lingering_disks TSA_GUARDED_BY(mutex);
    bool shutdown_called TSA_GUARDED_BY(mutex) = false;
    /// Started on the first scheduled shutdown.
    std::unique_ptr<ThreadFromGlobalPoolNoTracingContextPropagation> thread TSA_GUARDED_BY(mutex);
};

}
