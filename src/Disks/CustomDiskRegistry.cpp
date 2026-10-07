#include <Disks/CustomDiskRegistry.h>

#include <Disks/IDisk.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
}

CustomDiskRegistry::CustomDiskRegistry()
    : log(getLogger("CustomDiskRegistry"))
{
}

CustomDiskRegistry::~CustomDiskRegistry()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

CustomDiskRegistrationPtr CustomDiskRegistry::getOrCreate(const String & name, CustomDiskRegistrations nested)
{
    std::lock_guard lock(mutex);

    auto & weak_registration = registrations[name];
    auto registration = weak_registration.lock();
    if (!registration)
    {
        registration = std::make_shared<CustomDiskRegistration>(name, std::move(nested));
        weak_registration = registration;
    }
    return registration;
}

CustomDiskRegistrationPtr CustomDiskRegistry::tryGet(const String & name) const
{
    std::lock_guard lock(mutex);

    auto it = registrations.find(name);
    if (it == registrations.end())
        return nullptr;
    return it->second.lock();
}

bool CustomDiskRegistry::canRelease(const String & name) const
{
    std::lock_guard lock(mutex);

    auto it = registrations.find(name);
    return it != registrations.end() && it->second.expired();
}

void CustomDiskRegistry::release(const String & name)
{
    std::lock_guard lock(mutex);

    registrations.erase(name);
    names_pending_shutdown.insert(name);
}

void CustomDiskRegistry::scheduleShutdown(const String & name, DiskPtr disk)
{
    DiskToShutdown disk_to_shutdown{name, std::move(disk)};

    if (disk_to_shutdown.disk)
    {
        std::lock_guard lock(mutex);
        if (!shutdown_called)
        {
            try
            {
                if (!thread)
                    thread = std::make_unique<ThreadFromGlobalPoolNoTracingContextPropagation>([this] { run(); });
                disks_to_shutdown.push_back(std::move(disk_to_shutdown));
            }
            catch (...)
            {
                /// Do not leave the name pending forever, `waitForShutdown` would never return.
                removePendingName(name);
                shutdown_finished_cv.notify_all();
                throw;
            }
            disks_to_shutdown_cv.notify_one();
            return;
        }
    }

    /// There is nothing to shut down, or the server is shutting down and there is nothing to wait for anymore.
    finishShutdown(std::move(disk_to_shutdown));
}

void CustomDiskRegistry::waitForShutdown(const String & name)
{
    std::unique_lock lock(mutex);
    shutdown_finished_cv.wait(lock, [&]() TSA_NO_THREAD_SAFETY_ANALYSIS { return !names_pending_shutdown.contains(name); });

    /// Nothing notifies about the last reference to the old disk being dropped, so there is nothing to wait for.
    if (hasLingeringDisk(name))
        throw Exception(ErrorCodes::ABORTED,
            "Cannot create custom disk {}: the previous disk with the same definition, which is no longer used by any "
            "table or database, is still referenced and has not been destroyed yet. Try again later",
            backQuote(name));
}

bool CustomDiskRegistry::isShutdownPending(const String & name)
{
    std::lock_guard lock(mutex);
    return names_pending_shutdown.contains(name) || hasLingeringDisk(name);
}

void CustomDiskRegistry::shutdown()
{
    std::unique_ptr<ThreadFromGlobalPoolNoTracingContextPropagation> thread_to_join;
    {
        std::lock_guard lock(mutex);
        shutdown_called = true;
        thread_to_join = std::move(thread);
        disks_to_shutdown_cv.notify_one();
    }

    if (thread_to_join)
        thread_to_join->join();
}

/// TSA_NO_THREAD_SAFETY_ANALYSIS because TSA does not support `std::unique_lock` used with `std::condition_variable`.
void CustomDiskRegistry::run() TSA_NO_THREAD_SAFETY_ANALYSIS
{
    DB::setThreadName(ThreadName::CUSTOM_DISK_SHUTDOWN);

    std::unique_lock lock(mutex);
    while (true)
    {
        disks_to_shutdown_cv.wait(lock, [this]() TSA_NO_THREAD_SAFETY_ANALYSIS { return shutdown_called || !disks_to_shutdown.empty(); });

        /// The scheduled disks are shut down even when the server is shutting down: the disks
        /// released while the tables are being shut down are not in the disk selector anymore.
        if (disks_to_shutdown.empty())
            return;

        DiskToShutdown disk_to_shutdown = std::move(disks_to_shutdown.front());
        disks_to_shutdown.pop_front();

        lock.unlock();
        finishShutdown(std::move(disk_to_shutdown));
        lock.lock();
    }
}

void CustomDiskRegistry::finishShutdown(DiskToShutdown disk_to_shutdown)
{
    if (auto disk = std::move(disk_to_shutdown.disk))
    {
        /// `shutdown` makes the disk reject further requests, so it may only be called when nothing can
        /// use the disk anymore. The disk is no longer reachable by name, so no new reference to it can
        /// appear, and holding the only one left means there is no user of it either.
        if (disk.use_count() != 1)
        {
            LOG_DEBUG(log, "Custom disk {} is still referenced after being unregistered, it will not be shut down explicitly",
                backQuote(disk_to_shutdown.name));

            std::lock_guard lock(mutex);
            lingering_disks.emplace(disk_to_shutdown.name, disk);
        }
        else
        {
            try
            {
                disk->shutdown();
            }
            catch (...)
            {
                tryLogCurrentException(log, "while shutting down custom disk " + backQuote(disk_to_shutdown.name));
            }
        }

        /// The reference is dropped before the next disk is taken: that may be the disk this one
        /// wraps, which is shut down only if nothing else references it.
    }

    std::lock_guard lock(mutex);
    removePendingName(disk_to_shutdown.name);
    shutdown_finished_cv.notify_all();
}

void CustomDiskRegistry::removePendingName(const String & name)
{
    auto it = names_pending_shutdown.find(name);
    chassert(it != names_pending_shutdown.end());
    if (it != names_pending_shutdown.end())
        names_pending_shutdown.erase(it);
}

bool CustomDiskRegistry::hasLingeringDisk(const String & name)
{
    bool alive = false;
    auto [begin, end] = lingering_disks.equal_range(name);
    for (auto it = begin; it != end;)
    {
        if (it->second.expired())
        {
            it = lingering_disks.erase(it);
        }
        else
        {
            alive = true;
            ++it;
        }
    }
    return alive;
}

}
