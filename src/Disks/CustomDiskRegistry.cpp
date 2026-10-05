#include <Disks/CustomDiskRegistry.h>

#include <Disks/IDisk.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>

namespace DB
{

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

bool CustomDiskRegistry::remove(const String & name)
{
    std::lock_guard lock(mutex);

    auto it = registrations.find(name);
    if (it == registrations.end())
        return false;

    /// A table or database has taken the same disk definition again while the last registration
    /// was being destroyed, and the entry now points to a new registration.
    if (!it->second.expired())
        return false;

    registrations.erase(it);
    return true;
}

void CustomDiskRegistry::scheduleShutdown(DiskPtr disk)
{
    {
        std::lock_guard lock(mutex);
        if (!shutdown_called)
        {
            if (!thread)
                thread = std::make_unique<ThreadFromGlobalPoolNoTracingContextPropagation>([this] { run(); });
            disks_to_shutdown.push_back(std::move(disk));
            disks_to_shutdown_cv.notify_one();
            return;
        }
    }

    /// The server is shutting down, and there is nothing to wait for anymore.
    shutdownDisk(std::move(disk), log);
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
        /// unregistered while the tables are being shut down are not in the disk selector anymore.
        if (disks_to_shutdown.empty())
            return;

        DiskPtr disk = std::move(disks_to_shutdown.front());
        disks_to_shutdown.pop_front();

        lock.unlock();
        /// The reference is dropped before the next disk is taken: that may be the disk this one
        /// wraps, which is shut down only if nothing else references it.
        shutdownDisk(std::move(disk), log);
        lock.lock();
    }
}

void CustomDiskRegistry::shutdownDisk(DiskPtr disk, LoggerPtr log)
{
    /// `shutdown` makes the disk reject further requests, so it may only be called when nothing can
    /// use the disk anymore. The disk is no longer reachable by name, so no new reference to it can
    /// appear, and holding the only one left means there is no user of it either.
    if (disk.use_count() != 1)
    {
        LOG_DEBUG(log, "Custom disk {} is still referenced after being unregistered, it will not be shut down explicitly",
            backQuote(disk->getName()));
        return;
    }

    try
    {
        disk->shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "while shutting down custom disk " + backQuote(disk->getName()));
    }
}

}
