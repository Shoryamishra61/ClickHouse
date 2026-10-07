#pragma once

#include <Common/Scheduler/IResourceManager.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Core/UUID.h>
#include <base/defines.h>
#include <base/types.h>

#include <boost/noncopyable.hpp>

#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>


namespace DB
{

class QuerySlot;

/// The scheduling identity of one query on this server, shared by all parts of the query that run
/// here: the query itself, its `PARALLEL WITH` subqueries and the tasks of its distributed plan.
///
/// Members share one workload classifier and therefore one `ResourceSchedulingContext`: the
/// query-aware schedulers see the query as a single flow with one weight, one priority and one
/// attained service, no matter how many parts it is split into. Requests of one flow are served in
/// arrival order by every scheduler, so a member is never starved by its siblings.
///
/// Members also share one query slot (`CREATE RESOURCE ... (QUERY)`). The first member to need a
/// slot acquires it, and the others join it instead of queueing behind other queries. Otherwise a
/// part of an admitted query could wait for admission while its siblings hold slots and wait for its
/// output.
///
/// A query can run several distributed plans (e.g. one per distributed subquery). The tasks of all of
/// them carry the id of the initiator's group, so on every server they join one group per query.
///
/// Parts of the query that change the workload or a scheduling setting are scheduled as a separate
/// query: they join a group derived from the query's group, one per distinct workload and settings.
/// A derived group keeps its parent alive, so the parts that come later find the same groups.
class QuerySchedulingGroup : public std::enable_shared_from_this<QuerySchedulingGroup>, private boost::noncopyable
{
public:
    QuerySchedulingGroup(String workload_, const ClassifierSettings & settings_, ClassifierPtr classifier_, std::shared_ptr<QuerySchedulingGroup> parent_ = nullptr);

    /// Unique id of the group.
    const UUID & getId() const { return id; }

    /// The group of the query itself: this group, or the group this one is derived from. Its id is
    /// sent to the servers that run tasks of the query's distributed plans, so the tasks of all parts
    /// of the query find the same groups there, whatever workload and settings each part uses.
    std::shared_ptr<QuerySchedulingGroup> getRoot();

    /// Whether a part of the query with this workload and these scheduling settings belongs to the
    /// group. A part that changes any of them is scheduled as a separate query.
    bool accepts(const String & workload_, const ClassifierSettings & settings_) const;

    const ClassifierPtr & getClassifier() const { return classifier; }

    /// Returns the group of a part of the query with this workload and these scheduling settings: the
    /// query's group if it accepts them, otherwise the derived group for them, made with a classifier
    /// from `make_classifier` if there is none. Gives the same result for the query's group and for
    /// any group derived from it.
    std::shared_ptr<QuerySchedulingGroup> getGroupFor(
        const String & workload_, const ClassifierSettings & settings_, const std::function<ClassifierPtr()> & make_classifier);

    /// Returns the query slot of the group, acquiring one through `link` if no member holds it.
    /// Waits while another member is being admitted. The slot is released when the last member
    /// holding it releases it.
    std::shared_ptr<QuerySlot> acquireQuerySlot(ResourceLink link, std::chrono::steady_clock::time_point admission_deadline);

private:
    const UUID id;
    const String workload;
    const ClassifierSettings settings;
    const ClassifierPtr classifier;
    const std::shared_ptr<QuerySchedulingGroup> parent;

    std::mutex derived_mutex;
    std::vector<std::weak_ptr<QuerySchedulingGroup>> derived TSA_GUARDED_BY(derived_mutex);

    std::timed_mutex admission_mutex;
    std::weak_ptr<QuerySlot> query_slot; /// Guarded by `admission_mutex`
};

using QuerySchedulingGroupPtr = std::shared_ptr<QuerySchedulingGroup>;

/// Groups of the queries whose distributed plans run on this server, by the id of the initiator's
/// group. The initiator registers the group of its query, and the tasks of the query that run on
/// this server join it here (the first task registers its own group if the initiator runs elsewhere).
/// An entry does not keep a group alive.
class DistributedQuerySchedulingGroups : private boost::noncopyable
{
public:
    static DistributedQuerySchedulingGroups & instance();

    /// Registers `group` under `group_id`, replacing a previous group with that id.
    void add(const String & group_id, const QuerySchedulingGroupPtr & group);

    /// Returns the group of a part of the query whose group has id `group_id`. `make_group` gets the
    /// live group registered under `group_id` (or `nullptr`) and returns the group of the part. If no
    /// live group is registered, the returned group is registered, so concurrent parts of one query
    /// get one group. A registered group is never replaced: a part that does not accept it joins a
    /// group derived from it (see `QuerySchedulingGroup::getGroupFor`).
    /// `make_group` is called under the lock of the registry.
    QuerySchedulingGroupPtr join(const String & group_id, const std::function<QuerySchedulingGroupPtr(const QuerySchedulingGroupPtr &)> & make_group);

private:
    void addLocked(const String & group_id, const QuerySchedulingGroupPtr & group) TSA_REQUIRES(mutex);

    std::mutex mutex;
    std::unordered_map<String, std::weak_ptr<QuerySchedulingGroup>> groups TSA_GUARDED_BY(mutex);
};

}
