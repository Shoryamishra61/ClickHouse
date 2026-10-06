#include <Interpreters/QuerySchedulingGroup.h>

#include <gtest/gtest.h>

#include <barrier>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

QuerySchedulingGroupPtr makeGroup(const String & workload)
{
    return std::make_shared<QuerySchedulingGroup>(workload, ClassifierSettings{}, /*classifier_=*/ nullptr);
}

ClassifierPtr noClassifier()
{
    return nullptr;
}

/// Joins the query whose group is registered under `group_id` as a part with workload `workload`,
/// the way a task does on a worker.
QuerySchedulingGroupPtr joinAs(DistributedQuerySchedulingGroups & groups, const String & group_id, const String & workload)
{
    return groups.join(group_id, [&](const QuerySchedulingGroupPtr & registered)
    {
        if (registered)
            return registered->getGroupFor(workload, ClassifierSettings{}, noClassifier);
        return makeGroup(workload);
    });
}

}

/// Parts of one query that join concurrently, with no group registered yet, get one group.
TEST(DistributedQuerySchedulingGroups, ConcurrentJoinGetsOneGroup)
{
    DistributedQuerySchedulingGroups groups;
    constexpr size_t num_threads = 16;
    std::vector<QuerySchedulingGroupPtr> joined(num_threads);
    std::barrier<> start(num_threads);

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
        threads.emplace_back([&, i]
        {
            start.arrive_and_wait();
            joined[i] = joinAs(groups, "query", "w");
        });
    for (auto & thread : threads)
        thread.join();

    for (const auto & group : joined)
        EXPECT_EQ(group, joined.front());
}

/// Parts that do not accept the registered group share a derived group per workload, and the
/// registered group stays.
TEST(DistributedQuerySchedulingGroups, MismatchedPartsShareDerivedGroup)
{
    DistributedQuerySchedulingGroups groups;
    auto initiator = makeGroup("w");
    groups.add("query", initiator);

    auto other = joinAs(groups, "query", "other");
    EXPECT_NE(other, initiator);
    EXPECT_EQ(joinAs(groups, "query", "other"), other);
    EXPECT_NE(joinAs(groups, "query", "third"), other);

    EXPECT_EQ(joinAs(groups, "query", "w"), initiator);
}

/// A derived group keeps the group it is derived from alive, so the parts that come after the last
/// part of the original group finished still join the same groups.
TEST(DistributedQuerySchedulingGroups, DerivedGroupKeepsParentAlive)
{
    DistributedQuerySchedulingGroups groups;
    auto first = joinAs(groups, "query", "w");
    auto other = joinAs(groups, "query", "other");
    std::weak_ptr<QuerySchedulingGroup> weak_first = first;
    first.reset();

    ASSERT_FALSE(weak_first.expired());
    EXPECT_EQ(joinAs(groups, "query", "other"), other);
    EXPECT_EQ(joinAs(groups, "query", "w"), weak_first.lock());
}

/// A part that finds only an expired entry registers its own group.
TEST(DistributedQuerySchedulingGroups, ExpiredGroupIsReplaced)
{
    DistributedQuerySchedulingGroups groups;
    groups.add("query", makeGroup("w"));

    auto group = joinAs(groups, "query", "w");
    ASSERT_NE(group, nullptr);
    EXPECT_EQ(joinAs(groups, "query", "w"), group);
}
