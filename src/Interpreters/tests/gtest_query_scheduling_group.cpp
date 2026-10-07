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

/// The query's group keeps its derived groups, so parts with the same workload that run one after
/// another join the same group, and the derived groups go away with the query's group.
TEST(DistributedQuerySchedulingGroups, QueryGroupKeepsDerivedGroups)
{
    DistributedQuerySchedulingGroups groups;
    auto root = joinAs(groups, "query", "w");
    std::weak_ptr<QuerySchedulingGroup> weak_other = joinAs(groups, "query", "other");

    ASSERT_FALSE(weak_other.expired());
    EXPECT_EQ(joinAs(groups, "query", "other"), weak_other.lock());

    root.reset();
    EXPECT_TRUE(weak_other.expired());
}

/// Parts of the query find the same groups through the query's group and through a group derived
/// from it, and every derived group reports the query's group as its root.
TEST(DistributedQuerySchedulingGroups, DerivedGroupsAreFlat)
{
    auto root = makeGroup("w");
    auto other = root->getGroupFor("other", ClassifierSettings{}, noClassifier);
    ASSERT_NE(other, root);

    EXPECT_EQ(other->getRoot(), root);
    EXPECT_EQ(root->getRoot(), root);
    EXPECT_EQ(other->getGroupFor("w", ClassifierSettings{}, noClassifier), root);
    EXPECT_EQ(other->getGroupFor("other", ClassifierSettings{}, noClassifier), other);

    auto third = other->getGroupFor("third", ClassifierSettings{}, noClassifier);
    EXPECT_EQ(third->getRoot(), root);
    EXPECT_EQ(root->getGroupFor("third", ClassifierSettings{}, noClassifier), third);
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
