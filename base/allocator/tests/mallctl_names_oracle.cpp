/// Walks every name of the `mallctl` tree (inner nodes and leaves; every indexed level with a set of interesting
/// indices, including 0, 1, 4095, 4096, 4097 and the bounds of the index functions) and checks that the reference
/// jemalloc's `mallctlnametomib` gives the same result (error code, MIB length and MIB), also with truncated MIB
/// buffers, for lenient index spellings and for names unknown to both. Also checks that no named level of the
/// reference has more children than ours, and the sizes of the ctl structures.

#include <allocator/Arenas.h>
#include <allocator/BackgroundThread.h>
#include <allocator/Base.h>
#include <allocator/ExtentMap.h>
#include <allocator/Mallctl.h>
#include <allocator/MallctlImpl.h>
#include <allocator/Options.h>
#include <allocator/Pages.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadState.h>

#include "Test.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstring>

extern "C" {
int je_mallctl(const char * name, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length);
int je_mallctlnametomib(const char * name, size_t * mib_ptr, size_t * mib_length_ptr);
int je_mallctlbymib(const size_t * mib, size_t mib_length, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length);

size_t ref_sizeof_mallctl_arena();
size_t ref_sizeof_mallctl_arenas();
size_t ref_sizeof_mallctl_stats();
size_t ref_sizeof_mallctl_arena_stats();
int ref_option_profiling();
int ref_option_profiling_stats();

/// The reference pulls in the libunwind-based profiler backtrace, which is never called here.
int unw_backtrace(void **, int)
{
    return 0;
}
}

using namespace jemalloc;

namespace
{

ThreadState test_thread_state;

/// The arena state of the reference (`arenas.narenas`, `arena.<i>.initialized` as of its first refresh), mirrored
/// into our arena table (real arenas at the same indices).
unsigned ref_num_arenas = 0;
bool ref_initialized[MALLOCX_ARENA_LIMIT];

/// Boots the arena layer as the a0 initialization does (`arena_boot`, the global extent map, the arena table) and
/// creates real arenas (the ctl merges their stats on refresh).
void bootArenas()
{
    sizeBoot(default_size_class_data, options.cache_oblivious);
    REQUIRE(!arena_extent_map_global.init(base0Get(), /* zeroed */ true));
    REQUIRE(!arenaBoot(&default_size_class_data, base0Get(), false));
    REQUIRE(!arenas_lock.init("arenas", MutexRank::ARENAS, MutexLockOrder::RankExclusive));
    num_arenas_auto = 1;
    manual_arena_base = 2;
    numArenasTotalSet(1);
    REQUIRE(!backgroundThreadBoot0());
    REQUIRE(!backgroundThreadBoot1(nullptr, base0Get()));
}

/// `arenaNew` enters and leaves reentrancy for `idx != 0`, which needs a tsd in a nominal state.
ThreadState & arenaThreadState()
{
    static ThreadState thread_state;
    thread_state.state.store(thread_state_nominal_slow, std::memory_order_relaxed);
    return thread_state;
}

void setUpArenaTable()
{
    bootArenas();
    for (unsigned i = 0; i < ref_num_arenas; ++i)
        if (ref_initialized[i])
            REQUIRE(arenaNew(&arenaThreadState(), i, &arena_config_default) != nullptr);
    numArenasTotalSet(ref_num_arenas);
}

void setUp()
{
    static bool done = false;
    if (done)
        return;
    done = true;

    size_t len = sizeof(ref_num_arenas);
    REQUIRE(je_mallctl("arenas.narenas", &ref_num_arenas, &len, nullptr, 0) == 0);
    REQUIRE(ref_num_arenas > 0 && ref_num_arenas <= MALLOCX_ARENA_LIMIT);
    for (unsigned i = 0; i < ref_num_arenas; ++i)
    {
        char name[64];
        std::snprintf(name, sizeof(name), "arena.%u.initialized", i);
        len = sizeof(bool);
        REQUIRE(je_mallctl(name, &ref_initialized[i], &len, nullptr, 0) == 0);
    }

    /// The options that decide whether index functions accept indices.
    options.profiling = ref_option_profiling() != 0;
    options.profiling_stats = ref_option_profiling_stats() != 0;

    REQUIRE(!pages::boot());
    REQUIRE(!baseBoot(nullptr));
    setUpArenaTable();
    REQUIRE(!mallctlBoot());
}

struct Totals
{
    size_t names = 0;
    size_t found = 0;
    size_t leaves = 0;
    size_t mismatches = 0;
};

Totals totals;

/// Compares one name (with the full and with truncated MIB buffers). `expected` is the MIB of the name in our tree
/// (if `expect_found`).
void compareName(const char * name, const size_t * expected, size_t expected_len, bool expect_found)
{
    ++totals.names;
    size_t ref_mib[MALLCTL_MAX_DEPTH] = {};
    size_t our_mib[MALLCTL_MAX_DEPTH] = {};
    size_t ref_len = MALLCTL_MAX_DEPTH;
    size_t our_len = MALLCTL_MAX_DEPTH;
    int ref_result = je_mallctlnametomib(name, ref_mib, &ref_len);
    int our_result = mallctlNameToMIB(test_thread_state, name, our_mib, &our_len);

    bool ok = ref_result == our_result;
    if (ok && our_result == 0)
    {
        ok = ref_len == our_len && std::memcmp(ref_mib, our_mib, our_len * sizeof(size_t)) == 0;
        if (ok && expect_found)
            ok = our_len == expected_len && std::memcmp(our_mib, expected, our_len * sizeof(size_t)) == 0;
    }
    if (!ok)
    {
        ++totals.mismatches;
        if (totals.mismatches <= 30)
        {
            std::fprintf(stderr, "Mismatch for \"%s\": reference %d (len %zu:", name, ref_result, ref_len);
            for (size_t i = 0; i < ref_len && ref_result == 0; ++i)
                std::fprintf(stderr, " %zu", ref_mib[i]);
            std::fprintf(stderr, "), ours %d (len %zu:", our_result, our_len);
            for (size_t i = 0; i < our_len && our_result == 0; ++i)
                std::fprintf(stderr, " %zu", our_mib[i]);
            std::fprintf(stderr, ")\n");
        }
        CHECK(false);
        return;
    }
    if (our_result != 0)
        return;
    ++totals.found;

    /// Truncated MIB buffers return a prefix successfully.
    for (size_t capacity = 1; capacity < our_len; ++capacity)
    {
        size_t ref_mib2[MALLCTL_MAX_DEPTH] = {};
        size_t our_mib2[MALLCTL_MAX_DEPTH] = {};
        size_t ref_len2 = capacity;
        size_t our_len2 = capacity;
        int ref_result2 = je_mallctlnametomib(name, ref_mib2, &ref_len2);
        int our_result2 = mallctlNameToMIB(test_thread_state, name, our_mib2, &our_len2);
        CHECK_EQ(our_result2, ref_result2);
        CHECK_EQ(our_len2, ref_len2);
        CHECK(std::memcmp(ref_mib2, our_mib2, sizeof(ref_mib2)) == 0);
    }

    /// `mallctlMIBNameToMIB` resolves the rest of the name relative to every prefix.
    for (const char * dot = std::strchr(name, '.'); dot != nullptr; dot = std::strchr(dot + 1, '.'))
    {
        char prefix[256];
        size_t prefix_len = static_cast<size_t>(dot - name);
        std::memcpy(prefix, name, prefix_len);
        prefix[prefix_len] = '\0';
        size_t mib[MALLCTL_MAX_DEPTH] = {};
        size_t mib_length = MALLCTL_MAX_DEPTH;
        if (mallctlNameToMIB(test_thread_state, prefix, mib, &mib_length) != 0)
            continue;
        size_t total_len = MALLCTL_MAX_DEPTH;
        CHECK_EQ(mallctlMIBNameToMIB(test_thread_state, mib, mib_length, dot + 1, &total_len), 0);
        CHECK_EQ(total_len, our_len);
        CHECK(std::memcmp(mib, our_mib, our_len * sizeof(size_t)) == 0);
    }
}

/// The indices tried at every indexed level.
size_t indices[32];
size_t num_indices = 0;

void addIndex(size_t i)
{
    for (size_t k = 0; k < num_indices; ++k)
        if (indices[k] == i)
            return;
    REQUIRE(num_indices < std::size(indices));
    indices[num_indices++] = i;
}

void initIndices()
{
    for (size_t i : {size_t(0), size_t(1), size_t(2), size_t(4095), size_t(4096), size_t(4097), size_t(4098)})
        addIndex(i);
    for (size_t bound :
         {size_t(SIZE_CLASS_NUM_BINS),
          size_t(SIZE_CLASS_NUM_SIZES - SIZE_CLASS_NUM_BINS),
          size_t(SIZE_CLASS_NUM_PAGE_SIZES),
          size_t(64),
          size_t(ref_num_arenas)})
    {
        addIndex(bound - 1);
        addIndex(bound);
        addIndex(bound + 1);
    }
    addIndex(size_t(UINT32_MAX) + 1);
}

/// Visits `node` (named `name`, MIB `mib[0 .. depth)`) and its subtree.
void walk(const MallctlNode & node, char * name, size_t name_len, size_t * mib, size_t depth)
{
    compareName(name, mib, depth, true);
    if (node.isLeaf())
    {
        ++totals.leaves;
        return;
    }

    /// Names unknown to both.
    {
        char bogus[256];
        std::snprintf(bogus, sizeof(bogus), "%s.no_such_child", name);
        compareName(bogus, nullptr, 0, false);
        std::snprintf(bogus, sizeof(bogus), "%s.", name);
        compareName(bogus, nullptr, 0, false);
        std::snprintf(bogus, sizeof(bogus), "%s..x", name);
        compareName(bogus, nullptr, 0, false);
    }

    if (node.isIndexed())
    {
        const MallctlNode & super_node = node.children[0];
        for (size_t k = 0; k < num_indices; ++k)
        {
            int n = std::snprintf(name + name_len, 256 - name_len, ".%zu", indices[k]);
            mib[depth] = indices[k];
            /// Only valid indices are walked further (the comparison of the node itself covers invalid ones).
            size_t probe_mib[MALLCTL_MAX_DEPTH];
            size_t probe_len = MALLCTL_MAX_DEPTH;
            bool valid = mallctlNameToMIB(test_thread_state, name, probe_mib, &probe_len) == 0;
            compareName(name, mib, depth + 1, valid);
            if (!valid)
                continue;
            for (size_t j = 0; j < super_node.num_children; ++j)
            {
                const MallctlNode & child = super_node.children[j];
                int m = std::snprintf(name + name_len + n, 256 - name_len - n, ".%s", child.name);
                mib[depth + 1] = j;
                walk(child, name, name_len + n + m, mib, depth + 2);
            }
        }
        /// Lenient spellings of the index (`malloc_strtoumax` skips whitespace, accepts a sign and stops at the
        /// first non-digit) and invalid ones.
        for (const char * spelling :
             {"0x", " 1", "+1", "01", "-1", "-2", "", "x", "18446744073709551615", "18446744073709551614", "99999999999999999999"})
        {
            char lenient[256];
            std::snprintf(lenient, sizeof(lenient), "%s.%s", name, spelling);
            compareName(lenient, nullptr, 0, false);
            if (super_node.num_children > 0)
            {
                std::snprintf(lenient, sizeof(lenient), "%s.%s.%s", name, spelling, super_node.children[0].name);
                compareName(lenient, nullptr, 0, false);
            }
        }
        name[name_len] = '\0';
        return;
    }

    for (size_t j = 0; j < node.num_children; ++j)
    {
        const MallctlNode & child = node.children[j];
        int n = name_len == 0 ? std::snprintf(name, 256, "%s", child.name)
                              : std::snprintf(name + name_len, 256 - name_len, ".%s", child.name);
        mib[depth] = j;
        walk(child, name, name_len + n, mib, depth + 1);
        name[name_len] = '\0';
    }

    /// The reference has no named child past our last one.
    if (depth + 1 <= MALLCTL_MAX_DEPTH)
    {
        mib[depth] = node.num_children;
        CHECK_EQ(je_mallctlbymib(mib, depth + 1, nullptr, nullptr, nullptr, 0), ENOENT);
        CHECK_EQ(mallctlByMIB(test_thread_state, mib, depth + 1, nullptr, nullptr, nullptr, 0), ENOENT);
    }
}

}

TEST(MallctlNamesOracle, StructureSizes)
{
    CHECK_EQ(sizeof(MallctlArena), ref_sizeof_mallctl_arena());
    CHECK_EQ(sizeof(MallctlArenas), ref_sizeof_mallctl_arenas());
    CHECK_EQ(sizeof(MallctlStats), ref_sizeof_mallctl_stats());
    /// For the implementation of `MallctlArenaStats`.
    std::fprintf(stderr, "sizeof(ctl_arena_stats_t) = %zu\n", ref_sizeof_mallctl_arena_stats());
}

TEST(MallctlNamesOracle, AllNames)
{
    setUp();
    initIndices();

    char name[256] = "";
    size_t mib[MALLCTL_MAX_DEPTH + 1];
    walk(mallctl_super_root_node[0], name, 0, mib, 0);

    std::fprintf(
        stderr,
        "%zu names compared, %zu found, %zu leaves, %zu mismatches (reference narenas = %u)\n",
        totals.names,
        totals.found,
        totals.leaves,
        totals.mismatches,
        ref_num_arenas);
    CHECK_EQ(totals.mismatches, size_t(0));
    CHECK_GT(totals.leaves, size_t(1000));
}

TEST(MallctlNamesOracle, MiscellaneousNames)
{
    setUp();
    for (const char * name :
         {"",
          ".",
          "..",
          ".version",
          "version.",
          "version..",
          "Version",
          "versio",
          "versionx",
          "epoch.0",
          "stats.arenas.0.",
          "stats.arenas.0x.pactive",
          "stats.arenas. 0.pactive",
          "stats.arenas.0.bins.0.mutex.num_ops.x",
          "arena.4096.decay",
          "arena.4097.decay",
          "arenas.bin.0.size",
          "arenas.bin.0",
          "arenas.bin",
          "arenas",
          "opt.malloc_conf",
          "opt.malloc_conf.symlink",
          "thread.tcache.ncached_max.write",
          "experimental.hooks.prof_sample",
          "stats.mutexes.reset",
          "stats.mutexes.ctl.max_num_thds",
          "prof.stats.bins.0.live",
          "prof.stats.lextents.0.accum"})
    {
        compareName(name, nullptr, 0, false);
    }
    CHECK_EQ(totals.mismatches, size_t(0));
}
