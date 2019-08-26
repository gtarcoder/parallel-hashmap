#include <vector>

#include "gtest/gtest.h"

#include "parallel_hashmap/phmap_dump.h"

namespace phmap {
namespace container_internal {
namespace {

TEST(DumpLoad, FlatHashSet_uin32) {
    phmap::flat_hash_set<uint32_t> st1 = { 1991, 1202 };

    {
        phmap::BinaryOutputArchive ar_out("./dump.data");
        EXPECT_TRUE(st1.dump(ar_out));
    }

    phmap::flat_hash_set<uint32_t> st2;
    {
        phmap::BinaryInputArchive ar_in("./dump.data");
        EXPECT_TRUE(st2.load(ar_in));
    }
    EXPECT_TRUE(st1 == st2);
}

TEST(DumpLoad, FlatHashMap_uint64_uint32) {
    phmap::flat_hash_map<uint64_t, uint32_t> mp1 = {
        { 78731, 99}, {13141, 299}, {2651, 101} };

    {
        phmap::BinaryOutputArchive ar_out("./dump.data");
        EXPECT_TRUE(mp1.dump(ar_out));
    }

    phmap::flat_hash_map<uint64_t, uint32_t> mp2;
    {
        phmap::BinaryInputArchive ar_in("./dump.data");
        EXPECT_TRUE(mp2.load(ar_in));
    }

    EXPECT_TRUE(mp1 == mp2);
}

TEST(DumpLoad, ParallelFlatHashMap_uint64_uint32) {
    phmap::parallel_flat_hash_map<uint64_t, uint32_t> mp1 = {
        {99, 299}, {992, 2991}, {299, 1299} };

    {
        phmap::BinaryOutputArchive ar_out("./dump.data");
        EXPECT_TRUE(mp1.dump(ar_out));
    }

    phmap::parallel_flat_hash_map<uint64_t, uint32_t> mp2;
    {
        phmap::BinaryInputArchive ar_in("./dump.data");
        EXPECT_TRUE(mp2.load(ar_in));
    }
    EXPECT_TRUE(mp1 == mp2);
}

#if defined(__linux__)
TEST(MmapDumpLoad, FlatHashMap_uint64_uint32) {
    using MapType = phmap::flat_hash_map<uint64_t, uint32_t,
            phmap::container_internal::hash_default_hash<uint64_t>,
            phmap::container_internal::hash_default_eq<uint64_t>,
            phmap::MmapAllocator<
                        phmap::container_internal::Pair<const uint64_t, uint32_t>>>;

    MapType mp1 = {{ 78731, 99}, {13141, 299}, {2651, 101} };
    {
        phmap::MmapOutputArchive ar_out("./dump.data");
        EXPECT_TRUE(mp1.mmap_dump(ar_out));
    }

    MapType mp2;
    {
        phmap::MmapInputArchive ar_in("./dump.data");
        EXPECT_TRUE(mp2.mmap_load(ar_in));
    }
    EXPECT_TRUE(mp1 == mp2);
}

TEST(MmapDumpLoad, ParallelFlatHashMap_uint64_uint32) {
    using MapType = parallel_flat_hash_map<uint64_t, uint32_t,
            phmap::container_internal::hash_default_hash<uint64_t>,
            phmap::container_internal::hash_default_eq<uint64_t>,
            phmap::MmapAllocator<
                        phmap::container_internal::Pair<const uint64_t, uint32_t>>,
            4,
            phmap::NullMutex>;

    MapType mp1 = {{ 78731, 99}, {13141, 299}, {2651, 101} };
    {
        phmap::MmapOutputArchive ar_out("./dump.data");
        EXPECT_TRUE(mp1.mmap_dump(ar_out));
    }

    MapType mp2;
    {
        phmap::MmapInputArchive ar_in("./dump.data");
        EXPECT_TRUE(mp2.mmap_load(ar_in));
    }
    EXPECT_TRUE(mp1 == mp2);
}
#endif
}
}
}

