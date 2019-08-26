#include <iostream>
#include <parallel_hashmap/phmap_dump.h>

void dump_load_uint64_uint32() {
    phmap::flat_hash_map<uint64_t, uint32_t> mp1 = { {100, 99}, {300, 299} };

    for (const auto& n : mp1)
        std::cout << n.first << "'s value is: " << n.second << "\n";

    {
        phmap::BinaryOutputArchive ar_out("./dump.data");
        mp1.dump(ar_out);
    }

    phmap::flat_hash_map<uint64_t, uint32_t> mp2;
    {
        phmap::BinaryInputArchive ar_in("./dump.data");
        mp2.load(ar_in);
    }

    for (const auto& n : mp2)
        std::cout << n.first << "'s value is: " << n.second << "\n";
}

void dump_load_parallel_flat_hash_map() {
    phmap::parallel_flat_hash_map<uint64_t, uint32_t> mp1 = {
        {100, 99}, {300, 299}, {101, 992} };

    for (const auto& n : mp1)
        std::cout << "key: " << n.first << ", value: " << n.second << "\n";

    {
        phmap::BinaryOutputArchive ar_out("./dump.data");
        mp1.dump(ar_out);
    }

    phmap::parallel_flat_hash_map<uint64_t, uint32_t> mp2;
    {
        phmap::BinaryInputArchive ar_in("./dump.data");
        mp2.load(ar_in);
    }

     for (const auto& n : mp2)
        std::cout << "key: " << n.first << ", value: " << n.second << "\n";
}

#if defined(__linux__)
void mmap_load_uint64_uint32() {
    using MapType = phmap::flat_hash_map<uint64_t, uint32_t,
            phmap::container_internal::hash_default_hash<uint64_t>,
            phmap::container_internal::hash_default_eq<uint64_t>,
            phmap::MmapAllocator<
                        phmap::container_internal::Pair<const uint64_t, uint32_t>>>;
    MapType mp1{ {100, 99}, {300, 299} };
    for (const auto& n : mp1)
        std::cout << n.first << "'s value is: " << n.second << "\n";

    {
        phmap::MmapOutputArchive ar_out("./dump.data");
        mp1.mmap_dump(ar_out);
    }

    MapType mp2;
    {
        phmap::MmapInputArchive ar_in("./dump.data");
        mp2.mmap_load(ar_in);
    }

    for (const auto& n : mp2)
        std::cout << n.first << "'s value is: " << n.second << "\n";
}

void mmap_load_parallel_flat_hash_map() {
    using MapType = phmap::parallel_flat_hash_map<uint64_t, uint32_t,
            phmap::container_internal::hash_default_hash<uint64_t>,
            phmap::container_internal::hash_default_eq<uint64_t>,
            phmap::MmapAllocator<
                        phmap::container_internal::Pair<const uint64_t, uint32_t>>,
            4,
            phmap::NullMutex>;

    MapType mp1 {{100, 99}, {300, 299}, {101, 992} };
    {
        phmap::MmapOutputArchive ar_out("./dump.data");
        mp1.mmap_dump(ar_out);
    }

    MapType mp2;
    {
        phmap::MmapInputArchive ar_in("./dump.data");
        mp2.mmap_load(ar_in);
    }

    for (const auto& n : mp2)
        std::cout << n.first << "'s value is: " << n.second << "\n";
}
#endif

int main()
{
    dump_load_uint64_uint32();
    dump_load_parallel_flat_hash_map();
#if defined(__linux__)
    mmap_load_uint64_uint32();
    mmap_load_parallel_flat_hash_map();
#endif
    return 0;
}

