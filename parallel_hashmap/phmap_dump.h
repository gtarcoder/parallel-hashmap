#if !defined(phmap_dump_h_guard_)
#define phmap_dump_h_guard_

// ---------------------------------------------------------------------------
// Copyright (c) 2019, Gregory Popovitch - greg7mdp@gmail.com
//
//       providing dump/load/mmap_load
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// ---------------------------------------------------------------------------

#include <iostream>
#include <fstream>
#include <sstream>
#include "phmap.h"
namespace phmap
{

namespace type_traits_internal {

#if defined(__GLIBCXX__) && __GLIBCXX__ < 20150801
    template<typename T> struct IsTriviallyCopyable : public std::integral_constant<bool, __has_trivial_copy(T)> {};
#else
    template<typename T> struct IsTriviallyCopyable : public std::is_trivially_copyable<T> {};
#endif

template <class T1, class T2>
struct IsTriviallyCopyable<std::pair<T1, T2>> {
    static constexpr bool value = IsTriviallyCopyable<T1>::value && IsTriviallyCopyable<T2>::value;
};
}

namespace container_internal {

// ------------------------------------------------------------------------
// dump/load for raw_hash_set
// ------------------------------------------------------------------------
template <class Policy, class Hash, class Eq, class Alloc>
template<typename OutputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::dump(OutputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");

    if (!ar.dump(size_)) {
        std::cerr << "Failed to dump size_" << std::endl;
        return false;
    }
    if (size_ == 0) {
        return true;
    }
    if (!ar.dump(capacity_)) {
        std::cerr << "Failed to dump capacity_" << std::endl;
        return false;
    }
    if (!ar.dump(reinterpret_cast<char*>(ctrl_),
        sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1))) {

        std::cerr << "Failed to dump ctrl_" << std::endl;
        return false;
    }
    if (!ar.dump(reinterpret_cast<char*>(slots_),
                    sizeof(slot_type) * capacity_)) {
        std::cerr << "Failed to dump slot_" << std::endl;
        return false;
    }
    return true;
}

template <class Policy, class Hash, class Eq, class Alloc>
template<typename InputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::load(InputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");
    raw_hash_set<Policy, Hash, Eq, Alloc>().swap(*this); // clear any existing content
    if (!ar.load(&size_)) {
        std::cerr << "Failed to load size_" << std::endl;
        return false;
    }
    if (size_ == 0) {
        return true;
    }
    if (!ar.load(&capacity_)) {
        std::cerr << "Failed to load capacity_" << std::endl;
        return false;
    }

    // allocate memory for ctrl_ and slots_
    initialize_slots();
    if (!ar.load(reinterpret_cast<char*>(ctrl_),
        sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1))) {
        std::cerr << "Failed to load ctrl" << std::endl;
        return false;
    }
    if (!ar.load(reinterpret_cast<char*>(slots_),
                    sizeof(slot_type) * capacity_)) {
        std::cerr << "Failed to load slot" << std::endl;
        return false;
    }
    return true;
}


template <class Policy, class Hash, class Eq, class Alloc>
template<typename OutputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::mmap_dump(OutputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");

    size_t align_size = Layout::Alignment();
    if (!ar.dump(size_, align_size)) {
        std::cerr << "Failed to dump size_" << std::endl;
        return false;
    }
    if (size_ == 0) {
        return true;
    }
    if (!ar.dump(capacity_, align_size)) {
        std::cerr << "Failed to dump capacity_" << std::endl;
        return false;
    }
    if (!ar.dump(reinterpret_cast<char*>(ctrl_),
        sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1), align_size)) {

        std::cerr << "Failed to dump ctrl_" << std::endl;
        return false;
    }
    if (!ar.dump(reinterpret_cast<char*>(slots_),
                    sizeof(slot_type) * capacity_, align_size)) {
        std::cerr << "Failed to dump slot_" << std::endl;
        return false;
    }
    return true;
}

template <class Policy, class Hash, class Eq, class Alloc>
template<typename MmapInputArchive>
bool raw_hash_set<Policy, Hash, Eq, Alloc>::mmap_load(MmapInputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");
    raw_hash_set<Policy, Hash, Eq, Alloc>().swap(*this); // clear any existing content

    assert(ar.initialized());
    auto closure = ar.closure();
    this->alloc_ref().set_closure(closure);
    size_t align_size = Layout::Alignment();
    if (!ar.load(&size_, align_size)) {
        std::cerr << "Failed to load size!" << std::endl;
        return false;
    }
    if (size_ == 0) {
        return true;
    }
    if (!ar.load(&capacity_, align_size)) {
        std::cerr << "Failed to load capacity!" << std::endl;
        return false;
    }


    if (std::is_same<SlotAlloc, std::allocator<slot_type>>::value) {
        infoz_ = Sample();
    }
    reset_growth_left();
    infoz_.RecordStorageChanged(size_, capacity_);

    char* p_ctrl = ar.load(
        sizeof(ctrl_t) * (capacity_ + Group::kWidth + 1), align_size);
    ctrl_ = reinterpret_cast<ctrl_t*>(p_ctrl);

    char* p_slots = ar.load(sizeof(slot_type) * capacity_, align_size);
    slots_ = reinterpret_cast<slot_type*>(p_slots);
    return true;
}

// ------------------------------------------------------------------------
// dump/load for parallel_hash_set
// ------------------------------------------------------------------------
template <size_t N,
          template <class, class, class, class> class RefSet,
          class Mtx_,
          class Policy, class Hash, class Eq, class Alloc>
template<typename OutputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc>::dump(OutputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");

    if (! ar.dump(subcnt())) {
        std::cerr << "Failed to dump meta!" << std::endl;
        return false;
    }
    for (size_t i = 0; i < sets_.size(); ++i) {
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.dump(ar)) {
            std::cerr << "Failed to dump submap " << i << std::endl;
            return false;
        }
    }
    return true;
}

template <size_t N,
          template <class, class, class, class> class RefSet,
          class Mtx_,
          class Policy, class Hash, class Eq, class Alloc>
template<typename InputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc>::load(InputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");

    size_t submap_count = 0;
    if (!ar.load(&submap_count)) {
        std::cerr << "Failed to load submap count!" << std::endl;
        return false;
    }

    if (submap_count != subcnt()) {
        std::cerr << "submap count(" << submap_count << ") != N(" << N << ")" << std::endl;
        return false;
    }

    for (size_t i = 0; i < submap_count; ++i) {
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.load(ar)) {
            std::cerr << "Failed to load submap " << i << std::endl;
            return false;
        }
    }
    return true;
}


template <size_t N,
          template <class, class, class, class> class RefSet,
          class Mtx_,
          class Policy, class Hash, class Eq, class Alloc>
template<typename OutputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc>::mmap_dump(OutputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");

    size_t align_size = EmbeddedSet::Layout::Alignment();
    if (! ar.dump(subcnt(), align_size)) {
        std::cerr << "Failed to dump meta!" << std::endl;
        return false;
    }
    for (size_t i = 0; i < sets_.size(); ++i) {
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.mmap_dump(ar)) {
            std::cerr << "Failed to dump submap " << i << std::endl;
            return false;
        }
    }
    return true;
}

template <size_t N,
          template <class, class, class, class> class RefSet,
          class Mtx_,
          class Policy, class Hash, class Eq, class Alloc>
template<typename InputArchive>
bool parallel_hash_set<N, RefSet, Mtx_, Policy, Hash, Eq, Alloc>::mmap_load(InputArchive& ar) {
    static_assert(type_traits_internal::IsTriviallyCopyable<value_type>::value,
                    "value_type should be trivially copyable");

    assert(ar.initialized());
    auto closure = ar.closure();
    this->alloc_ref().set_closure(closure);
    size_t submap_count = 0;
    size_t align_size = EmbeddedSet::Layout::Alignment();
    if (!ar.load(&submap_count, align_size)) {
        std::cerr << "Failed to load submap count!" << std::endl;
        return false;
    }

    if (submap_count != subcnt()) {
        std::cerr << "submap count(" << submap_count << ") != N(" << N << ")" << std::endl;
        return false;
    }

    for (size_t i = 0; i < submap_count; ++i) {
        auto& inner = sets_[i];
        typename Lockable::UniqueLock m(const_cast<Inner&>(inner));
        if (!inner.set_.mmap_load(ar)) {
            std::cerr << "Failed to load submap " << i << std::endl;
            return false;
        }
    }
    return true;
}

} // namespace container_internal



// ------------------------------------------------------------------------
// BinaryArchive
//       File is closed when archive object is destroyed
// ------------------------------------------------------------------------

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------
class BinaryOutputArchive {
public:
    BinaryOutputArchive(const char *file_path) {
        ofs_.open(file_path, std::ios_base::binary);
    }

    bool dump(const char *p, size_t sz) {
        ofs_.write(p, sz);
        return true;
    }

    template<typename V>
    typename std::enable_if<type_traits_internal::IsTriviallyCopyable<V>::value, bool>::type
    dump(const V& v) {
        ofs_.write(reinterpret_cast<const char *>(&v), sizeof(V));
        return true;
    }

private:
    std::ofstream ofs_;
};


class BinaryInputArchive {
public:
    BinaryInputArchive(const char * file_path) {
        ifs_.open(file_path, std::ios_base::binary);
    }

    bool load(char* p, size_t sz) {
        ifs_.read(p, sz);
        return true;
    }

    template<typename V>
    typename std::enable_if<type_traits_internal::IsTriviallyCopyable<V>::value, bool>::type
    load(V* v) {
        ifs_.read(reinterpret_cast<char *>(v), sizeof(V));
        return true;
    }

private:
    std::ifstream ifs_;
};


#if defined(__linux__)  // only support linux's mmap now
// mmap dump && mmap load
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

class MmapOutputArchive {
public:
    MmapOutputArchive(const std::string& file_path): offset_(0) {
        ofs_.open(file_path.c_str(), std::ios_base::binary);
    }

    virtual ~MmapOutputArchive() {
    }

    bool dump(char* p, size_t sz, size_t align_size) {
        ofs_.write(p, sz);
        offset_ += sz;
        return align(align_size);
    }

    template<typename V>
    typename std::enable_if<type_traits_internal::IsTriviallyCopyable<V>::value, bool>::type
    dump(const V& v, size_t align_size) {
        ofs_.write(reinterpret_cast<char*>(const_cast<V*>(&v)), sizeof(V));
        offset_ += sizeof(V);
        return align(align_size);
    }

private:
    // padding for align
    bool align(size_t align) {
        size_t padding_size = (offset_ + align - 1) / align * align - offset_;
        if (padding_size == 0) {
            return true;
        }
        std::string padding(padding_size, '\0');
        ofs_.write(padding.c_str(), padding_size);
        offset_ += padding_size;
        return true;
    }

    std::ofstream ofs_;
    size_t offset_;
};


class MmapInputArchive {
public:
    class MmapClosure {
    public:
        MmapClosure(bool init = false, size_t len = 0, void* a = NULL):
            initialized(init), length(len), addr(a) {};

        ~MmapClosure() {
            if (initialized && addr != MAP_FAILED) {
                munmap(addr, length);
            }
        }
        bool initialized;
        size_t length;
        void* addr;
    };

    MmapInputArchive(const std::string& file_path):
        initialized_(false), closure_(nullptr) {
        int fd = open(file_path.c_str(), O_RDWR);
        if (fd == -1) {
            std::cerr << "Failed to open file " << file_path << std::endl;
            return;
        }

        struct stat st;
        if (fstat(fd, &st) == -1) {
            std::cerr << "Failed to stat file " << file_path << std::endl;
            close(fd);
            return;
        }

        file_size_ = st.st_size;

        addr_ = mmap(NULL, file_size_, PROT_READ|PROT_WRITE,
            MAP_SHARED|MAP_LOCKED|MAP_POPULATE, fd, 0);
        if (addr_ == MAP_FAILED) {
            std::cerr << "Failed to mmap file " << file_path << std::endl;
            close(fd);
            return;
        }

        close(fd);
        initialized_ = true;
        offset_ = 0;
        closure_ = std::make_shared<MmapClosure>(initialized_, file_size_, addr_);
    };

    ~MmapInputArchive() {
    }

    bool initialized() const {
        return initialized_;
    }

    template<typename T>
    bool load(T* t, size_t align_size = 1) {
        assert(offset_ + sizeof(T) <= file_size_);
        char* p = (char*)addr_ + offset_;
        offset_ += sizeof(T);
        *t = *(reinterpret_cast<T*>(p));
        return align(align_size);
    }

    char* load(size_t n, size_t align_size = 1) {
        assert(offset_ + n <= file_size_);
        char* p = (char*)addr_ + offset_;
        offset_ += n;
        align(align_size);
        return p;
    }

    std::shared_ptr<MmapClosure> closure() {
        return closure_;
    }
private:
    // padding for align
    bool align(size_t align) {
        size_t padding_size = (offset_ + align - 1) / align * align - offset_;
        if (padding_size == 0) {
            return true;
        }
        offset_ += padding_size;
        return true;
    }

    bool initialized_;
    size_t file_size_;
    void* addr_;
    size_t offset_;
    std::shared_ptr<MmapClosure> closure_;
};

template<typename T>
class MmapAllocator: public std::allocator<T> {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using MmapClosure = typename MmapInputArchive::MmapClosure;

    MmapAllocator(): closure(nullptr) {
    }

    ~MmapAllocator() {
        // will call ~MmapClosure();
    }

    MmapAllocator(const MmapAllocator& m) {
        this->closure  = m.closure;
    };

    template<typename U>
    MmapAllocator(const MmapAllocator<U>& m) {
        this->closure = m.closure;
    };


    inline pointer allocate(size_type n, const void * = 0) {
        auto ret = std::allocator<T>::allocate(n);
        return ret;
    }

    inline void deallocate(pointer p, size_type n) {
        // mmaped memory, do not free here
        if (closure
            && (char*)closure->addr <= (char*)p
            && (char*)p < (char*)closure->addr + closure->length) {
            return;
        } else {
            std::allocator<T>::deallocate(p, n);
        }
    }
    template<typename U>
    struct rebind {
        typedef MmapAllocator<U> other;
    };

    void set_closure(std::shared_ptr<MmapClosure> c) {
        if (closure == nullptr) {
            closure = c;
        }
    }
public:
    std::shared_ptr<MmapClosure> closure;
};
#endif // end if __linux__

} // namespace phmap

#endif // phmap_dump_h_guard_
