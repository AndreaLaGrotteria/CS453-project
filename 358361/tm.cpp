/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers

// Internal headers
#include <tm.h>

#include "macros.h"

#include <atomic>
#include <unordered_set>
#include <map>
#include <vector>
#include <string.h>

#define NUM_SEGMENTS 512
#define NUM_WORDS 2048

//Global vars
static std::atomic_uint timestamp_global{0};

//Thread vars


//Generic Lock implementation
struct Lock{
    bool is_locked;
    uint64_t stamp;
};

//TimeStampLock implementation
class TimeStampLock{
    private:
    std::atomic_uint64_t lock;

    public:
    TimeStampLock() : lock(0) {}
    TimeStampLock(const TimeStampLock &timestamp) { 
        lock = timestamp.lock.load(); 
    }

    Lock timestamp_lock_get_value()
    {
        Lock result;
        uint64_t val = this->lock.load();
        uint64_t stamp, lock;
        stamp = val >> 1;
        lock = val & 1;

        if(lock == 1) result.is_locked = true;
        else result.is_locked = false;
        result.stamp = stamp;
        return result;
    }

    bool lock_CAS(Lock old_lock, Lock new_lock)
    {
        // if((stamp >> 63) == 1) throw -1;
        uint64_t old_val = (old_lock.stamp << 1) | old_lock.is_locked;;
        uint64_t new_val = (new_lock.stamp << 1) | new_lock.is_locked;
        return this->lock.compare_exchange_strong(old_val, new_val);
    }

    bool get_lock(){
        // get value if unlocked by locking it and CandS
        Lock old_value = this->timestamp_lock_get_value();
        if(old_value.is_locked) return false;
        Lock new_value = {true, old_value.stamp};
        return lock_CAS(new_value,old_value);
    }

    bool release_lock(TimeStampLock* lock, uint64_t stamp, bool set_new){
        // get value if unlocked by locking it and CandS
        Lock old_value = this->timestamp_lock_get_value();
        if(!old_value.is_locked) return false;
        if(set_new){
            Lock new_value = {false, stamp};
            return lock_CAS(new_value,old_value);
        } else{
            Lock new_value = {false, old_value.stamp};
            return lock_CAS(new_value,old_value);
        }
    }
};

struct WordLock{
    WordLock() : lock(), id(0) {};
    TimeStampLock lock;
    uint64_t id;
};


// Transaction implementation
struct Tx{
    Tx(bool is_ro) : is_ro(is_ro), rv(0), wv(0) {};
    bool is_ro;
    uint64_t rv, wv;
    std::unordered_set<void*> read_set;
    std::map<uintptr_t, void*> write_set;
};

// Shared memory region implementation
struct MemoryRegion{
    MemoryRegion(size_t size, size_t align) : size(size), align(align), allocated_segments(2), segments(NUM_SEGMENTS, std::vector<WordLock>(NUM_WORDS)) {}
    size_t size, align;
    // number of allocated segments 
    std::atomic<uint64_t> allocated_segments;
    // contains all the segments, each segment made up of words
    std::vector<std::vector<WordLock>> segments;
};


/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) {
    MemoryRegion* region = new MemoryRegion(size, align);
    if(unlikely(region == NULL)) return invalid_shared;
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) {
    delete (MemoryRegion*) shared;
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) {
    return (void *)((uint64_t)1 << 32);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) {
    return ((MemoryRegion*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) {
    return ((MemoryRegion*) shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro)) {
    Tx* tx = new Tx(is_ro);
    if(unlikely(tx == NULL)) return invalid_tx;
    tx->rv = timestamp_global.load();
    return (tx_t)tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) {
    // TODO: tm_end(shared_t, tx_t)
    return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) {
    // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t tx, void const* source, size_t size, void* target) {
    size_t align = tm_align(shared);
    Tx *tx_ptr = (Tx*)tx;

    for(size_t i=0; i<size/align; i++){
        uintptr_t target_w = (uintptr_t)target + i*align;
        uintptr_t source_w = (uintptr_t)source + i*align;
        
        void *tmp = malloc(align);
        memcpy(tmp, (void*)source_w, align);
        tx_ptr->write_set[target_w] = tmp;
    }

    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void** unused(target)) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    // TODO: tm_free(shared_t, tx_t, void*)
    return false;
}
