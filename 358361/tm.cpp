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

#include "macros.h"
#include <atomic>
#include <chrono>
#include <iostream>
#include <unordered_map>
#include <memory>
#include <thread>
#include <shared_mutex>
#include <string.h>
#include <tm.hpp>
#include <unordered_set>
#include <vector>

#define NUM_SEGMENTS 512
#define NUM_WORDS 2048

// auxiliar structure used in the TimestampLock class
struct Lock {
    bool is_locked;
    uint64_t stamp, lock;
};

// Global timestamp
static std::atomic_uint timestamp_global(0);

class TimestampLock {
private:
// serialized value
  std::atomic_uint64_t lock_value;

public:
  TimestampLock() : lock_value(0) {}
  TimestampLock(const TimestampLock &timestamp) { 
    lock_value = timestamp.lock_value.load(); 
  }

  Lock get_value(){
    // get serialized value, first 63 bits are stamp, last bit the lock
    uint64_t val = this->lock_value.load();
    uint64_t stamp, lock;
    stamp = (((uint64_t)1 << 63) - 1) & val;
    lock = val >> 63;
    
    // translate into a Lock data structure and return it
    Lock result;
    if(lock == 1) result.is_locked = true;
    else result.is_locked = false;
    result.stamp = stamp;
    result.lock = val;
    return result;
  }
  
  bool lock_CAS(bool is_locked, uint64_t stamp, uint64_t old){
    if((stamp >> 63) == 1) throw -1; // Technically we may have so many versions that we overflow the 63 bits
    uint64_t new_value = stamp;
    // is it is locked then set the new value as version + locked bit = 1 else  locked bit = 0
    if(is_locked) new_value = ((uint64_t)1 << 63) | stamp;
    // compare and swap
    return this->lock_value.compare_exchange_strong(old,new_value);
  }

  bool get_lock(){
    // get value if unlocked by locking it and CandS
    Lock lock = this->get_value();
    if(lock.is_locked) return false;
    return lock_CAS(true,lock.stamp,lock.lock);
  }

  bool release_lock(bool set_a_new_timestamp, uint64_t new_value){
    // free lock and optionally change timestamp
    Lock lock = this->get_value();
    if(!lock.is_locked) return false;
    if(set_a_new_timestamp) return this->lock_CAS(false, new_value, lock.lock);
    else return this->lock_CAS(false,lock.stamp,lock.lock);
  }
};

// Transaction implementation
struct Tx{
    bool is_ro;
    // RV, WV
    uint64_t read, write;
    std::unordered_set<void*> read_set;
    std::unordered_map<uintptr_t, void*> write_set;
};

// Local transaction structure
static thread_local Tx local_tx;

struct WordLock{
    WordLock() : lock(), word_id(0) {}
    TimestampLock lock;
    uint64_t word_id;
};

// MemoryRegion implementation
struct MemoryRegion{
    MemoryRegion(size_t size, size_t align) : size(size), allocated_segments(2), align(align), segments(NUM_SEGMENTS, std::vector<WordLock>(NUM_WORDS)) {}
    size_t size, align;
    std::atomic_uint64_t allocated_segments;
    // contains all the segments, each segment made up of words
    std::vector<std::vector<WordLock>> segments;
};


/** [thread-safe] Translate a target address to a segment index and a word index.
 * @param shared Shared memory region associated with the transaction
 * @param target Target address to translate
 * @return Pair of segment index and word index
**/
std::pair<uint64_t,uint64_t> translate_address(size_t align, uintptr_t target){
    std::pair<uint64_t,uint64_t> result(target >> 32, ((target << 32)>>32) / align);
    return result;
}


/** Initialize a new transaction
 * 
 */
void init_transaction(){
    // clear sets, free words in write set, set timestamps to 0 for reads, set readonly to false
    local_tx.read_set.clear();
    // free write set words
    for(auto &ptr : local_tx.write_set){
        free(ptr.second);
    }
    local_tx.write_set.clear();
    local_tx.read = 0;
    local_tx.is_ro = false;
}

/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t size, size_t align) noexcept {
    MemoryRegion* shared = new struct MemoryRegion(size,align);
    if(unlikely(!shared)) return invalid_shared;
    return shared;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t shared) noexcept {
    delete ((struct MemoryRegion*) shared);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void *tm_start(shared_t unused(shared)) noexcept {
  return (void *)((uint64_t)1 << 32);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t shared) noexcept {
  return ((struct MemoryRegion*)shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t shared) noexcept {
  return ((struct MemoryRegion*)shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t unused(shared), bool is_ro) noexcept {
    // get new value of read stamp, set readonly
    local_tx.read = timestamp_global.load();
    local_tx.is_ro = is_ro;
    return (uintptr_t)&local_tx;   
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t shared, tx_t unused(tx), void const *source, size_t size,
              void *target) noexcept {

    size_t align = tm_align(shared);

    for(size_t i=0; i<size/align; i++){
        uintptr_t target_w = (uintptr_t)target + i*align;
        uintptr_t source_w = (uintptr_t)source + i*align;
        
        void *tmp = malloc(align);
        memcpy(tmp, (void*)source_w, align);
        local_tx.write_set[target_w] = tmp;
    }

    return true;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t shared, tx_t unused(tx), void const *source, size_t size,
             void *target) noexcept {
    size_t align = tm_align(shared);

    for(int i=0; i<size/align; i++){
        uintptr_t target_w = (uintptr_t)target + i*align;
        uintptr_t source_w = (uintptr_t)source + i*align;
        std::pair<uint64_t,uint64_t> address = translate_address(align, source_w);

        //If the transaction is not read only, check if the word is in the write set and if yes update value. Also add to read set
        if(!local_tx.is_ro){
            auto w = local_tx.write_set.find(source_w);
            if(w != local_tx.write_set.end()){
                memcpy((void*)target_w, w->second, align);
                continue;
            }
        }
        
        WordLock *lock = &(((MemoryRegion*)shared)->segments[address.first][address.second]);
        Lock old_value = lock->lock.get_value();
        memcpy((void*)target_w, &lock->word_id, align);
        Lock new_value = lock->lock.get_value();

        if(local_tx.is_ro){
            if(new_value.is_locked || new_value.stamp > local_tx.read){
                init_transaction();
                return false;
            }
        } else {
            if(new_value.is_locked || old_value.stamp != new_value.stamp || new_value.stamp > local_tx.read){
                init_transaction();
                return false;
            }

            local_tx.read_set.emplace((void*)source_w);
        }
    }
    return true;
}

/** Release the locks on the write set
 * 
 * @param tx_ptr Transaction pointer
 * @param region Memory region
 * @param end End of the write set
 */
void release_locks(MemoryRegion *region, std::pair<const uintptr_t, void *> &end){
    for(auto & elem : local_tx.write_set){
        if(&elem == &end) break;
        std::pair<uint64_t,uint64_t> address = translate_address(region->align, elem.first);
        WordLock *word_lock = &((region)->segments[address.first][address.second]);
        word_lock->lock.release_lock(0, false);
    }
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t shared, tx_t unused(tx)) noexcept{
    // printf("Committing transaction %ld\n",local_tx.write);

    MemoryRegion* region = (MemoryRegion*)shared;
    std::pair<const uintptr_t, void *> null_pair = {0, nullptr};

    // if tx is readonly or nothing was written anyway just init new transaction and commit
    if(local_tx.is_ro || local_tx.write_set.empty()){
        init_transaction();
        // printf("Committed RO transaction\n");
        return true;
    }

    //Acquire locks on the write_set
    for(auto & elem : local_tx.write_set){
        std::pair<uint64_t,uint64_t> address = translate_address(region->align, elem.first);
        WordLock *word_lock = &((region)->segments[address.first][address.second]);
        int i=0;
        bool got_lock = word_lock->lock.get_lock();
        //Bounded spinlock
        while(!got_lock){
            if(i++ > 100000){
                release_locks(region, elem);
                init_transaction();
                // printf("Aborted transaction: failed to acquire lock on write-set\n");
                return false;
            }
            got_lock = word_lock->lock.get_lock();
        }
    }

    local_tx.write = timestamp_global.fetch_add(1) + 1;

    //Validate read set
    if(local_tx.read != local_tx.write - 1){
        for(auto word : local_tx.read_set){
            std::pair<uint64_t,uint64_t> address = translate_address(region->align, (uintptr_t)word);
            WordLock *word_lock = &((region)->segments[address.first][address.second]);
            Lock lock = word_lock->lock.get_value();
            if(lock.is_locked || lock.stamp > local_tx.read){
                release_locks(region, null_pair);
                init_transaction();
                // printf("Aborted transaction: read-set validation failed\n");

                return false;
            }
        }
    }

    //Commit and release locks
    for(auto pair : local_tx.write_set){
        std::pair<uint64_t,uint64_t> adds = translate_address(region->align, (uintptr_t)pair.first);
        WordLock &lock = region->segments[adds.first][adds.second];
        memcpy(&lock.word_id,pair.second,region->align);
        lock.lock.release_lock(true,local_tx.write);
    }

    // printf("Committed RW transaction - # write-set: %ld\n", local_tx.write_set.size());

    init_transaction();

    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
Alloc tm_alloc(shared_t shared, tx_t unused(tx), size_t unused(size),
               void **target) noexcept {
  *target = (void*)(((MemoryRegion*)shared)->allocated_segments.fetch_add(1) << 32);
  return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx),
             void *unused(segment)) noexcept {
  return true;
}