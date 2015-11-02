//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once

#include <assert.h>
#include <sys/mman.h>
#include <list>
#include <vector>

#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"

namespace rocksdb {

/**
 * HashTable<T, Hash, Equal>
 *
 * Traditional implementation of hash table with syncronization built on top
 * don't perform very well in multi-core scenarios. This is an implementation
 * designed for multi-core scenarios with high lock contention.
 *
 *                         |<-------- alpha ------------->|
 *               Buckets   Collision list
 *          ---- +----+    +---+---+--- ...... ---+---+---+
 *         /     |    |--->|   |   |              |   |   |
 *        /      +----+    +---+---+--- ...... ---+---+---+
 *       /       |    |
 * Locks/        +----+
 * +--+/         .    .
 * |  |          .    .
 * +--+          .    .
 * |  |          .    .
 * +--+          .    .
 * |  |          .    .
 * +--+          .    .
*      \         +----+
 *      \        |    |
 *       \       +----+
 *        \      |    |
 *         \---- +----+
 *
 * The lock contention is spread over an array of locks. This helps improve
 * concurrent access. The spine is designed for a certain capacity and load
 * factor. When the capacity planning is done correctly we can expect
 * O(load_factor = 1) insert, access and remove time.
 *
 * Micro benchmark on debug build gives about .5 Million/sec rate of insert,
 * erase and lookup in parallel (total of about 1.5 Million ops/sec). If the
 * blocks were of 4K, the hash table can support  a virtual throughput of
 * 6 GB/s.
 *
 * T      Object type (contains both key and value)
 * Hash   Function that returns an hash from type T
 * Equal  Returns if two objects are equal
 *        (We need explicit equal for pointer type)
 */
template<class T, class Hash, class Equal>
class HashTable {
 public:
  explicit HashTable(const size_t capacity = 1024 * 1024,
                     const float load_factor = 2.0,
                     const size_t nlocks = 256) {
    // initialize the spine
    assert(capacity);
    assert(load_factor);
    nbuckets_ = capacity / load_factor;
    buckets_ = new Bucket[nbuckets_];
    assert(buckets_);
#ifdef NDEBUG
    LockMem(buckets_, nbuckets_ * sizeof(Bucket));
#endif

    // initialize locks
    assert(nlocks);
    nlocks_ = nlocks;
    locks_ = new port::RWMutex[nlocks_];
    assert(locks_);
#ifdef NDEBUG
    LockMem(locks_, nlocks_ * sizeof(port::RWMutex));
#endif
  }

  virtual ~HashTable() {
    delete[] buckets_;
    delete[] locks_;
  }

  /**
   * Insert given record to hash table
   */
  bool Insert(const T& t) {
    assert(nbuckets_);
    assert(nlocks_);

    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    WriteLock _(&locks_[lock_idx]);
    auto& bucket = buckets_[bucket_idx];

    // Check if the key already exists
    auto it = Find(bucket.list_, t);
    assert(it == bucket.list_.end());
    if (it != bucket.list_.end()) {
      return false;
    }

    // insert to bucket
    bucket.list_.push_back(t);
    return true;
  }

  /**
   * Lookup hash table
   *
   * Please note that read lock should be held by the caller. This is because
   * the caller owns the data, and should hold the read lock as long as he
   * operates on the data.
   */
  bool Find(const T& t, T* ret) {
    assert(nbuckets_);
    assert(nlocks_);

    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    locks_[lock_idx].AssertHeld();

    auto& bucket = buckets_[bucket_idx];
    auto it = Find(bucket.list_, t);
    if (it == bucket.list_.end()) {
      // not found in the bucket
      return false;;
    }

    if (ret) {
      *ret = *it;
    }
    return true;
  }

  /**
   * Erase a given key from the hash table
   */
  bool Erase(const T& t, T* ret) {
    assert(nbuckets_);
    assert(nlocks_);

    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    WriteLock _(&locks_[lock_idx]);

    auto& bucket = buckets_[bucket_idx];
    auto it = Find(bucket.list_, t);
    if (it == bucket.list_.end()) {
      // not found in the list
      return false;;
    }

    if (ret) {
      *ret = *it;
    }
    bucket.list_.erase(it);
    return true;
  }

  /**
   * Fetch the mutex associated with a key
   * This call is used to hold the lock for a given data for extended period of
   * time.
   */
  port::RWMutex* GetMutex(const T& t) {
    assert(nbuckets_);
    assert(nlocks_);

    const uint64_t h = Hash()(t);
    const uint32_t bucket_idx = h % nbuckets_;
    const uint32_t lock_idx = bucket_idx % nlocks_;

    return &locks_[lock_idx];
  }

 private:
  // Substitute for std::find with custom comparator operator
  typename std::list<T>::const_iterator Find(const std::list<T>& list,
                                             const T& t) {
    for (auto it = list.begin(); it != list.end(); ++it) {
      if (Equal()(*it, t)) {
        return it;
      }
    }
    return list.end();
  }

  // Models a bucket in the spine
  struct Bucket {
    std::list<T> list_;
  };

#ifdef NDEBUG
  void LockMem(void* const data, const size_t size) {
    int status = mlock(data, size);
    if (status) {
      throw std::runtime_error(strerror(errno));
    }
  }
#endif

  uint32_t nbuckets_;                   // No. of buckets in the spine
  Bucket* buckets_;                     // Spine of the hash buckets
  uint32_t nlocks_;                     // No. of locks
  port::RWMutex* locks_;                // Granular locks
};

}  // namespace rocksdb
