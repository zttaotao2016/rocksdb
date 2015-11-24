// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <list>
#include <memory>
#include <string>

#include "cache/cache_tier.h"
#include "db/skiplist.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

namespace rocksdb {

/**
 * CacheWriteBuffer
 *
 * Buffer abstraction that can be manipulated via append
 * (not thread safe)
 */
class CacheWriteBuffer {
 public:
  explicit CacheWriteBuffer(const size_t size)
    : size_(size),
      pos_(0) {
    buf_.reset(new char[size_]);

    assert(!pos_);
    assert(size_);
  }

  virtual ~CacheWriteBuffer() {}

  size_t Append(const char* buf, const size_t size) {
    assert(pos_ + size <= size_);

    const size_t ret = pos_;
    memcpy(buf_.get() + pos_, buf, size);
    pos_ += size;
    assert(pos_ <= size_);
    return ret;
  }

  void Reset() { pos_ = 0; }
  size_t Free() const { return size_ - pos_; }
  size_t Capacity() const { return size_; }
  size_t Used() const { return pos_; }
  char* Data() const { return buf_.get(); }

 private:
  std::unique_ptr<char[]> buf_;
  const size_t size_;
  size_t pos_;
};

/**
 * CacheWriteBufferAllocator
 *
 * Buffer pool abstraction
 * (not thread safe)
 */
class CacheWriteBufferAllocator {
 public:
  CacheWriteBufferAllocator()
    : auto_expand_(false),
      buffer_count_(0) {
  }

  virtual ~CacheWriteBufferAllocator() {
    MutexLock _(&lock_);
    assert(bufs_.size() * buffer_size_ == Capacity());
    for (auto* buf : bufs_) {
      delete buf;
    }
    bufs_.clear();
  }

  void Init(const uint32_t bufferSize, const uint32_t bufferCount,
            const uint64_t max_size) {
    assert(max_size >= bufferSize * bufferCount);

    MutexLock _(&lock_);

    target_size_ = max_size;
    buffer_size_ = bufferSize;

    for (uint32_t i = 0; i < buffer_count_; i++) {
      auto* buf = new CacheWriteBuffer(buffer_size_);
      assert(buf);
      if (buf) {
        bufs_.push_back(buf);
      }
    }

    buffer_count_ = bufs_.size();
  }

  CacheWriteBuffer* Allocate() {
    MutexLock _(&lock_);

    if (bufs_.empty()) {
      if (!ExpandBuffer()) {
        return nullptr;
      }
    }

    assert(!bufs_.empty());

    CacheWriteBuffer* const buf = bufs_.front();
    bufs_.pop_front();

    return buf;
  }

  void Deallocate(CacheWriteBuffer* const buf) {
    assert(buf);

    MutexLock _(&lock_);

    buf->Reset();

    bufs_.push_back(buf);

    AdjustCapacity();
  }

  size_t Capacity() const { return buffer_count_ * buffer_size_; }
  size_t Free() const { return bufs_.size() * buffer_size_; }
  size_t buffersize() const { return buffer_size_; }

 private:
  bool ExpandBuffer() {
    lock_.AssertHeld();

    if (!auto_expand_ && Capacity() >= target_size_) {
      return false;
    }

    assert(auto_expand_ || Capacity() < target_size_);

    auto* const buf = new CacheWriteBuffer(buffer_size_);
    if (!buf) {
      return false;
    }

    buf->Reset();
    bufs_.push_back(buf);

    buffer_count_++;

    return true;
  }

  void AdjustCapacity() {
    lock_.AssertHeld();

    while (Free() > target_size_) {
      auto* buf = bufs_.front();
      bufs_.pop_front();
      delete buf;
      --buffer_count_;
    }

    assert(Free() <= Capacity());
    assert(Free() <= target_size_);
  }

  port::Mutex lock_;                    // Sync lock
  const bool auto_expand_;              // Should act like buffer pool ?
  size_t buffer_size_;                  // Size of each buffer
  size_t buffer_count_;                 // Buffer count
  size_t target_size_;                  // Ideal operational size
  std::list<CacheWriteBuffer*> bufs_;   // Buffer stash
};

}  // namespace rocksdb
