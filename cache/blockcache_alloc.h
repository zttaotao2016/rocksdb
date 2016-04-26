// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <list>
#include <memory>
#include <string>

#include "rocksdb/cache_tier.h"
#include "rocksdb/comparator.h"
#include "rocksdb/env.h"

#include "db/skiplist.h"
#include "port/port_posix.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

namespace rocksdb {

//
// CacheWriteBuffer
//
// Buffer abstraction that can be manipulated via append
// (not thread safe)
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

  void FillTrailingZeros() {
    assert(pos_ <= size_);
    memset(buf_.get() + pos_, '0', size_ - pos_);
    pos_ = size_;
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

//
// CacheWriteBufferAllocator
//
// Buffer pool abstraction(not thread safe)
//
class CacheWriteBufferAllocator {
 public:
  CacheWriteBufferAllocator(const uint32_t buffer_size,
                            const uint32_t buffer_count)
    : buffer_size_(buffer_size) {
    MutexLock _(&lock_);
    buffer_size_ = buffer_size;
    for (uint32_t i = 0; i < buffer_count; i++) {
      auto* buf = new CacheWriteBuffer(buffer_size_);
      assert(buf);
      if (buf) {
        bufs_.push_back(buf);
      }
    }
  }

  virtual ~CacheWriteBufferAllocator() {
    MutexLock _(&lock_);
    assert(bufs_.size() * buffer_size_ == Capacity());
    for (auto* buf : bufs_) {
      delete buf;
    }
    bufs_.clear();
  }

  CacheWriteBuffer* Allocate() {
    MutexLock _(&lock_);
    if (bufs_.empty()) {
      return nullptr;
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
  }

  size_t Capacity() const { return bufs_.size() * buffer_size_; }
  size_t Free() const { return bufs_.size() * buffer_size_; }
  size_t BufferSize() const { return buffer_size_; }

 private:
  port::Mutex lock_;                    // Sync lock
  size_t buffer_size_;                  // Size of each buffer
  std::list<CacheWriteBuffer*> bufs_;   // Buffer stash
};

}  // namespace rocksdb
