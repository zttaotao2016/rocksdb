#pragma once

#include "blkcache/persistent_blkcache.h"
#include "db/skiplist.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include <list>
#include <memory>
#include <string>

namespace rocksdb {

/**
 *
 */
class WriteBuffer {
 public:

  WriteBuffer(const size_t size)
    : size_(size) {
    buf_.reset(new char[size_]);

    Reset();

    assert(!pos_);
    assert(size_);
  }

  virtual ~WriteBuffer() {}

  int Append(const char* buf, const size_t size) {
    const size_t ret = pos_;

    assert(pos_ + size <= size_);

    memcpy(buf_.get() + pos_, buf, size);
    pos_ += size;

    assert(pos_ <= size_);

    return ret;
  }

  void Reset() {
    memset(buf_.get(), 'x', size_);
    pos_ = 0;
  }

  size_t Free() const { return size_ - pos_; }
  size_t Capacity() const { return size_; }
  size_t Used() const { return pos_; }
  char* Data() const { return buf_.get(); }

 private:

  std::unique_ptr<char> buf_;
  const size_t size_;
  size_t pos_;
};

/**
 *
 */
class WriteBufferAllocator {
 public:

  WriteBufferAllocator() : bufferCount_(0) {}

  void Init(const size_t bufferSize, const size_t bufferCount,
            const size_t max_size) {
    assert(max_size >= bufferSize * bufferCount);

    MutexLock _(&lock_);

    max_size_ = max_size;
    bufferSize_ = bufferSize;
    bufferCount_ = bufferCount;

    for (size_t i = 0; i < bufferCount_; i++) {
      auto* buf = new WriteBuffer(bufferSize_);
      if (buf) {
        bufs_.push_back(buf);
      }
    }
  }

  virtual ~WriteBufferAllocator() {
    MutexLock _(&lock_);
    assert(bufs_.size() * bufferSize_ == Capacity());
  }

  WriteBuffer* Allocate() {
    MutexLock _(&lock_);

    if (bufs_.empty()) {
      if (!ExpandBuffer()) {
        return nullptr;
      }
    }

    assert(!bufs_.empty());

    WriteBuffer* const buf = bufs_.front();
    bufs_.pop_front();

    return buf;
  }

  void Deallocate(WriteBuffer* const buf) {
    assert(buf);

    MutexLock _(&lock_);

    buf->Reset();

    bufs_.push_back(buf);
  }

  size_t Capacity() const { return bufferCount_ * bufferSize_; }
  size_t Free() const { return bufs_.size() * bufferSize_; }
  size_t buffersize() const { return bufferSize_; }

 private:
  bool ExpandBuffer() {
    lock_.AssertHeld();

    if (Capacity() >= max_size_) {
      return false;
    }

    assert(Capacity() <= max_size_);

    auto* const buf = new WriteBuffer(bufferSize_);
    if (!buf) {
      return false;
    }

    buf->Reset();
    bufs_.push_back(buf);

    bufferCount_++;

    return true;
  }

  port::Mutex lock_;
  size_t bufferSize_;
  size_t bufferCount_;
  size_t max_size_;
  std::list<WriteBuffer*> bufs_;
};
}
