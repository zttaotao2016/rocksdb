#pragma once

#include <list>
#include <sys/time.h>
#include <limits>

#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"

namespace rocksdb {

/**
 * Simple logger that prints message on stdout
 */
class ConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::ERROR_LEVEL) {}

  void Logv(const char* format, va_list ap) override {
    MutexLock _(&lock_);
    vprintf(format, ap);
    printf("\n");
  }

  port::Mutex lock_;
};

/**
 * Simple synchronized queue implementation
 */
template<class T>
class BoundedQueue {
 public:
  explicit BoundedQueue(
    const size_t max_size = std::numeric_limits<size_t>::max())
    : cond_empty_(&lock_),
      max_size_(max_size) {}

  virtual ~BoundedQueue() {}

  void Push(T&& t) {
    MutexLock _(&lock_);
    if (max_size_ != std::numeric_limits<size_t>::max()
        && size_ + t.Size() >= max_size_) {
      // overflow
      return;
    }

    size_ += t.Size();
    q_.push_back(std::move(t));
    cond_empty_.SignalAll();
  }

  T Pop() {
    MutexLock _(&lock_);
    while (q_.empty()) {
      cond_empty_.Wait();
    }

    T t = std::move(q_.front());
    size_ -= t.Size();
    q_.pop_front();
    return std::move(t);
  }

  size_t Size() const {
    MutexLock _(&lock_);
    return size_;
  }

 private:
  mutable port::Mutex lock_;
  port::CondVar cond_empty_;
  std::list<T> q_;
  size_t size_ = 0;
  const size_t max_size_;
};

/**
 * Simple timer implementation to calculate latency
 * TODO: Find a replacement
 */
class Timer {
 public:
  uint64_t ElapsedMicroSec() const {
    return NowInMicroSec() - start_;
  }

  uint64_t ElapsedSec() const {
    return (NowInMicroSec() - start_) / 1000000;
  }

 private:
  uint64_t NowInMicroSec() const {
    timeval tv;
    gettimeofday(&tv, /*tz=*/ nullptr);
    return tv.tv_sec * 1000000 + tv.tv_usec;
  }

  const uint64_t start_ = NowInMicroSec();
};



}
