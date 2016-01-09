#pragma once

#include <unistd.h>
#include <memory>
#include <thread>

#include "cache/cache_util.h"
#include "include/rocksdb/env.h"

namespace rocksdb {

/**
 * Representation of IO to device
 */
struct IO {
  explicit IO(const bool signal) : signal_(signal) {}
  explicit IO(WriteableCacheFile* const file, CacheWriteBuffer* const buf)
    : file_(file), buf_(buf) {}

  IO(const IO&) = default;
  IO& operator=(const IO&) = default;
  size_t Size() const { return sizeof(IO); }

  WriteableCacheFile* const file_ = nullptr;  // File to write to
  CacheWriteBuffer* const buf_ = nullptr;     // buffer to write
  bool signal_ = false;                       // signal to processor
};


/**
 * Abstraction to do writing to device. It is part of pipelined architecture.
 */
class ThreadedWriter : public Writer {
 public:
  ThreadedWriter(CacheTier* const cache, const size_t qdepth = 1)
      : Writer(cache) {
    for (size_t i = 0; i < qdepth; ++i) {
      std::thread th(&ThreadedWriter::ThreadMain, this);
      threads_.push_back(std::move(th));
    }
  }

  virtual ~ThreadedWriter() {}

  void Stop() override {
    // notify all threads to exit
    for (size_t i = 0; i < threads_.size(); ++i) {
      q_.Push(IO(/*signal=*/ true));
    }

    // wait for all threads to exit
    for (auto& th : threads_) {
      th.join();
    }
  }

  void Write(WriteableCacheFile* file, CacheWriteBuffer* buf) override {
    q_.Push(IO(file, buf));
  }

 private:
  /**
   * Thread entry point.
   */
  void ThreadMain() {
    while (true) {
      IO io(q_.Pop());

      if (io.signal_) {
        // that's secret signal to exit
        break;
      }

      while(!cache_->Reserve(io.buf_->Used())) {
        // Why would we fail to reserve space ? IO error on disk is the only
        // reasonable explanation
        sleep(1);
      }

      Slice data(io.buf_->Data(), io.buf_->Used());
      Status s = io.file_->file_->Append(data);
      if (!s.ok()) {
        // That is definite IO error to device. There is not much we can
        // do but ignore the failure. This can lead to corruption of data (!)
        continue;
      }
      assert(s.ok());
      io.file_->BufferWriteDone(io.buf_);
    }
  }

  BoundedQueue<IO> q_;
  std::vector<std::thread> threads_;
};

}  // namespace rocksdb
