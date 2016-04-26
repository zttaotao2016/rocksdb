#pragma once

#include <unistd.h>
#include <memory>
#include <thread>

#include "rocksdb/env.h"
#include "cache/cache_util.h"

namespace rocksdb {

/**
 * Representation of IO to device
 */
struct IO {
  explicit IO(const bool signal) : signal_(signal) {}
  explicit IO(WriteableCacheFile* const file, CacheWriteBuffer* const buf,
              const uint64_t file_off)
    : file_(file), buf_(buf),
      file_off_(file_off) {}

  IO(const IO&) = default;
  IO& operator=(const IO&) = default;
  size_t Size() const { return sizeof(IO); }

  WriteableCacheFile* const file_ = nullptr;  // File to write to
  CacheWriteBuffer* const buf_ = nullptr;     // buffer to write
  uint64_t file_off_ = 0;                     // file offset
  bool signal_ = false;                       // signal to processor
};


/**
 * Abstraction to do writing to device. It is part of pipelined architecture.
 */
class ThreadedWriter : public Writer {
 public:
  ThreadedWriter(CacheTier* const cache, const size_t qdepth,
                 const size_t io_size)
      : Writer(cache), io_size_(io_size) {
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

  void Write(WriteableCacheFile* file, CacheWriteBuffer* buf,
             const uint64_t file_off) override {
    q_.Push(IO(file, buf, file_off));
  }

 private:
  /**
   * Thread entry point.
   */
  void ThreadMain() {
    while (true) {
      // Fetch the IO to process
      IO io(q_.Pop());
      if (io.signal_) {
        // that's secret signal to exit
        break;
      }

      // Reserve space for writing the buffer
      while (!cache_->Reserve(io.buf_->Used())) {
        // We can fail to reserve space if every file in the system
        sleep(1);
      }

      DispatchIO(io);

      io.file_->BufferWriteDone(io.buf_);
    }
  }

  void DispatchIO(const IO& io) {
    size_t written = 0;
    while (written < io.buf_->Used()) {
      Slice data(io.buf_->Data() + written, io_size_);
      Status s = io.file_->file_->Append(data);
      assert(s.ok());
      if (!s.ok()) {
        // That is definite IO error to device. There is not much we can
        // do but ignore the failure. This can lead to corruption of data on
        // disk, but the cache will skip while reading
        std::cerr << "Error writing data to file. " << s.ToString()
                  << std::endl;
      }
      written += io_size_;
    }
  }

  const size_t io_size_;
  BoundedQueue<IO> q_;
  std::vector<std::thread> threads_;
};

}  // namespace rocksdb
