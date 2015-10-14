#pragma once

#include <memory>
#include "include/rocksdb/env.h"
#include "blkcache/blkcache_cachefile.h"

namespace rocksdb {

/**
 *
 *
 */
class IOQueue {
 public:

  struct IO {
    IO(WriteableCacheFile* const file, WriteBuffer* const buf)
        : file_(file), buf_(buf)
    {}

    WriteableCacheFile* file_;
    WriteBuffer* buf_;
  };

  virtual ~IOQueue() {}

  virtual void Push(IO* io) = 0;
  virtual IO* Pop() = 0;
};

/**
 *
 *
 */
class BlockingIOQueue : public IOQueue {
 public:
  BlockingIOQueue() : cond_empty_(&lock_) {}

  void Push(IO* io) override {
    MutexLock _(&lock_);
    q_.push_back(io);
    cond_empty_.SignalAll();
  }

  IO* Pop() override {
    MutexLock _(&lock_);

    while (q_.empty()) {
      cond_empty_.Wait();
    }

    assert(!q_.empty());
    lock_.AssertHeld();

    IO* io = q_.front();
    q_.pop_front();

    return io;
  }

 private:
  port::Mutex lock_;
  port::CondVar cond_empty_;
  std::list<IOQueue::IO*> q_;
};

/**
 *
 *
 */
class ThreadedWriter : public Writer {
 public:

  ThreadedWriter(Env* const env, const size_t qdepth = 1)
      : env_(env),
        qdepth_(qdepth),
        q_(new BlockingIOQueue()) {
    for (size_t i = 0; i < qdepth_; ++i) {
      env_->StartThread(&ThreadedWriter::IOMain, this);
    }
  }

  virtual ~ThreadedWriter()
  {
  }

  void Stop() override
  {
    for (size_t i = 0; i < qdepth_; ++i) {
      q_->Push(new IOQueue::IO(nullptr, nullptr));
    }

    env_->WaitForJoin();
  }

  void Write(WriteableCacheFile* file, WriteBuffer* buf) override {
    q_->Push(new IOQueue::IO(file, buf));
  }

 private:

  static void IOMain(void* arg) {
    auto* self = (ThreadedWriter*) arg;

    while (true) {
      unique_ptr<IOQueue::IO> io(self->q_->Pop());

      if (!io->file_) {
        // that's secret signal to exit
        break;
      }

      WriteableCacheFile* const f = io->file_;
      WriteBuffer* const buf = io->buf_;

      Status s = f->file_->Append(Slice(buf->Data(), buf->Used()));
      if (!s.ok()) {
        // print error
        continue;
      }

      assert(s.ok());
      f->BufferWriteDone(buf);
    }
  }

  Env* env_;
  const size_t qdepth_;
  unique_ptr<IOQueue> q_;
};

}  // namespace rocksdb
