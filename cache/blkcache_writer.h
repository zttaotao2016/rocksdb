#pragma once

#include <unistd.h>
#include <memory>
#include "include/rocksdb/env.h"

namespace rocksdb {

/**
 *
 *
 */
class IOQueue {
 public:

  struct IO {
    IO(WriteableCacheFile* const file, CacheWriteBuffer* const buf)
        : file_(file), buf_(buf)
    {}

    WriteableCacheFile* file_;
    CacheWriteBuffer* buf_;
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

  virtual ~BlockingIOQueue() {
    for (auto* io : q_) {
      delete io;
    }
    q_.clear();
  }

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

  ThreadedWriter(PersistentBlockCache* const cache, Env* const env,
                 const size_t qdepth = 1)
      : Writer(cache),
        env_(env),
        qdepth_(qdepth),
        q_(new BlockingIOQueue()),
        cond_exit_(&th_lock_),
        nthreads_(0) {
    MutexLock _(&th_lock_);
    for (size_t i = 0; i < qdepth_; ++i) {
      ++nthreads_;
      env_->StartThread(&ThreadedWriter::IOMain, this);
    }
  }

  virtual ~ThreadedWriter() {
  }

  void Stop() override {
    MutexLock _(&th_lock_);
    for (size_t i = 0; i < qdepth_; ++i) {
      q_->Push(new IOQueue::IO(nullptr, nullptr));
    }

    while (nthreads_) {
      cond_exit_.Wait();
    }
  }

  void Write(WriteableCacheFile* file, CacheWriteBuffer* buf) override {
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
      CacheWriteBuffer* const buf = io->buf_;

      while(!self->cache_->Reserve(buf->Used())) {
        sleep(1);
      }

      Status s = f->file_->Append(Slice(buf->Data(), buf->Used()));
      if (!s.ok()) {
        // print error
        continue;
      }

      assert(s.ok());
      f->BufferWriteDone(buf);
    }

    MutexLock _(&self->th_lock_);
    --self->nthreads_;
    self->cond_exit_.Signal();
  }

  Env* env_;
  const size_t qdepth_;
  unique_ptr<IOQueue> q_;
  port::Mutex th_lock_;
  port::CondVar cond_exit_;
  size_t nthreads_;
};

}  // namespace rocksdb
