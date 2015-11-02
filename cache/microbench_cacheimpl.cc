#include <iostream>
#include <atomic>
#include <sys/time.h>
#include <unordered_map>
#include <gflags/gflags.h>
#include <functional>

#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"
#include "cache/blkcache.h"

using namespace rocksdb;
using namespace std;

DEFINE_int32(nsec, 10, "nsec");
DEFINE_int32(nthread_write, 1, "Insert threads");
DEFINE_int32(nthread_read, 0, "Lookup threads");
DEFINE_string(path, "/tmp/microbench/blkcache", "Path for cachefile");
DEFINE_int32(buffer_size, 1024 * 1024, "Write buffer size");
DEFINE_int32(nbuffers, 100, "Buffer count");
DEFINE_int32(cache_file_size, 100, "Cache file size");
DEFINE_int32(qdepth, 2, "qdepth");
DEFINE_int32(iosize, 4 * 1024 * 1024, "IO size");

uint64_t NowInMillSec() {
  timeval tv;
  gettimeofday(&tv, /*tz=*/ nullptr);
  return tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

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

//
// Benchmark driver
//
class MicroBenchmark {
 public:

  MicroBenchmark()
    : bytes_(0) {
    Status status;

    Env* env = Env::Default();
    BlockCacheImpl::Options opt;
    opt.path = FLAGS_path;
    opt.info_log = shared_ptr<Logger>(new ConsoleLogger());
    opt.writeBufferSize = FLAGS_buffer_size;
    opt.writeBufferCount = FLAGS_nbuffers;
    opt.max_bufferpool_size_ = 2 * FLAGS_buffer_size * FLAGS_nbuffers;
    opt.maxCacheFileSize = FLAGS_cache_file_size;
    opt.writer_qdepth_ = FLAGS_qdepth;

    cout << "Creating BlockCacheImpl" << endl;

    impl_.reset(new BlockCacheImpl(env, opt));

    cout << "Opening cache" << endl;

    status = impl_->Open();
    assert(status.ok());

    StartThreads(FLAGS_nthread_write, WriteMain);
    StartThreads(FLAGS_nthread_read, ReadMain);

    const uint64_t start = NowInMillSec();
    while (!quit_) {
      quit_ = NowInMillSec() - start > size_t(FLAGS_nsec * 1000);
      sleep(1);
    }

    impl_->Close();

    sec_ = (NowInMillSec() - start) / 1000;

    env->WaitForJoin();

    if (sec_) {
      cout << "sec: " << sec_ << endl;
      cout << "written: " << bytes_ << " B" << endl;
      cout << "W MB/s: " << (bytes_ / sec_) / (1024 * 1024) << endl;
    }
  }

  void RunWrite() {
    while (!quit_) {
      uint64_t k[3];
      k[0] = k[1] = 0;
      k[2] = random();

      char data[FLAGS_iosize];
      Slice key((char*) &k, sizeof(k));
      LBA lba;
      while (true) {
        Status status = impl_->Insert(key, data, FLAGS_iosize, &lba);
        if (status == Status::TryAgain()) {
          continue;
        }
        assert(status.ok());
        break;
      }

      bytes_ += FLAGS_iosize;
    }
  }

  void RunRead() {
    while (!quit_) {
    }
  }

 private:

  void StartThreads(const size_t n, void (*fn)(void*)) {
    Env* env = Env::Default();
    for (size_t i = 0; i < n; ++i) {
      env->StartThread(fn, this);
    }
  }

  static void WriteMain(void* args) {
    ((MicroBenchmark*) args)->RunWrite();
  }

  static void ReadMain(void* args) {
    ((MicroBenchmark*) args)->RunRead();
  }

  unique_ptr<BlockCacheImpl> impl_;
  bool quit_ = false;
  atomic<uint64_t> bytes_;
  uint64_t sec_ = 0;
};

//
// main
//
int
main(int argc, char** argv) {
  google::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          " [OPTIONS]...");
  google::ParseCommandLineFlags(&argc, &argv, false);

  unique_ptr<MicroBenchmark> _(new MicroBenchmark());

  return 0;
}
