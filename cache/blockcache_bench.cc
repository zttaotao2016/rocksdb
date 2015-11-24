#include <iostream>
#include <atomic>
#include <sys/time.h>
#include <unordered_map>
#include <gflags/gflags.h>
#include <functional>

#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"
#include "cache/blockcache.h"

using namespace rocksdb;
using namespace std;

DEFINE_int32(nsec, 10, "nsec");
DEFINE_int32(nthread_write, 1, "Insert threads");
DEFINE_int32(nthread_read, 1, "Lookup threads");
DEFINE_string(path, "/tmp/microbench/blkcache", "Path for cachefile");
DEFINE_uint64(cache_size, UINT64_MAX, "Cache size");
DEFINE_int32(iosize, 4*1024, "IO size");
DEFINE_bool(benchmark, false, "Benchmark mode");

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
    : insert_key_(0),
      read_key_(0),
      bytes_written_(0),
      bytes_read_(0) {
    Status status;

    Env* env = Env::Default();
    auto log = shared_ptr<Logger>(new ConsoleLogger());
    BlockCacheOptions opt(env, FLAGS_path, FLAGS_cache_size, log, FLAGS_iosize);

    cout << "Creating BlockCacheImpl" << endl;

    impl_.reset(new BlockCacheImpl(opt));

    cout << "Opening cache" << endl;

    status = impl_->Open();
    assert(status.ok());

    Prepop();

    cout << "Prepop completed" << endl;

    StartThreads(FLAGS_nthread_write, WriteMain);
    StartThreads(FLAGS_nthread_read, ReadMain);

    const uint64_t start = NowInMillSec();
    while (!quit_) {
      quit_ = NowInMillSec() - start > size_t(FLAGS_nsec * 1000);
      sleep(1);
    }

    impl_->Close();

    env->WaitForJoin();

    sec_ = (NowInMillSec() - start) / 1000;


    if (sec_) {
      cout << "sec: " << sec_ << endl;
      cout << "written " << bytes_written_ << " B" << endl;
      cout << "Read " << bytes_read_ << " B" << endl;
      cout << "W MB/s: " << (bytes_written_ / sec_) / (1024 * 1024) << endl;
      cout << "R MB/s: " << (bytes_read_ / sec_) / (1024 * 1024) << endl;
    }
  }

  void Prepop()
  {
    for (uint64_t i = 0; i < 1024 * 1024; ++i) {
      InsertKey(i);
      insert_key_++;
      read_key_++;
    }
  }

  void RunWrite() {
    while (!quit_) {
      InsertKey(insert_key_++);
      bytes_written_ += FLAGS_iosize;
    }
  }

  void InsertKey(const uint64_t val) {
    uint64_t k[3];
    k[0] = k[1] = 0;
    k[2] = val;

    char data[FLAGS_iosize];
    if (!FLAGS_benchmark) {
      memset(data, val % 255, FLAGS_iosize);
    }
    Slice key((char*) &k, sizeof(k));

    while (!quit_) {
      Status status = impl_->Insert(key, data, FLAGS_iosize);
      if (status == Status::TryAgain()) {
        continue;
      }
      assert(status.ok());
      break;
    }
  }

  void RunRead() {
    while (!quit_) {
      uint64_t k[3];
      k[0] = k[1] = 0;
      k[2] = random() % read_key_;

      Slice key((char*) &k, sizeof(k));
      LBA lba;

      unique_ptr<char[]> val;
      size_t size;
      bool ok = impl_->Lookup(key, &val, &size);
      assert(ok);
      assert(size == (size_t) FLAGS_iosize);

      // Verify data
      if (!FLAGS_benchmark) {
        char expected[FLAGS_iosize];
        memset(expected, k[2] % 255, FLAGS_iosize);
        assert(memcmp(val.get(), expected, size) == 0);
      }

      bytes_read_ += size;
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
  atomic<uint64_t> insert_key_;
  atomic<uint64_t> read_key_;
  bool quit_ = false;
  atomic<uint64_t> bytes_written_;
  atomic<uint64_t> bytes_read_;
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

  cout << "benchmark : " << FLAGS_benchmark << endl;

  unique_ptr<MicroBenchmark> _(new MicroBenchmark());

  return 0;
}
