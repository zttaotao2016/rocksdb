#include <iostream>
#include <atomic>
#include <unordered_map>
#include <gflags/gflags.h>
#include <functional>
#include <memory>

#include "cache/cache_util.h"
#include "cache/blockcache.h"
#include "cache/cache_volatile.h"
#include "cache/cache_env.h"
#include "include/rocksdb/env.h"
#include "table/block_builder.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"
#include "util/histogram.h"

using namespace rocksdb;
using namespace std;

DEFINE_int32(nsec, 10, "nsec");
DEFINE_int32(nthread_write, 1, "Insert threads");
DEFINE_int32(nthread_read, 1, "Lookup threads");
DEFINE_string(path, "/tmp/microbench/blkcache", "Path for cachefile");
DEFINE_uint64(cache_size, std::numeric_limits<uint64_t>::max(), "Cache size");
DEFINE_int32(iosize, 4 * 1024, "Read IO size");
DEFINE_int32(writer_iosize, 4 * 1024, "File writer IO size");
DEFINE_int32(writer_qdepth, 1, "File writer qdepth");
DEFINE_bool(enable_pipelined_writes, false, "Enable async writes");
DEFINE_string(cache_type, "block_cache",
              "Cache type. (block_cache, volatile, tiered)");
DEFINE_bool(benchmark, false, "Benchmark mode");
DEFINE_int32(volatile_cache_pct, 10, "Percentage of cache in memory tier.");

std::unique_ptr<CacheTier> NewVolatileCache() {
  assert(FLAGS_cache_size != std::numeric_limits<uint64_t>::max());
  std::unique_ptr<CacheTier> pcache(new VolatileCache(FLAGS_cache_size));
  return pcache;
}

#if defined(OS_LINUX)
static std::shared_ptr<Env> shared_env(new DirectIOEnv(Env::Default()));
Env* env = shared_env.get();
#else
Env* env = Env::Default();
#endif

std::unique_ptr<CacheTier> NewBlockCache() {
  Status status;
  auto log = shared_ptr<Logger>(new ConsoleLogger());
  BlockCacheOptions opt(env, FLAGS_path, FLAGS_cache_size, log);
  opt.writer_dispatch_size = FLAGS_writer_iosize;
  opt.writer_qdepth = FLAGS_writer_qdepth;
  opt.pipeline_writes_ = FLAGS_enable_pipelined_writes;
  opt.max_write_pipeline_backlog_size = std::numeric_limits<uint64_t>::max();
  std::unique_ptr<CacheTier> cache(new BlockCacheImpl(opt));
  status = cache->Open();
  return cache;
}

std::unique_ptr<TieredCache> NewTieredCache() {
  auto log = shared_ptr<Logger>(new ConsoleLogger());
  auto pct = FLAGS_volatile_cache_pct / (double) 100;
  BlockCacheOptions opt(env, FLAGS_path, (1 - pct) * FLAGS_cache_size, log);
  opt.writer_dispatch_size = FLAGS_writer_iosize;
  opt.writer_qdepth = FLAGS_writer_qdepth;
  opt.pipeline_writes_ = FLAGS_enable_pipelined_writes;
  opt.max_write_pipeline_backlog_size = std::numeric_limits<uint64_t>::max();
  return std::move(TieredCache::New(FLAGS_cache_size * pct, opt));
}

//
// Benchmark driver
//
class CacheTierBenchmark {
 public:
  CacheTierBenchmark(std::shared_ptr<CacheTier>&& cache)
    : cache_(cache) {
    if (FLAGS_nthread_read) {
      std::cout << "Pre-populating" << std::endl;
      Prepop();
      std::cout << "Pre-population completed" << std::endl;
    }

    stats_.Clear();

    // Start IO threads
    std::list<std::thread> threads;
    Spawn(FLAGS_nthread_write, threads,
          std::bind(&CacheTierBenchmark::Write, this));
    Spawn(FLAGS_nthread_read,  threads,
          std::bind(&CacheTierBenchmark::Read, this));

    // Wait till FLAGS_nsec and then signal to quit
    Timer t;
    while (!quit_) {
      quit_ = t.ElapsedSec() > size_t(FLAGS_nsec);
      sleep(1);
    }
    const size_t sec = t.ElapsedSec();

    // Wait for threads to exit
    Join(threads);
    // Print stats
    PrintStats(sec);
    // Close the cache
    cache_->Flush_TEST();
    cache_->Close();
  }

 private:
  void PrintStats(const size_t sec) {
    std::cout << "Test stats" << std::endl
              << "* Elapsed: " << sec << " s" << std::endl
              << "* Write Latency:" << std::endl
              << stats_.write_latency_.ToString() << std::endl
              << "* Read Latency:" << std::endl
              << stats_.read_latency_.ToString() << std::endl
              << "* Bytes written:" << std::endl
              << stats_.bytes_written_.ToString() << std::endl
              << "* Bytes read:" << std::endl
              << stats_.bytes_read_.ToString() << std::endl;

    std::cout << "Cache stats:" << std::endl
              << cache_->PrintStats() << std::endl;
  }

  //
  // Insert implementation
  //
  void Prepop()
  {
    for (uint64_t i = 0; i < 1024 * 1024; ++i) {
      InsertKey(i);
      insert_key_limit_++;
      read_key_limit_++;
    }

    // Wait until data is flushed
    cache_->Flush_TEST();
    // warmup the cache
    for (uint64_t i = 0; i < 1024 * 1024; ReadKey(i++));
  }

  void Write() {
    while (!quit_) {
      InsertKey(insert_key_limit_++);
    }
  }

  void InsertKey(const uint64_t key) {
    // construct key
    uint64_t k[3];
    Slice block_key = FillKey(k, key);

    // construct value
    auto block = std::move(NewBlock(key));

    // insert
    Timer timer;
    while (true) {
      Status status = cache_->Insert(block_key, block.get(), FLAGS_iosize);
      if (status.ok()) {
        break;
      }

      // transient error is possible if we run without pipelining
      assert(!FLAGS_enable_pipelined_writes);
    }

    // adjust stats
    stats_.write_latency_.Add(timer.ElapsedMicroSec());
    stats_.bytes_written_.Add(FLAGS_iosize);
  }

  //
  // Read implementation
  //
  void Read() {
    while (!quit_) {
      ReadKey(random() % read_key_limit_);
    }
  }

  void ReadKey(const uint64_t val) {
    // construct key
    uint64_t k[3];
    Slice key = FillKey(k, val);

    // Lookup in cache
    Timer timer;
    std::unique_ptr<char[]> block;
    uint64_t size;
    Status status = cache_->Lookup(key, &block, &size);
    if (!status.ok()) {
      std::cout << status.ToString() << std::endl;
    }
    assert(status.ok());
    assert(size == (uint64_t) FLAGS_iosize);

    // adjust stats
    stats_.read_latency_.Add(timer.ElapsedMicroSec());
    stats_.bytes_read_.Add(FLAGS_iosize);

    // verify content
    if (!FLAGS_benchmark) {
      auto expected_block = std::move(NewBlock(val));
      assert(memcmp(block.get(), expected_block.get(), FLAGS_iosize) == 0);
    }
  }

  // create data for a key by filling with a certain pattern
  std::unique_ptr<char[]> NewBlock(const uint64_t val) {
    unique_ptr<char[]> data(new char[FLAGS_iosize]);
    memset(data.get(), val % 255, FLAGS_iosize);
    return std::move(data);
  }

  // spawn threads
  void Spawn(const size_t n, std::list<std::thread>& threads,
             const std::function<void()>& fn) {
    for (size_t i = 0; i < n; ++i) {
      std::thread th(fn);
      threads.push_back(std::move(th));
    }
  }

  // join threads
  void Join(std::list<std::thread>& threads) {
    for (auto& th : threads) {
      th.join();
    }
  }

  // construct key
  Slice FillKey(uint64_t (&k)[3], const uint64_t val) {
    k[0] = k[1] = 0;
    k[2] = val;
    return Slice((char*) &k, sizeof(k));
  }

  // benchmark stats
  struct Stats {
    void Clear() {
      bytes_written_.Clear();
      bytes_read_.Clear();
      read_latency_.Clear();
      write_latency_.Clear();
    }

    HistogramImpl bytes_written_;
    HistogramImpl bytes_read_;
    HistogramImpl read_latency_;
    HistogramImpl write_latency_;
  };

  shared_ptr<CacheTier> cache_;          // cache implementation
  atomic<uint64_t> insert_key_limit_{0}; // data inserted upto
  atomic<uint64_t> read_key_limit_{0};   // data can be read safely upto
  bool quit_ = false;                    // Quit thread ?
  mutable Stats stats_;                  // Stats
};

//
// main
//
int
main(int argc, char** argv) {
  google::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          " [OPTIONS]...");
  google::ParseCommandLineFlags(&argc, &argv, false);

  cout << "Config" << endl
       << "======" << endl
       << "* nsec=" << FLAGS_nsec << endl
       << "* nthread_write=" << FLAGS_nthread_write << endl
       << "* path=" << FLAGS_path << endl
       << "* cache_size=" << FLAGS_cache_size << endl
       << "* iosize=" << FLAGS_iosize << endl
       << "* writer_iosize=" << FLAGS_writer_iosize << endl
       << "* writer_qdepth=" << FLAGS_writer_qdepth << endl
       << "* enable_pipelined_writes=" << FLAGS_enable_pipelined_writes << endl
       << "* cache_type=" << FLAGS_cache_type << endl
       << "* benchmark=" << FLAGS_benchmark << endl
       << "* volatile_cache_pct=" << FLAGS_volatile_cache_pct << endl;

  std::shared_ptr<CacheTier> cache;
  if (FLAGS_cache_type == "block_cache") {
    std::cout << "Using block cache implementation" << std::endl;
    cache = std::move(NewBlockCache());
  } else if (FLAGS_cache_type == "volatile") {
    std::cout << "Using volatile cache implementation" << std::endl;
    cache = std::move(NewVolatileCache());
  } else if (FLAGS_cache_type == "tiered") {
    std::cout << "Using tiered cache implementation" << std::endl;
    cache = std::move(NewTieredCache());
  } else {
    std::cout << "Unknown option for cache" << std::endl;
  }

  std::unique_ptr<CacheTierBenchmark> benchmark(
    new CacheTierBenchmark(std::move(cache)));

  return 0;
}
