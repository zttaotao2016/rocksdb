#include <iostream>
#include <atomic>
#include <unordered_map>
#include <gflags/gflags.h>
#include <functional>
#include <memory>

#include "cache/cache_util.h"
#include "cache/cache_tier_testhelper.h"
#include "cache/blockcache.h"
#include "cache/cache_volatile.h"
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
DEFINE_uint64(cache_size, UINT64_MAX, "Cache size");
DEFINE_int32(iosize, 4*1024, "IO size");
DEFINE_bool(enable_pipelined_writes, false, "Enable async writes");
DEFINE_string(cache_type, "block_cache", "Cache type. (blockcache, volatile)");
DEFINE_bool(benchmark, false, "Benchmark mode");
DEFINE_int32(volatile_cache_pct, 10, "Percentage of cache in memory tier.");

std::unique_ptr<PrimaryCacheTier> NewVolatileCache() {
  assert(FLAGS_cache_size != UINT64_MAX);
  std::unique_ptr<PrimaryCacheTier> pcache(new VolatileCache(FLAGS_cache_size));
  return pcache;
}

std::unique_ptr<PrimaryCacheTier> NewBlockCache() {
  Status status;
  Env* env = Env::Default();
  auto log = shared_ptr<Logger>(new ConsoleLogger());
  BlockCacheOptions opt(env, FLAGS_path, FLAGS_cache_size, log);
  opt.pipeline_writes_ = FLAGS_enable_pipelined_writes;
  std::unique_ptr<SecondaryCacheTier> scache(new BlockCacheImpl(opt));
  status = scache->Open();
  assert(status.ok());
  std::unique_ptr<PrimaryCacheTier> pcache(
    new SecondaryCacheTierCloak(std::move(scache)));
  return pcache;
}

std::unique_ptr<TieredCache> NewTieredCache() {
  Env* env = Env::Default();
  auto log = shared_ptr<Logger>(new ConsoleLogger());
  auto pct = FLAGS_volatile_cache_pct / (double) 100;
  BlockCacheOptions opt(env, FLAGS_path, (1 - pct) * FLAGS_cache_size, log);
  return std::move(TieredCache::New(FLAGS_cache_size * pct, opt));
}

//
// Benchmark driver
//
class PrimaryCacheTierBenchmark {
 public:
  PrimaryCacheTierBenchmark(std::shared_ptr<PrimaryCacheTier>&& cache)
    : cache_(cache) {
    Prepop();

    // Start IO threads
    std::list<std::thread> threads;
    Spawn(FLAGS_nthread_write, threads,
          std::bind(&PrimaryCacheTierBenchmark::Write, this));
    Spawn(FLAGS_nthread_read,  threads,
          std::bind(&PrimaryCacheTierBenchmark::Read, this));

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

  void Prepop()
  {
    for (uint64_t i = 0; i < 1024 * 1024; ++i) {
      InsertKey(i);
      insert_key_limit_++;
      read_key_limit_++;
    }

    cache_->Flush_TEST();

    // warmup the cache
    for (uint64_t i = 0; i < 1024 * 1024; ReadKey(i++));

    std::cout << "Pre-population completed" << std::endl;

    stats_.Clear();
  }

  //
  // Insert implementation
  //
  void Write() {
    while (!quit_) {
      InsertKey(insert_key_limit_++);
    }
  }

  void InsertKey(const uint64_t key) {
    uint64_t k[3];
    k[0] = k[1] = 0;
    k[2] = key;
    Slice block_key((char*) &k, sizeof(k));

    auto block = std::move(NewBlock(key));

    Timer timer;
    Cache::Handle* h = cache_->InsertBlock(
      block_key, block.get(), &PrimaryCacheTierBenchmark::DeleteBlock);
    assert(h);
    assert(cache_->Value(h) == block.get());
    stats_.write_latency_.Add(timer.ElapsedMicroSec());

    stats_.bytes_written_.Add(block->size());
    block.release();
    cache_->Release(h);
  }

  std::unique_ptr<Block> NewBlock(const uint64_t val) {
    BlockBuilder bb(/*restarts=*/ 1);
    std::string key(100, val % 255);
    std::string value(FLAGS_iosize, val % 255);
    bb.Add(Slice(key), Slice(value));
    Slice block_template = bb.Finish();

    const size_t block_size = block_template.size();
    unique_ptr<char[]> data(new char[block_size]);
    memcpy(data.get(), block_template.data(), block_size);

    std::unique_ptr<Block> block(Block::NewBlock(std::move(data), block_size));
    assert(block);
    assert(block->size());
    return std::move(block);
  }

  static void DeleteBlock(const Slice&, void* data) {
    delete reinterpret_cast<Block*>(data);
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
      uint64_t k[3];
      k[0] = k[1] = 0;
      k[2] = val;

      Slice key((char*) &k, sizeof(k));

      Timer timer;
      Cache::Handle* h = cache_->Lookup(key);
      if (!h) {
        return;
      }
      stats_.read_latency_.Add(timer.ElapsedMicroSec());

      Block* block = reinterpret_cast<Block*>(cache_->Value(h));
      assert(block);

      if (!FLAGS_benchmark) {
        // verify data
        auto expected_block = std::move(NewBlock(val));
        assert(block->size() == expected_block->size());
        assert(memcmp(block->data(), expected_block->data(), block->size())
                == 0);
      }

      stats_.bytes_read_.Add(block->size());
      cache_->Release(h);
  }

 private:
  void Spawn(const size_t n, std::list<std::thread>& threads,
             const std::function<void()>& fn) {
    for (size_t i = 0; i < n; ++i) {
      std::thread th(fn);
      threads.push_back(std::move(th));
    }
  }

  void Join(std::list<std::thread>& threads) {
    for (auto& th : threads) {
      th.join();
    }
  }

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

  shared_ptr<PrimaryCacheTier> cache_;
  atomic<uint64_t> insert_key_limit_{0};
  atomic<uint64_t> read_key_limit_{0};
  bool quit_ = false;
  mutable Stats stats_;
};

//
// main
//
int
main(int argc, char** argv) {
  google::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          " [OPTIONS]...");
  google::ParseCommandLineFlags(&argc, &argv, false);

  std::unique_ptr<TieredCache> tcache;
  std::shared_ptr<PrimaryCacheTier> cache;
  if (FLAGS_cache_type == "block_cache") {
    std::cout << "Using block cache implementation" << std::endl;
    cache = std::move(NewBlockCache());
  } else if (FLAGS_cache_type == "volatile") {
    std::cout << "Using volatile cache implementation" << std::endl;
    cache = std::move(NewVolatileCache());
  } else if (FLAGS_cache_type == "tiered") {
    std::cout << "Using tiered cache implementation" << std::endl;
    tcache = std::move(NewTieredCache());
    cache = tcache->GetCache();
  } else {
    std::cout << "Unknown option for cache" << std::endl;
  }

  std::unique_ptr<PrimaryCacheTierBenchmark> benchmark(
    new PrimaryCacheTierBenchmark(std::move(cache)));

  return 0;
}
