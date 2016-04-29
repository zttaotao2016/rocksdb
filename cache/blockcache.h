#pragma once

#include <list>
#include <set>
#include <memory>
#include <string>
#include <stdexcept>
#include <thread>
#include <sstream>

#include "rocksdb/comparator.h"
#include "rocksdb/cache_tier.h"
#include "rocksdb/cache.h"

#include "cache/blockcache_file.h"
#include "cache/blockcache_metadata.h"
#include "cache/blockcache_file_writer.h"
#include "cache/cache_util.h"
#include "port/port_posix.h"
#include "db/skiplist.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include "util/histogram.h"

namespace rocksdb {

//
// Block cache implementation
//
class BlockCacheImpl : public CacheTier {
 public:
  BlockCacheImpl(const BlockCacheOptions& opt)
    : opt_(opt),
      insert_ops_(opt_.max_write_pipeline_backlog_size),
      insert_th_(&BlockCacheImpl::InsertMain, this),
      buffer_allocator_(opt.write_buffer_size, opt.write_buffer_count()),
      writer_(this, opt_.writer_qdepth, opt_.writer_dispatch_size) {
    Info(opt_.log, "Initializing allocator. size=%d B count=%d",
         opt_.write_buffer_size, opt_.write_buffer_count());
  }

  virtual ~BlockCacheImpl() {}

  //
  // Override from PageCache
  //
  Status Insert(const Slice& key, const void* data, const size_t size) override;
  Status Lookup(const Slice & key, std::unique_ptr<char[]>* data,
                size_t* size) override;

  //
  // Override from CacheTier
  //
  Status Open() override;
  Status Close() override;
  bool Erase(const Slice& key) override;
  bool Reserve(const size_t size) override;

  std::string PrintStats() override {
    std::ostringstream os;
    os << "persistentcache.blockcache.total_reads_time_microsec: "
       << stats_.total_cache_reads_time_microsec_ << std::endl
       << "persistentcache.blockcache.total_reads_bytes: "
       << stats_.total_cache_reads_bytes_ << std::endl
       << "persistentcache.blockcache.total_reads: "
       << stats_.total_cache_reads_ << std::endl
       << "persistentcache.blockcache.total_writes_time_microsec: "
       << stats_.total_cache_writes_time_microsec_ << std::endl
       << "persistentcache.blockcache.total_writes_bytes: "
       << stats_.total_cache_writes_bytes_ << std::endl
       << "persistentcache.blockcache.total_writes: "
       << stats_.total_cache_writes_ << std::endl
       << "persistentcache.blockcache.bytes_piplined: "
       << stats_.bytes_pipelined_.ToString() << std::endl
       << "persistentcache.blockcache.bytes_written: "
       << stats_.bytes_written_.ToString() << std::endl
       << "persistentcache.blockcache.bytes_read: "
       << stats_.bytes_read_.ToString() << std::endl
       << "persistentcache.blockcache.cache_hits: "
       << stats_.cache_hits_ << std::endl
       << "persistentcache.blockcache.cache_misses: "
       << stats_.cache_misses_ << std::endl
       << "persistentcache.blockcache.cache_errors: "
       << stats_.cache_errors_ << std::endl
       << "persistentcache.blockcache.cache_hits_pct: "
       << stats_.CacheHitPct() << std::endl
       << "persistentcache.blockcache.cache_misses_pct: "
       << stats_.CacheMissPct() << std::endl
       << "persistentcache.blockcache.read_hit_latency: "
       << stats_.read_hit_latency_.ToString() << std::endl
       << "persistentcache.blockcache.read_miss_latency: "
       << stats_.read_miss_latency_.ToString() << std::endl
       << "persistentcache.blockcache.write_latency: "
       << stats_.write_latency_.ToString() << std::endl
       << CacheTier::PrintStats();
    return os.str();
  }

  void Flush_TEST() override {
    while (insert_ops_.Size()) {
      sleep(1);
    }
  }

 private:
  // Pipelined operation
  struct InsertOp {
    explicit InsertOp(const bool exit_loop)
      : exit_loop_(exit_loop) {}
    explicit InsertOp(std::string&& key, std::unique_ptr<char[]>&& data,
                      const size_t size)
      : key_(std::move(key)), data_(std::move(data)), size_(size) {}
    ~InsertOp() {}

    InsertOp() = delete;
    InsertOp(InsertOp&) = delete;
    InsertOp(InsertOp&& rhs) = default;
    InsertOp& operator=(InsertOp&& rhs) = default;

    size_t Size() const { return size_; }

    std::string key_;
    std::unique_ptr<char[]> data_;
    const size_t size_ = 0;
    const bool exit_loop_ = false;
  };

  // entry point for insert thread
  void InsertMain();
  // insert implementation
  Status InsertImpl(const Slice& key, const std::unique_ptr<char[]>& buf,
                    const size_t size);
  // Create a new cache file
  void NewCacheFile();
  // Get cache directory path
  std::string GetCachePath() const { return opt_.path + "/cache"; }

  // Statistics
  struct Stats {
    HistogramImpl bytes_pipelined_;
    HistogramImpl bytes_written_;
    HistogramImpl bytes_read_;
    HistogramImpl read_hit_latency_;
    HistogramImpl read_miss_latency_;
    HistogramImpl write_latency_;
    uint64_t total_cache_reads_ = 0;
    uint64_t total_cache_writes_ = 0;
    uint64_t total_cache_reads_time_microsec_ = 0;
    uint64_t total_cache_writes_time_microsec_ = 0;
    uint64_t total_cache_reads_bytes_ = 0;
    uint64_t total_cache_writes_bytes_ = 0;
    uint64_t cache_hits_ = 0;
    uint64_t cache_misses_ = 0;
    uint64_t cache_errors_ = 0;

    double CacheHitPct() const {
      const auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_hits_ / (double) lookups : 0.0;
    }

    double CacheMissPct() const {
      const auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_misses_ / (double) lookups : 0.0;
    }
  };

  port::RWMutex lock_;                  // Synchronization
  const BlockCacheOptions opt_;         // BlockCache options
  BoundedQueue<InsertOp> insert_ops_;   // Ops waiting for insert
  std::thread insert_th_;               // Insert thread
  uint32_t writer_cache_id_ = 0;        // Current cache file identifier
  WriteableCacheFile* cache_file_ = nullptr;   // Current cache file reference
  CacheWriteBufferAllocator buffer_allocator_; // Buffer provider
  ThreadedWriter writer_;               // Writer threads
  BlockCacheMetadata metadata_;         // Cache meta data manager
  std::atomic<uint64_t> size_{0};       // Size of the cache
  Stats stats_;                         // Statistics
};

}  // namespace rocksdb
