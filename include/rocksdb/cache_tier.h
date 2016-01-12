#pragma once

#include <iostream>
#include <list>

#include "include/rocksdb/env.h"
#include "include/rocksdb/status.h"
#include "include/rocksdb/cache.h"

namespace rocksdb {

/**
 * BlockCache options
 */
struct BlockCacheOptions {
  explicit BlockCacheOptions(Env* const _env, const std::string& _path,
                             const uint64_t _cache_size,
                             const std::shared_ptr<Logger>& _log,
                             const uint32_t _write_buffer_size
                                                  = 1 * 1024 * 1024) {
    env = _env;
    path = _path;
    log = _log;
    cache_size = _cache_size;
    cache_file_size = 100ULL * 1024 * 1024;
    writer_qdepth = 1;
    write_buffer_size = _write_buffer_size;
    write_buffer_count = 200;
    bufferpool_limit = 2ULL * write_buffer_size * write_buffer_count;
  }

  /**
   * Env abstraction to use for systmer level operations
   */
  Env* env;

  /**
   * Path for the block cache where blocks are persisted
   */
  std::string path;

  /**
   * Log handle for logging messages
   */
  std::shared_ptr<Logger> log;

  /**
   * Logical cache size
   */
  uint64_t cache_size = UINT64_MAX;

  /**
   * Cache consists of multiples of small files. This is the size of individual
   * cache file
   *
   * default: 1M
   */
  uint32_t cache_file_size = 100ULL * 1024 * 1024;

  /**
   * The writers can issues IO to the devices in parallel. This parameter
   * controls the qdepth to use for a given block device
   */
  uint32_t writer_qdepth = 2;

  /**
   * Pipeline writes. The write will be delayed and asynchronous. This helps
   * avoid regression in the eviction code path of the primary tier
   */
  bool pipeline_writes_ = true;

  /**
   * Max pipeline buffer size. This is the maximum backlog we can accumulate
   * while waiting for writes.
   *
   * Default: 1GiB
   */
  uint64_t max_write_pipeline_backlog_size = 1ULL * 1024 * 1024 * 1024;

  /**
   * IO size to block device
   */
  uint32_t write_buffer_size = 1 * 1024 * 1024;

  /**
   * Number of buffers to pool
   * (should be greater than cache file size)
   */
  uint32_t write_buffer_count = 200;

  /**
   * Buffer poll limit to which it can grow
   */
  uint64_t bufferpool_limit = 2ULL * cache_file_size;

  BlockCacheOptions MakeBlockCacheOptions(const std::string& path,
                                          const uint64_t size,
                                          const std::shared_ptr<Logger>& lg);
};

/**
 * Represents a logical record on device
 *
 * LBA = { cache-file-id, offset, size }
 */
struct LogicalBlockAddress {
  LogicalBlockAddress() {}
  LogicalBlockAddress(const uint32_t cache_id, const uint32_t off,
                      const uint16_t size)
      : cache_id_(cache_id), off_(off), size_(size) {}

  uint32_t cache_id_ = 0;
  uint32_t off_ = 0;
  uint16_t size_ = 0;
};

typedef LogicalBlockAddress LBA;

/**
 * Abstraction for a general cache tier
 */
class CacheTier {
 public:
  virtual ~CacheTier() {}

  /**
   * Close cache
   */
  virtual Status Close() = 0;

  /**
   * Print stats as string
   */
  virtual std::string PrintStats() { return std::string(); }


  // TEST: Flush data
  virtual void Flush_TEST() = 0;
};

/**
 * Secondary cache tier only supports storage/retrieval of raw data
 */
class SecondaryCacheTier : public CacheTier, public PageCache {
 public:
  typedef LogicalBlockAddress LBA;

  virtual ~SecondaryCacheTier() {}

  /**
   * Create or open an existing cache
   */
  virtual Status Open() = 0;

  /**
   * Remove a given key from the cache
   */
  virtual bool Erase(const Slice& key) = 0;

  /**
   * Expand the cache to accommodate new data
   */
  virtual bool Reserve(const size_t size) = 0;

  // Insert to page cache
  virtual Status Insert(const Slice& page_key, const void* data,
                        const size_t size) = 0;

  // Lookup page cache by page identifier
  virtual Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                        size_t* size) = 0;

  std::shared_ptr<CacheTier> next_tier_;
};

/**
 * Primary cache tier should act also as the cache front end and should comply
 * with Cache interface
 */
class PrimaryCacheTier : public Cache, public CacheTier {
 public:
  virtual ~PrimaryCacheTier() {}

  Status Close() override {
    if (next_tier_) {
      next_tier_->Close();
    }
    return Status::OK();
  }

  // TEST: Flush data
  void Flush_TEST() override {
    if (next_tier_) {
      next_tier_->Flush_TEST();
    }
  }

  std::unique_ptr<SecondaryCacheTier> next_tier_;
};

/**
 * Abstraction that helps you construct a tier of caches and presents as a
 * unified cache.
 */
class TieredCache {
 public:
  explicit TieredCache(std::unique_ptr<PrimaryCacheTier>&& pcache,
                       std::unique_ptr<SecondaryCacheTier>&& scache)
    : pcache_(std::move(pcache))
  {
    pcache_->next_tier_ = std::move(scache);
  }

  virtual ~TieredCache() {}

  std::shared_ptr<PrimaryCacheTier> GetCache() { return pcache_; }

  /**
   * Factory method for creating tiered cache
   */
  static std::unique_ptr<TieredCache> New(const size_t mem_size,
                                          const BlockCacheOptions& options);

 private:
  std::shared_ptr<PrimaryCacheTier> pcache_;
};

}  // namespace rocksdb
