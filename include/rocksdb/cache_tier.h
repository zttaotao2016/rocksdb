#pragma once

#include <iostream>
#include <list>
#include <limits>

#include "include/rocksdb/env.h"
#include "include/rocksdb/status.h"
#include "include/rocksdb/page_cache.h"

namespace rocksdb {

//
// BlockCache options
//
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
    writer_dispatch_size = write_buffer_size = _write_buffer_size;
  }

  //
  // Validate the settings
  //
  Status ValidateSettings() const {
    // check for null and empty
    if (!env || path.empty()) {
      return Status::InvalidArgument("empty or null args");
    }

    // checks related to file size
    if (cache_size < cache_file_size
        || write_buffer_size >= cache_file_size
        || write_buffer_size * write_buffer_count() < 2 * cache_file_size) {
      return Status::InvalidArgument("invalid cache size");
    }

    // check writer settings
    if (!writer_qdepth || writer_dispatch_size > write_buffer_size
        || write_buffer_size % writer_dispatch_size) {
      return Status::InvalidArgument("invalid writer settings");
    }

    return Status::OK();
  }

  //
  // Env abstraction to use for systmer level operations
  //
  Env* env;

  //
  // Path for the block cache where blocks are persisted
  //
  std::string path;

  //
  // Log handle for logging messages
  //
  std::shared_ptr<Logger> log;

  //
  // Logical cache size
  //
  uint64_t cache_size = std::numeric_limits<uint64_t>::max();

  // Cache consists of multiples of small files. This is the size of individual
  // cache file
  // default: 1M
  uint32_t cache_file_size = 100ULL * 1024 * 1024;

  // The writers can issues IO to the devices in parallel. This parameter
  // controls the qdepth to use for a given block device
  uint32_t writer_qdepth = 2;

  // Pipeline writes. The write will be delayed and asynchronous. This helps
  // avoid regression in the eviction code path of the primary tier
  bool pipeline_writes_ = true;

   // Max pipeline buffer size. This is the maximum backlog we can accumulate
   // while waiting for writes.
   //
   // Default: 1GiB
  uint64_t max_write_pipeline_backlog_size = 1ULL * 1024 * 1024 * 1024;

  //
  // IO size to block device
  //
  uint32_t write_buffer_size = 1ULL * 1024 * 1024;


  //
  // Write buffer count
  //
  size_t write_buffer_count() const {
    assert(write_buffer_size);
    return (writer_qdepth + 1.2) * cache_file_size / write_buffer_size;
  }

  // Writer dispatch size. The writer thread will dispatch the IO at the
  // specified IO size
  uint64_t writer_dispatch_size = 1ULL * 1024 * 1024;

  BlockCacheOptions MakeBlockCacheOptions(const std::string& path,
                                          const uint64_t size,
                                          const std::shared_ptr<Logger>& lg);
};

//
// Represents a logical record on device
//
// LBA = { cache-file-id, offset, size }
//
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

//
// Abstraction for a general cache tier
//
class CacheTier : public PageCache {
 public:
  typedef LogicalBlockAddress LBA;

  virtual ~CacheTier() {}

  // Create or open an existing cache
  virtual Status Open() {
    if (next_tier_) {
      return next_tier_->Open();
    }

    return Status::OK();
  }

  // Close cache
  virtual Status Close() {
    if (next_tier_) {
      return next_tier_->Close();
    }

    return Status::OK();
  }

  // Expand the cache to accommodate new data
  virtual bool Reserve(const size_t size) {
    // default implementation is a pass through
    return true;
  }

  // Remove a given key from the cache
  virtual bool Erase(const Slice& key) {
    // default implementation is a pass through since not all cache tiers might
    // support erase
    return true;
  }

  // Print stats as string
  virtual std::string PrintStats() {
    if (next_tier_) {
      return next_tier_->PrintStats();
    }
    return std::string();
  }

  // TEST: Flush data
  virtual void Flush_TEST() {
    if (next_tier_) {
      next_tier_->Flush_TEST();
    }
  }

  // Insert to page cache
  virtual Status Insert(const Slice& page_key, const void* data,
                        const size_t size) = 0;

  // Lookup page cache by page identifier
  virtual Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                        size_t* size) = 0;

  std::shared_ptr<CacheTier> next_tier_;
};

//
// Abstraction that helps you construct a tiers of caches as a
// unified cache.
class TieredCache : public CacheTier {
 public:
  virtual ~TieredCache() {
    assert(tiers_.empty());
  }

  // open tiered cache
  Status Open() override {
    assert(!tiers_.empty());
    return tiers_.front()->Open();
  }

  // close tiered cache
  Status Close() override {
    assert(!tiers_.empty());
    Status status = tiers_.front()->Close();
    if (status.ok()) {
      tiers_.clear();
    }
    return status;
  }

  // erase an element
  bool Erase(const Slice& key) override {
    assert(!tiers_.empty());
    return tiers_.front()->Erase(key);
  }

  // Print stats
  std::string PrintStats() override {
    assert(!tiers_.empty());
    return tiers_.front()->PrintStats();
  }

  // insert to tiered cache
  Status Insert(const Slice& page_key, const void* data,
                const size_t size) override {
    assert(!tiers_.empty());
    return tiers_.front()->Insert(page_key, data, size);
  }

  // Lookup tiered cache
  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override {
    assert(!tiers_.empty());
    return tiers_.front()->Lookup(page_key, data, size);
  }

  // TEST: Flush data
  void Flush_TEST() override {
    assert(!tiers_.empty());
    tiers_.front()->Flush_TEST();
  }

  // Factory method for creating tiered cache
  static std::unique_ptr<TieredCache> New(const size_t mem_size,
                                          const BlockCacheOptions& options);

 private:
  typedef std::shared_ptr<CacheTier> tier_t;

  void AddTier(const tier_t& tier) {
    if (!tiers_.empty()) {
      tiers_.back()->next_tier_ = tier;
    }
    tiers_.push_back(tier);
  }

  std::list<tier_t> tiers_;
};

}  // namespace rocksdb
