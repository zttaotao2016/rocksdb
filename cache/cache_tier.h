#pragma once

#include "include/rocksdb/status.h"
#include "include/rocksdb/cache.h"

namespace rocksdb {

struct BlockCacheOptions;

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
