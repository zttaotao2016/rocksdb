#pragma once

#include "include/rocksdb/status.h"
#include "include/rocksdb/cache.h"

namespace rocksdb {

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
};

/**
 * Secondary cache tier only supports storage/retrieval of raw data
 */
class SecondaryCacheTier : public CacheTier {
 public:
  typedef LogicalBlockAddress LBA;

  virtual ~SecondaryCacheTier() {}

  /**
   * Insert key value into the cache
   */
  virtual Status Insert(const Slice& key, void* data, const uint32_t size) = 0;

  /**
   * Lookup to see if the given key exists in the cache
   */
  virtual bool LookupKey(const Slice& key) = 0;

  /**
   * Lookup a given key in the cache
   */
  virtual bool Lookup(const Slice & key, std::unique_ptr<char[]>* val,
                      uint32_t* size) = 0;

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

  virtual ~TieredCache() {
    if (pcache_) {
      pcache_->Close();
    }
  }

  std::shared_ptr<PrimaryCacheTier> GetCache() { return pcache_; }

 private:
  std::shared_ptr<PrimaryCacheTier> pcache_;
};

}  // namespace rocksdb
