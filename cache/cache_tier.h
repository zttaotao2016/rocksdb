#pragma once

#include <iostream>
#include <list>

#include "include/rocksdb/status.h"
#include "include/rocksdb/page_cache.h"

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
class CacheTier : public PageCache {
 public:
  typedef LogicalBlockAddress LBA;

  virtual ~CacheTier() {}

  /**
   * Create or open an existing cache
   */
  virtual Status Open() {
    if (next_tier_) {
      return next_tier_->Open();
    }

    return Status::OK();
  }

  /**
   * Close cache
   */
  virtual Status Close() {
    if (next_tier_) {
      return next_tier_->Close();
    }

    return Status::OK();
  }

  /**
   * Expand the cache to accommodate new data
   */
  virtual bool Reserve(const size_t size) {
    // default implementation is a pass through
    return true;
  }

  /**
   * Remove a given key from the cache
   */
  virtual bool Erase(const Slice& key) {
    // default implementation is a pass through since not all cache tiers might
    // support erase
    return true;
  }

  /**
   * Print stats as string
   */
  virtual std::string PrintStats() { return std::string(); }

  // TEST: Flush data
  virtual void Flush_TEST() {
    if (next_tier_) {
      next_tier_->Flush_TEST();
    }
  }

  std::shared_ptr<CacheTier> next_tier_;
};

/**
 * Abstraction that helps you construct a tiers of caches as a
 * unified cache.
 */
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

  /**
   * Factory method for creating tiered cache
   */
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
