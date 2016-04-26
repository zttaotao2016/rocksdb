#pragma once

#include <atomic>
#include <sstream>

#include "rocksdb/cache.h"
#include "rocksdb/cache_tier.h"

#include "cache/hash_table_evictable.h"
#include "cache/hash_table.h"

namespace rocksdb {

class VolatileCache : public CacheTier {
 public:
  explicit VolatileCache(
    const size_t max_size = std::numeric_limits<size_t>::max())
    : max_size_(max_size) {}

  virtual ~VolatileCache();

  // insert to cache
  Status Insert(const Slice& page_key, const void* data,
                const size_t size) override;
  // lookup key in cache
  Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                size_t* size) override;
  // erase key from cache
  bool Erase(const Slice& key) override;

  // Print stats
  std::string PrintStats() override {
    std::ostringstream ss;
    ss << "pagecache.volatilecache.hits: "
       << stats_.cache_hits_ << std::endl
       << "pagecache.volatilecache.misses: "
       << stats_.cache_misses_ << std::endl
       << "pagecache.volatilecache.inserts: "
       << stats_.cache_inserts_ << std::endl
       << "pagecache.volatilecache.evicts: "
       << stats_.cache_evicts_ << std::endl
       << "pagecache.volatilecache.hit_pct: "
       << stats_.CacheHitPct() << std::endl
       << "pagecache.volatilecache.miss_pct: "
       << stats_.CacheMissPct() << std::endl
       << CacheTier::PrintStats();
    return std::move(ss.str());
  }

 private:
  /*
   * Cache data abstraction
   */
  struct CacheData : LRUElement<CacheData> {
    explicit CacheData(CacheData&& rhs)
      : key(std::move(rhs.key)), value(std::move(rhs.value)) {}

    explicit CacheData(const std::string& _key,
                       const std::string& _value = "")
      : key(_key), value(_value) {}

    virtual ~CacheData() {}

    const std::string key;
    const std::string value;
  };

  static void DeleteCacheData(CacheData* data);

  /*
   * Index and LRU definition
   */
  struct CacheDataHash {
    uint64_t operator()(const CacheData* obj) const {
      assert(obj);
      return std::hash<std::string>()(obj->key);
    }
  };

  struct CacheDataEqual {
    bool operator()(const CacheData* lhs, const CacheData* rhs) const {
      assert(lhs);
      assert(rhs);
      return lhs->key == rhs->key;
    }
  };

  typedef EvictableHashTable<CacheData, CacheDataHash,
                             CacheDataEqual> IndexType;

  struct Stats {
    uint64_t cache_misses_ = 0;
    uint64_t cache_hits_ = 0;
    uint64_t cache_inserts_ = 0;
    uint64_t cache_evicts_ = 0;

    double CacheHitPct() const {
      auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_hits_ / (double) lookups : 0.0;
    }

    double CacheMissPct() const {
      auto lookups = cache_hits_ + cache_misses_;
      return lookups ? 100 * cache_misses_ / (double) lookups : 0.0;
    }
  };

  // Evict LRU tail
  bool Evict();

  IndexType index_;                     // in-memory cache
  std::atomic<uint64_t> max_size_{0};   // Maximum size of the cache
  std::atomic<uint64_t> size_{0};       // Size of the cache
  Stats stats_;
};

}  // namespace rocksdb
