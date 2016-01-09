#pragma once

#include <atomic>
#include <sstream>

#include "include/rocksdb/cache.h"
#include "cache/cache_tier.h"
#include "cache/hash_table_evictable.h"
#include "cache/hash_table.h"

namespace rocksdb {

class VolatileCache : public CacheTier {
 public:
  explicit VolatileCache(const size_t max_size = UINT64_MAX)
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
    ss << "Volatile cache stats: " << std::endl
       << "* cache hits: " << stats_.cache_hits_ << std::endl
       << "* cache misses: " << stats_.cache_misses_ << std::endl;

    CacheTier::PrintStats();
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
  };

  // Evict LRU tail
  bool Evict();

  IndexType index_;                     // in-memory cache
  std::atomic<uint64_t> max_size_{0};   // Maximum size of the cache
  std::atomic<uint64_t> size_{0};       // Size of the cache
  Stats stats_;
};

}  // namespace rocksdb
