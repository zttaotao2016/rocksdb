#pragma once

#include <atomic>

#include "include/rocksdb/cache.h"
#include "cache/cache_tier.h"
#include "cache/hash_table_evictable.h"
#include "cache/hash_table.h"

namespace rocksdb {

class VolatileCache : public PrimaryCacheTier {
 public:
  VolatileCache(const size_t max_size = UINT64_MAX)
    : max_size_(max_size),
      size_(0) {
  }
  virtual ~VolatileCache();

  /*
   * override from Cache
   */
  typedef void (*deleter_t)(const Slice&, void*);

  Handle* Insert(const Slice& key, void* value, size_t charge,
                 deleter_t deleter) override;
  Handle* InsertBlock(const Slice& key, Block* value,
                      deleter_t deleter) override;
  Handle* Lookup(const Slice& key) override;
  void Release(Handle* handle) override;
  void* Value(Handle* handle) override;
  void Erase(const Slice& key) override;

  void SetCapacity(size_t capacity) override { max_size_ = capacity; }
  size_t GetCapacity() const override { return size_; }
  size_t GetUsage(Handle* handle) const override {
    return ((CacheObject*) handle)->Size();
  }

  /*
   * Not implemented
   */
  uint64_t NewId() override {
    assert(!"not supported");
    throw std::runtime_error("not supported");
  }
  size_t GetUsage() const override {
    throw std::runtime_error("not implemented");
  }

  size_t GetPinnedUsage() const override {
    throw std::runtime_error("not supported");
  }

  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override {
    throw std::runtime_error("not supported");
  }

 private:
  /*
   * Abstract representation of a cache object
   */
  struct CacheObject : Cache::Handle, LRUElement<CacheObject> {
    explicit CacheObject(const Slice& key)
      : key_(std::move(key.ToString())) {}

    virtual ~CacheObject() {}

    // Get the key for the object
    const std::string& Key() const { return key_; }
    // Get the opaque value of the object
    virtual const void* Value() const {
      throw std::runtime_error("not implemented");
    }
    // Get the size of the object
    virtual size_t Size() const {
      throw std::runtime_error("not implemented");
    }
    // Can this object be serialized ?
    virtual bool Serializable() const {
      throw std::runtime_error("not implemented");
    }

    const std::string key_;
  };

  /*
   * Pointer references. Not serializable
   */
  struct PtrRef : CacheObject {
    typedef void (*deleter_t)(const Slice&, void*);

    explicit PtrRef(const Slice& key, void* const data, const size_t size,
                    deleter_t deleter)
      : CacheObject(key), data_(data), size_(size), deleter_(deleter) {}

    virtual ~PtrRef() {
      assert(deleter_);
      if (deleter_) {
        (*deleter_)(Slice(key_), data_);
      }
    }

    const void* Value() const override { return data_; }
    size_t Size() const override { return size_; }
    bool Serializable() const override { return false; }

    void* data_;
    const size_t size_;
    deleter_t deleter_;
  };

  /*
   * Block (Serializable)
   */
  struct BlockData : CacheObject {
    typedef void (*deleter_t)(const Slice&, void*);

    explicit BlockData(const Slice& key, Block* const block, deleter_t deleter)
      : CacheObject(key), block_(block), deleter_(deleter) {}

    virtual ~BlockData() {
      assert(deleter_);
      if (deleter_) {
        (*deleter_)(Slice(key_), block_);
      }
    }

    const void* Value() const override { return block_; }
    size_t Size() const override { return block_->size(); }
    bool Serializable() const override { return true; }

    Block* const block_;
    deleter_t deleter_;
  };

  static void DeleteCacheObj(VolatileCache::CacheObject* obj) {
    assert(obj);
    delete obj;
  }

  /*
   * Index and LRU definition
   */
  struct CacheObjectHash {
    uint64_t operator()(const CacheObject* obj) const {
      assert(obj);
      return std::hash<std::string>()(obj->Key());
    }
  };

  struct CacheObjectEqual {
    bool operator()(const CacheObject* lhs, const CacheObject* rhs) const {
      assert(lhs);
      assert(rhs);
      return lhs->Key() == rhs->Key();
    }
  };

  typedef EvictableHashTable<CacheObject, CacheObjectHash, CacheObjectEqual> IndexType;

  // Evict LRU tail
  bool Evict();
  // Erase implementation without grabbing lock
  CacheObject* EraseFromIndex(const Slice& key);

  IndexType index_;                     // in-memory cache
  std::atomic<uint64_t> max_size_;      // Maximum size of the cache
  std::atomic<uint64_t> size_;          // Size of the cache
};

}  // namespace rocksdb
