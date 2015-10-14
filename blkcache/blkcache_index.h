#pragma once

#include <unordered_map>
#include <functional>
#include <string>
#include "include/rocksdb/slice.h"
#include "blkcache/persistent_blkcache.h"

namespace rocksdb {

/**
 *
 */
class BlockCacheIndex {
 public:
  virtual ~BlockCacheIndex() {}

  virtual bool Insert(const Slice& key, const LBA& lba) = 0;

  virtual bool Lookup(const Slice& key, LBA* lba) = 0;

  virtual bool Remove(const Slice& key) = 0;

  virtual int RemovePrefix(const Slice& key) { assert(!"not implemented"); }
};

/**
 *
 */
class SimpleBlockCacheIndex : public BlockCacheIndex {
 public:

  bool Insert(const Slice& key, const LBA& lba) override {
    WriteLock _(&rwlock_);
    auto status = index_.insert(std::make_pair(key, lba));
    assert(status.second);
    return status.second;
  }

  bool Lookup(const Slice& key, LBA* lba) override {
    ReadLock _(&rwlock_);

    auto it = index_.find(key);
    if (it == index_.end()) {
      return false;
    }

    assert(key == it->first);
    *lba = it->second;

    return true;
  }

  bool Remove(const Slice& key) override {
    WriteLock _(&rwlock_);
    const size_t count = index_.erase(key);
    assert(count <= 1);
   return count;
  }

 private:
  struct Hash {
    size_t operator()(const Slice& key) const {
      std::hash<std::string> h;
      return h(key.ToString());
    }
  };

  typedef std::unordered_map<Slice, LBA, Hash> IndexType;

  port::RWMutex rwlock_;
  IndexType index_;
};

/**
 *
 */
class CacheFileIndex {
 public:

  virtual ~CacheFileIndex() {}

  virtual bool Insert(BlockCacheFile* const file) = 0;

  virtual BlockCacheFile* Lookup(const uint32_t cache_id) = 0;

  virtual void Clear() = 0;
  // virtual bool Remove(const uint32_t cache_id) = 0;
};

/**
 *
 */
class SimpleCacheFileIndex : public CacheFileIndex
{
 public:

  virtual ~SimpleCacheFileIndex() {
    Clear();
  }

  bool Insert(BlockCacheFile* const file) override {
    WriteLock _(&rwlock_);
    auto status = index_.insert(std::make_pair(file->cacheid(), file));
    return status.second;
  }

  BlockCacheFile* Lookup (const uint32_t cache_id) override {
    ReadLock _(&rwlock_);
    auto it = index_.find(cache_id);
    if (it == index_.end()) {
      return nullptr;
    }

    assert(cache_id == it->first);
    return it->second;
  }

  void Clear()
  {
    WriteLock _(&rwlock_);
    for (auto it = index_.begin(); it != index_.end(); ++it) {
      delete it->second;
    }

    index_.clear();
  }

 private:

  struct Hash {
    size_t operator()(const uint32_t cache_id) const {
      return std::hash<uint32_t>()(cache_id);
    }
  };

  typedef std::unordered_map<uint32_t, BlockCacheFile*, Hash> IndexType;

  port::RWMutex rwlock_;
  IndexType index_;
};

}
