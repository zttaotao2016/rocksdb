#pragma once

#include <unordered_map>
#include <functional>
#include <string>
#include "include/rocksdb/slice.h"
#include "blkcache/blkcache_cachefile.h"

namespace rocksdb {

/**
 *
 */
class CacheFileIndex {
 public:

  virtual ~CacheFileIndex() {}

  virtual bool Insert(BlockCacheFile* const file) = 0;
  virtual BlockCacheFile* Lookup(const uint32_t cache_id) = 0;
  virtual void Clear() = 0;
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

/**
 *
 */
class BlockLookupIndex {
 public:

  virtual ~BlockLookupIndex() {}

  virtual bool Insert(const Slice& key, const LBA& lba) = 0;
  virtual bool Lookup(const Slice& key, LBA* lba) = 0;
  virtual bool Remove(const Slice& key) = 0;
};

/**
 *
 */
class SimpleBlockLookupIndex : public BlockLookupIndex {
 public:

  virtual ~SimpleBlockLookupIndex() {}

  bool Insert(const Slice& key, const LBA& lba) override;
  bool Lookup(const Slice& key, LBA* lba) override;
  bool Remove(const Slice& key) override;

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
class SimpleBlockCacheMetadata : public SimpleBlockLookupIndex,
                                 public SimpleCacheFileIndex {
 public:

  virtual ~SimpleBlockCacheMetadata() {}

  using SimpleCacheFileIndex::Insert;
  using SimpleCacheFileIndex::Lookup;
  using SimpleBlockLookupIndex::Insert;
  using SimpleBlockLookupIndex::Lookup;
};

}
