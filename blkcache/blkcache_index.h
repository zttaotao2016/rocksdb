#pragma once

#include <unordered_map>
#include <functional>
#include <string>

#include "include/rocksdb/slice.h"
#include "blkcache/blkcache_cachefile.h"
#include "blkcache/scalable_hash_table.h"
#include "blkcache/blkcache_lrulist.h"

namespace rocksdb {

/**
 *
 */
class CacheFileIndex {
 public:

  virtual ~CacheFileIndex() {}

  virtual bool Insert(BlockCacheFile* const file) = 0;
  virtual BlockCacheFile* Lookup(const uint32_t cache_id) = 0;
  virtual BlockCacheFile* Evict() = 0;
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

  bool Insert(BlockCacheFile* file) override {
    WriteLock _(&rwlock_);
    auto status = index_.insert(std::make_pair(file->cacheid(), file));
    if (status.second) {
      lru_.Push(file);
    }
    return status.second;
  }

  BlockCacheFile* Lookup (const uint32_t cache_id) override {
    ReadLock _(&rwlock_);
    auto it = index_.find(cache_id);
    if (it == index_.end()) {
      return nullptr;
    }

    assert(cache_id == it->first);
    lru_.Touch(it->second);
    return it->second;
  }

  BlockCacheFile* Evict() override {
    WriteLock _(&rwlock_);
    if (lru_.IsEmpty()) {
      return nullptr;
    }

    BlockCacheFile* f = lru_.Pop();
    auto it = index_.find(f->cacheid());
    assert(it != index_.end());
    assert(it->second == f);
    return f;
  }

  void Clear()
  {
    WriteLock _(&rwlock_);
    for (auto it = index_.begin(); it != index_.end(); ++it) {
      lru_.Unlink(it->second);
      delete it->second;
    }

    index_.clear();
    assert(lru_.IsEmpty());
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
  LRUList<BlockCacheFile> lru_;
};

/**
 *
 */
struct BlockInfo {
  BlockInfo(const Slice& key, const LBA& lba = LBA()) {
    key_ = new char[key.size()];
    memcpy(key_, key.data(), key.size());
    key_size_ = key.size();
    lba_ = lba;
  }

  ~BlockInfo() {
    delete[] key_;
  }

  char* key_;
  size_t key_size_;
  LBA lba_;
};


/**
 *
 */
class BlockLookupIndex {
 public:

  virtual ~BlockLookupIndex() {}

  virtual bool Insert(BlockInfo* binfo) = 0;
  virtual bool Lookup(const Slice& key, LBA* lba) = 0;
  virtual BlockInfo* Remove(const Slice& key) = 0;
};

/**
 *
 */
class SimpleBlockLookupIndex : public BlockLookupIndex {
 public:

  virtual ~SimpleBlockLookupIndex() {}

  bool Insert(BlockInfo* binfo) override;
  bool Lookup(const Slice& key, LBA* lba) override;
  BlockInfo* Remove(const Slice& key) override;

 private:

  struct Hash {
    size_t operator()(BlockInfo* node) const {
      std::hash<std::string> h;
      return h(std::string(node->key_, node->key_size_));
    }
  };

  struct Equal {
    size_t operator()(BlockInfo* lhs, BlockInfo* rhs) const {
      return lhs->key_size_ == rhs->key_size_
             && memcmp(lhs->key_, rhs->key_, lhs->key_size_) == 0;
    }
  };


  typedef ScalableHashTable<BlockInfo*, Hash, Equal> IndexType;

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
