#pragma once

#include <unordered_map>
#include <functional>
#include <string>

#include "rocksdb/slice.h"
#include "cache/blockcache_file.h"
#include "cache/hash_table.h"
#include "cache/hash_table_evictable.h"
#include "cache/lrulist.h"

namespace rocksdb {

/**
 * Block Cache Metadata
 *
 * The BlockCacheMetadata holds all the metadata associated with block cache. It
 * fundamentally contains 2 indexes and an LRU.
 *
 * Block Cache Index
 *
 * This is a forward index that maps a given key to a LBA (Logical Block
 * Address). LBA is a disk pointer that points to a record on the cache.
 *
 * LBA = { cache-id, offset, size }
 *
 * Cache File Index
 *
 * This is a forward index that maps a given cache-id to a cache file object.
 * Typically you would lookup using LBA and use the object to read or write
 */
struct BlockInfo {
  BlockInfo(const Slice& key, const LBA& lba = LBA())
    : key_(std::move(key.ToString())),
      lba_(lba) {}

  std::string key_;
  LBA lba_;
};

class BlockCacheMetadata {
 public:
  BlockCacheMetadata(const uint32_t blocks_capacity = 1024 * 1024,
                     const uint32_t cachefile_capacity = 10 * 1024)
    : cache_file_index_(cachefile_capacity),
      block_index_(blocks_capacity) {}

  virtual ~BlockCacheMetadata() {}

  /**
   * Insert a given cache file
   */
  bool Insert(BlockCacheFile* file);

  /**
   * Lookup cache file based on cache_id
   */
  BlockCacheFile* Lookup(const uint32_t cache_id);

  /**
   * Insert block information to block index
   */
  bool Insert(BlockInfo* binfo);

  /**
   * Lookup block information from block index
   */
  bool Lookup(const Slice& key, LBA* lba);

  /**
   * Remove a given from the block index
   */
  BlockInfo* Remove(const Slice& key);

  /**
   * Find and evict a cache file using LRU policy
   */
  BlockCacheFile* Evict();

  /**
   * Clear the metadata contents
   */
  virtual void Clear();

 protected:
  /**
   * Remove all block information from a given file
   */
  virtual void RemoveAllKeys(BlockCacheFile* file);

 private:
  /**
   * Cache file index definition
   *
   * cache-id => BlockCacheFile
   */
  struct BlockCacheFileHash {
    uint64_t operator()(const BlockCacheFile* rec) {
      return std::hash<uint32_t>()(rec->cacheid());
    }
  };

  struct BlockCacheFileEqual {
    uint64_t operator()(const BlockCacheFile* lhs, const BlockCacheFile* rhs) {
      return lhs->cacheid() == rhs->cacheid();
    }
  };

  static void DeleteBlockCacheFile(BlockCacheFile* file) {
    delete file;
  }

  typedef EvictableHashTable<BlockCacheFile, BlockCacheFileHash,
                             BlockCacheFileEqual> CacheFileIndexType;

  /**
   * Block Lookup Index
   *
   * key => LBA
   */
  static void DeleteBlockInfo(BlockInfo* binfo) {
    delete binfo;
  }

  struct Hash {
    size_t operator()(BlockInfo* node) const {
      return std::hash<std::string>()(node->key_);
    }
  };

  struct Equal {
    size_t operator()(BlockInfo* lhs, BlockInfo* rhs) const {
      return lhs->key_ == rhs->key_;
    }
  };

  typedef HashTable<BlockInfo*, Hash, Equal> BlockIndexType;

  CacheFileIndexType cache_file_index_;
  BlockIndexType block_index_;
};

}  // namespace rocksdb
