#pragma once

#include "cache/blockcache_file.h"
#include "cache/blockcache_metadata.h"
#include "cache/blockcache_file_writer.h"
#include "cache/cache_tier.h"
#include "db/skiplist.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "rocksdb/cache.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"
#include <list>
#include <set>
#include <memory>
#include <string>
#include <stdexcept>

namespace rocksdb {

/**
 * BlockCache options
 */
struct BlockCacheOptions {
  BlockCacheOptions(Env* const _env, const std::string& _path,
                    const uint64_t _cache_size,
                    const std::shared_ptr<Logger>& _log,
                    const uint32_t _write_buffer_size = 1 * 1024 * 1024) {
    env = _env;
    path = _path;
    log = _log;
    cache_size = _cache_size;
    cache_file_size = 100ULL * 1024 * 1024;
    writer_qdepth = 2;
    write_buffer_size = _write_buffer_size;
    write_buffer_count = 200;
    bufferpool_limit = 2ULL * write_buffer_size * write_buffer_count;
  }

  /**
   * Env abstraction to use for systmer level operations
   */
  Env* env;

  /**
   * Path for the block cache where blocks are persisted
   */
  std::string path;

  /**
   * Log handle for logging messages
   */
  std::shared_ptr<Logger> log;

  /**
   * Logical cache size
   */
  uint64_t cache_size = UINT64_MAX;

  /**
   * Cache consists of multiples of small files. This is the size of individual
   * cache file
   *
   * default: 1M
   */
  uint32_t cache_file_size = 100ULL * 1024 * 1024;

  /**
   * The writers can issues IO to the devices in parallel. This parameter
   * controls the qdepth to use for a given block device
   */
  uint32_t writer_qdepth = 1;

  /**
   * IO size to block device
   */
  uint32_t write_buffer_size = 1 * 1024 * 1024;

  /**
   * Number of buffers to pool
   * (should be greater than cache file size)
   */
  uint32_t write_buffer_count = 200;

  /**
   * Buffer poll limit to which it can grow
   */
  uint64_t bufferpool_limit = 2ULL * cache_file_size;

  BlockCacheOptions MakeBlockCacheOptions(const std::string& path,
                                          const uint64_t size,
                                          const std::shared_ptr<Logger>& lg);
};

/**
 * Block cache implementation
 */
class BlockCacheImpl : public SecondaryCacheTier {
 public:
  BlockCacheImpl(const BlockCacheOptions& opt)
      : opt_(opt),
        writerCacheId_(0),
        cacheFile_(nullptr),
        writer_(this, opt.env, opt_.writer_qdepth),
        size_(0) {
    Info(opt_.log, "Initializing allocator. size=%d B count=%d limit=%d B",
         opt_.write_buffer_size, opt_.write_buffer_count,
         opt_.bufferpool_limit);

    bufferAllocator_.Init(opt.write_buffer_size, opt.write_buffer_count,
                          opt.bufferpool_limit);
  }

  virtual ~BlockCacheImpl() {}

  // Open and initialize cache
  Status Open() override;

  /*
   * override from SecondaryCacheTier
   */
  Status Insert(const Slice& key, void* data, const size_t size) override;
  bool LookupKey(const Slice& key) override;
  bool Lookup(const Slice & key, std::unique_ptr<char[]>* data,
              size_t* size) override;
  bool Erase(const Slice& key) override;
  bool Reserve(const size_t size) override;
  Status Close() override;

 private:
  // Create a new cache file
  void NewCacheFile();
  // Get cache directory path
  std::string GetCachePath() const { return opt_.path + "/cache"; }

  port::RWMutex lock_;                  // Synchronization
  const BlockCacheOptions opt_;         // BlockCache options
  uint32_t writerCacheId_;              // Current cache file identifier
  WriteableCacheFile* cacheFile_;       // Current cache file reference
  CacheWriteBufferAllocator bufferAllocator_; // Buffer provider
  ThreadedWriter writer_;               // Writer threads
  BlockCacheMetadata metadata_;         // Cache meta data manager
  std::atomic<uint64_t> size_;          // Size of the cache
};

/**
 * Wrapper implementation for test BlockCacheImpl with RocksDB
 */
class RocksBlockCache : public Cache {
 public:
  /*
   * Handle abstraction to support raw pointers and blocks
   */
  struct HandleBase : Handle {
    typedef void (*deleter_t)(const Slice&, void*);

    explicit HandleBase(const Slice& key, const size_t size,
                        deleter_t deleter)
      : key_(std::move(key.ToString())),
        size_(size),
        deleter_(deleter) {
    }

    virtual ~HandleBase() {}

    virtual void* value() = 0;

    std::string key_;
    const size_t size_ = 0;
    deleter_t deleter_ = nullptr;
  };

  /*
   * Handle for raw pointers
   */
  struct DataHandle : HandleBase
  {
    explicit DataHandle(const Slice& key, char* const data = nullptr,
                        const size_t size = 0,
                        const deleter_t deleter = nullptr)
      : HandleBase(key, size, deleter)
      , data_(data) {}

    virtual ~DataHandle() {
      assert(deleter_);
      (*deleter_)(key_, data_);
    }

    void* value() override { return data_; }

    char* data_ = nullptr;
  };

  /*
   * Handle for blocks
   */
  struct BlockHandle : HandleBase
  {
    explicit BlockHandle(const Slice& key, Block* const block,
                         const deleter_t deleter = nullptr)
      : HandleBase(key, block->size(), deleter)
      , block_(block) {
      assert(block);
    }

    virtual ~BlockHandle() {
      if (deleter_) {
        (*deleter_)(key_, block_);
      } else {
        delete block_;
      }
    }

    void* value() override { return block_; }

    Block* block_ = nullptr;
  };

  RocksBlockCache(const shared_ptr<BlockCacheImpl>& cache_impl);
  RocksBlockCache(Env* env, const std::string path);
  virtual ~RocksBlockCache();

  // Cache override
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override;
  Handle* InsertBlock(const Slice& key, Block* value,
                      void (*deleter)(const Slice& key, void* value)) override;
  Handle* Lookup(const Slice& key) override;
  void Release(Handle* handle) override;
  void* Value(Handle* handle) override;
  void Erase(const Slice& key) override {
    cache_->Erase(key);
  }
  uint64_t NewId() override { return (uint64_t)this; }
  size_t GetUsage(Handle* handle) const override {
    return ((HandleBase*) handle)->size_;
  }
  void SetCapacity(size_t capacity) override {}
  size_t GetCapacity() const override {
    return 0;
  }

  // Not implemented override
  size_t GetUsage() const override {
    assert(!"not implemented");
  }

  // returns the memory size for the entries in use by the system
  size_t GetPinnedUsage() const override {
    assert(!"not implemented");
  }

  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override {
    assert(!"not implemented");
  }

 private:
  shared_ptr<BlockCacheImpl> cache_;
};

}  // namespace rocksdb
