#pragma once

#include "cache/blkcache_cachefile.h"
#include "cache/cache_tier.h"
#include "cache/blkcache_index.h"
#include "cache/blkcache_writer.h"
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
 * Block cache implementation
 */
class BlockCacheImpl : public SecondaryCacheTier {
 public:
  struct Options {
    std::string path;
    std::shared_ptr<Logger> info_log;
    uint32_t writeBufferSize = 1 * 1024 * 1024;
    uint32_t writeBufferCount = 10;
    uint64_t max_bufferpool_size_ = 2ULL * writeBufferSize * writeBufferCount;
    uint64_t maxCacheFileSize = 2ULL * 1024 * 1024;
    uint32_t writer_qdepth_ = 2;
    uint64_t max_size_ = UINT64_MAX;
  };

  BlockCacheImpl(Env* env, const Options& opt)
      : env_(env),
        opt_(opt),
        writerCacheId_(0),
        cacheFile_(nullptr),
        writer_(this, env_, opt_.writer_qdepth_),
        log_(opt.info_log),
        size_(0) {
    Info(log_, "Initializing allocator. size=%d B count=%d limit=%d B",
         opt_.writeBufferSize, opt_.writeBufferCount,
         opt_.max_bufferpool_size_);

    bufferAllocator_.Init(opt.writeBufferSize, opt.writeBufferCount,
                          opt.max_bufferpool_size_);
  }

  virtual ~BlockCacheImpl() {}

  // Open and initialize cache
  Status Open();

  /*
   * override from SecondaryCacheTier
   */
  Status Insert(const Slice& key, void* data, uint32_t size) override;
  bool LookupKey(const Slice& key) override;
  bool Lookup(const Slice & key, std::unique_ptr<char[]>* data,
              uint32_t* size) override;
  bool Erase(const Slice& key) override;
  bool Reserve(const size_t size) override;
  Status Close() override;

 private:
  // Create a new cache file
  void NewCacheFile();
  // Get cache directory path
  std::string GetCachePath() const { return opt_.path + "/cache"; }

  port::RWMutex lock_;            // Synchronization
  Env* const env_;                // Env
  const Options opt_;             // BlockCache options
  uint32_t writerCacheId_;        // Current cache file identifier (auto inc)
  WriteableCacheFile* cacheFile_; // Current cache file reference
  CacheWriteBufferAllocator bufferAllocator_; // Buffer provider
  ThreadedWriter writer_;               // Writer threads
  SimpleBlockCacheMetadata metadata_;   // Cache meta data manager
  std::shared_ptr<Logger> log_;         // logger
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
