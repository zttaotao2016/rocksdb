#pragma once

#include "blkcache/blkcache_cachefile.h"
#include "blkcache/persistent_blkcache.h"
#include "blkcache/blkcache_index.h"
#include "blkcache/blkcache_writer.h"
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
#include <memory>
#include <string>
#include <stdexcept>

namespace rocksdb {

/**
 *
 */
class BlockCacheImpl : public PersistentBlockCache {
 public:
  struct Options {
    std::string path;
    std::shared_ptr<Logger> info_log;
    uint32_t writeBufferSize = 1 * 1024 * 1024;
    uint32_t writeBufferCount = 10;
    uint32_t max_bufferpool_size_ = writeBufferSize * writeBufferCount;
    uint32_t maxCacheFileSize = 2 * 1024 * 1024;
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

  // override from PersistentBlockCache
  Status Open() override;
  Status Close() override;
  Status Insert(const Slice& key, void* data, uint16_t size, LBA* lba) override;
  bool Lookup(const Slice & key, std::unique_ptr<char>* data,
              uint32_t* size) override;
  bool Erase(const Slice& key) override;
  bool Reserve(const size_t size) override;

 private:
  void NewCacheFile();

  std::string GetCachePath() const { return opt_.path + "/cache"; }

  port::RWMutex lock_;
  Env* const env_;
  const Options opt_;
  uint32_t writerCacheId_;
  WriteableCacheFile* cacheFile_;
  CacheWriteBufferAllocator bufferAllocator_;
  ThreadedWriter writer_;
  SimpleBlockCacheMetadata metadata_;
  std::shared_ptr<Logger> log_;
  size_t size_;
};

class RocksBlockCache : public Cache {
 public:

  struct BlkCacheHandle : Handle
  {
    BlkCacheHandle()
      : should_dalloc_(false) {}

    void Copy(const Slice& key, char* const data, const size_t size)
    {
      char* k = new char[key.size()];
      memcpy(k, key.data(), key.size());
      key_ = Slice(k, key.size());

      data_ = new char[size];
      memcpy(data_, data, size);

      should_dalloc_ = true;
    }

    void Assign(const Slice& key, char* const data, const size_t size)
    {
      key_ = key;
      data_ = data;
      size_ = size;
      should_dalloc_ = false;
    }


    ~BlkCacheHandle()
    {
      if (should_dalloc_) {
        delete[] key_.data();
        delete[] data_;
      }
    }

    bool should_dalloc_;
    Slice key_;
    char* data_;
    size_t size_;
  };

  RocksBlockCache(Env* env, const std::string path);
  virtual ~RocksBlockCache();

  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override;

  Handle* Lookup(const Slice& key) override;

  void Release(Handle* handle) override;

  void* Value(Handle* handle) override;

  void Erase(const Slice& key) override {
    cache_->Erase(key);
  }

  uint64_t NewId() override { return (uint64_t)this; }

  void SetCapacity(size_t capacity) override { capacity_ = capacity; }

  size_t GetCapacity() const override {
    return capacity_;
  }

  size_t GetUsage() const override {
    assert(!"not implemented");
    throw std::runtime_error("not implemented");
  }

  // returns the memory size for a specific entry in the cache.
  size_t GetUsage(Handle* handle) const override {
    return ((BlkCacheHandle*) handle)->size_;
  }

  // returns the memory size for the entries in use by the system
  size_t GetPinnedUsage() const override {
    assert(!"not implemented");
    throw std::runtime_error("not supported");
  }

  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override {
    assert(!"not implemented");
    throw std::runtime_error("not supported");
  }

 private:
  size_t capacity_;
  shared_ptr<Logger> log_;
  shared_ptr<BlockCacheImpl> cache_;
};



}  // namespace rocksdb
