#pragma once

#include "blkcache/blkcache_cachefile.h"
#include "blkcache/blkcache_index.h"
#include "blkcache/blkcache_writer.h"
#include "blkcache/persistent_blkcache.h"
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

namespace rocksdb {

/**
 *
 */
class NVMBlockCache : public PersistentBlockCache {
 public:
  struct Options {
    std::string path;
    std::shared_ptr<Logger> info_log;
    uint32_t writeBufferSize = 1 * 1024 * 1024;
    uint32_t writeBufferCount = 10;
    uint32_t max_bufferpool_size_ = writeBufferSize * writeBufferCount;
    uint32_t maxCacheFileSize = 100 * 1024 * 1024;
    uint32_t writer_qdepth_ = 2;
  };

  NVMBlockCache(Env* env, const Options& opt)
      : env_(env),
        opt_(opt),
        writerCacheId_(0),
        cacheFile_(nullptr),
        writer_(env_, opt_.writer_qdepth_),
        log_(opt.info_log),
        block_index_(new SimpleBlockCacheIndex()),
        cache_file_index_(new SimpleCacheFileIndex()) {
    Info(log_, "Initializing allocator. size=%d B count=%d limit=%d B",
         opt_.writeBufferSize, opt_.writeBufferCount,
         opt_.max_bufferpool_size_);

    bufferAllocator_.Init(opt.writeBufferSize, opt.writeBufferCount,
                          opt.max_bufferpool_size_);
  }

  virtual ~NVMBlockCache() {}

  // override from PersistentBlockCache
  Status Open() override;
  Status Close() override;
  Status Insert(const Slice& key, void* data, uint16_t size, LBA* lba) override;
  bool Lookup(const Slice & key, std::unique_ptr<char>* data,
              uint32_t* size) override;
  // uint64_t GetCapacity() override;

 private:
  void NewCacheFile();

  std::string GetCachePath() const { return opt_.path + "/cache"; }

  port::RWMutex lock_;
  Env* const env_;
  const Options opt_;
  uint32_t writerCacheId_;
  WriteableCacheFile* cacheFile_;
  WriteBufferAllocator bufferAllocator_;
  ThreadedWriter writer_;
  std::shared_ptr<Logger> log_;
  std::unique_ptr<BlockCacheIndex> block_index_;
  std::unique_ptr<CacheFileIndex> cache_file_index_;
};

class NVMCache : public Cache {
 public:
  NVMCache() {}
  virtual ~NVMCache();

  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) = 0;

  virtual Handle* Lookup(const Slice& key) = 0;

  virtual void Release(Handle* handle) = 0;

  virtual void* Value(Handle* handle) = 0;

  virtual void Erase(const Slice& key) = 0;

  virtual uint64_t NewId() = 0;

  void SetCapacity(size_t capacity) override { assert("not supported"); }

  // returns the memory size for the entries in use by the system
  virtual size_t GetPinnedUsage() const = 0;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) = 0;

 private:
  shared_ptr<NVMBlockCache> nvm_;
};



}  // namespace rocksdb
