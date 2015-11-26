#pragma once

#include <list>
#include <set>
#include <memory>
#include <string>
#include <stdexcept>
#include <thread>

#include "cache/blockcache_file.h"
#include "cache/blockcache_metadata.h"
#include "cache/blockcache_file_writer.h"
#include "cache/cache_tier.h"
#include "cache/cache_util.h"
#include "db/skiplist.h"
#include "include/rocksdb/comparator.h"
#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "rocksdb/cache.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

namespace rocksdb {

/**
 * BlockCache options
 */
struct BlockCacheOptions {
  explicit BlockCacheOptions(Env* const _env, const std::string& _path,
                             const uint64_t _cache_size,
                             const std::shared_ptr<Logger>& _log,
                             const uint32_t _write_buffer_size
                                                  = 1 * 1024 * 1024) {
    env = _env;
    path = _path;
    log = _log;
    cache_size = _cache_size;
    cache_file_size = 100ULL * 1024 * 1024;
    writer_qdepth = 1;
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
  uint32_t writer_qdepth = 2;

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
    : insert_th_(&BlockCacheImpl::InsertMain, this),
      opt_(opt),
      writer_(this, opt_.writer_qdepth) {
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
  bool Lookup(const Slice & key, std::unique_ptr<char[]>* data,
              size_t* size) override;
  bool Erase(const Slice& key) override;
  bool Reserve(const size_t size) override;
  Status Close() override;

  virtual void Flush_TEST() override {
    while (insert_ops_.Size()) {
      sleep(1);
    }
  }

 private:
  /**
   * Insert op
   */
  struct InsertOp {
    explicit InsertOp(const bool exit_loop) : exit_loop_(exit_loop) {}
    explicit InsertOp(std::string&& key, std::unique_ptr<char[]>&& data,
                      const size_t size)
      : key_(std::move(key)), data_(std::move(data)), size_(size) {}

    ~InsertOp() {}

    InsertOp() = delete;
    InsertOp(InsertOp&) = delete;
    InsertOp(InsertOp&& rhs) = default;
    InsertOp& operator=(InsertOp&& rhs) = default;

    size_t Size() const { return size_; }

    std::string key_;
    std::unique_ptr<char[]> data_;
    const size_t size_ = 0;
    const bool exit_loop_ = false;
  };

  // entry point for insert thread
  void InsertMain();
  // insert implementation
  Status InsertImpl(const Slice& key, const std::unique_ptr<char[]>& buf,
                    const size_t size);
  // Create a new cache file
  void NewCacheFile();
  // Get cache directory path
  std::string GetCachePath() const { return opt_.path + "/cache"; }

  port::RWMutex lock_;                  // Synchronization
  BoundedQueue<InsertOp> insert_ops_;   // Ops waiting for insert
  std::thread insert_th_;               // Insert thread
  const BlockCacheOptions opt_;         // BlockCache options
  uint32_t writerCacheId_ = 0;          // Current cache file identifier
  WriteableCacheFile* cacheFile_ = nullptr;   // Current cache file reference
  CacheWriteBufferAllocator bufferAllocator_; // Buffer provider
  ThreadedWriter writer_;               // Writer threads
  BlockCacheMetadata metadata_;         // Cache meta data manager
  std::atomic<uint64_t> size_{0};       // Size of the cache
};

}  // namespace rocksdb
