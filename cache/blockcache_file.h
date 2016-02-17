#pragma once

#include <list>
#include <memory>
#include <string>

#include "include/rocksdb/comparator.h"
#include "include/rocksdb/env.h"
#include "include/rocksdb/cache_tier.h"

#include "cache/blockcache_alloc.h"
#include "cache/lrulist.h"
#include "db/skiplist.h"
#include "port/port_posix.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/mutexlock.h"

namespace rocksdb {

class WriteableCacheFile;
struct BlockInfo;

/**
 * BlockCacheFile
 *       ^
 *       |
 *       |
 * RandomAccessCacheFile (Can be used only for reading)
 *       ^
 *       |
 *       |
 * WriteableCacheFile ---* Writer (Used to pipeline writes to media)
 * (caches writes until flushed)
 */
class Writer {
 public:

  Writer(CacheTier* const cache)
    : cache_(cache) {
  }
  virtual ~Writer() {}

  virtual void Write(WriteableCacheFile* file, CacheWriteBuffer* buf,
                     const uint64_t file_off) = 0;
  virtual void Stop() = 0;

  CacheTier* const cache_;
};

/**
 * class BlockCacheFile
 *
 */
class BlockCacheFile : public LRUElement<BlockCacheFile>
{
 public:
  BlockCacheFile(const uint32_t cache_id)
    : LRUElement<BlockCacheFile>(),
      cache_id_(cache_id) {}

  BlockCacheFile(Env* const env, const std::string& dir,
                 const uint32_t cache_id)
    : LRUElement<BlockCacheFile>(),
      env_(env),
      dir_(dir),
      cache_id_(cache_id) {
  }

  virtual ~BlockCacheFile() {}

  virtual bool Append(const Slice& key, const Slice& val, LBA* const lba) {
    throw std::runtime_error("not implemented");
  }

  virtual bool Read(const LBA& lba, Slice* key, Slice* block, char* scratch) {
    throw std::runtime_error("not implemented");
  }

  std::string Path() const {
    return dir_ + "/" + std::to_string(cache_id_);
  }

  uint32_t cacheid() const { return cache_id_; }

  virtual void Add(BlockInfo* binfo) {
    throw std::runtime_error("not implemented");
  }

  std::list<BlockInfo*>& block_infos() { return block_infos_; }

  virtual Status Delete(size_t* size);

  uint32_t Id() const { return cache_id_; }

 protected:
  Env* const env_ = nullptr;
  const std::string dir_;
  const uint32_t cache_id_;
  std::list<BlockInfo*> block_infos_;
};

/**
 * class RandomAccessFile
 *
 * (thread safe)
 */
class RandomAccessCacheFile : public BlockCacheFile {
 public:

  RandomAccessCacheFile(Env* const env, const std::string& dir,
                        const uint32_t cache_id, const shared_ptr<Logger>& log)
    : BlockCacheFile(env, dir, cache_id),
      log_(log) {}

  virtual ~RandomAccessCacheFile() {}

  bool Open();

  bool Read(const LBA& lba, Slice* key, Slice* block, char* scratch) override;

  bool Append(const Slice&, const Slice&, LBA*) override {
    assert(!"Not implemented");
    return false;
  }

  void Add(BlockInfo* binfo) override {
    WriteLock _(&rwlock_);
    block_infos_.push_back(binfo);
  }

 private:

  std::unique_ptr<RandomAccessFile> file_;

 protected:

  bool OpenImpl();
  bool ParseRec(const LBA& lba, Slice* key, Slice* val, char* scratch);

  port::RWMutex rwlock_;
  std::shared_ptr<Logger> log_;
};

/**
 * class WriteableCacheFile
 *
 * (thread safe)
 */
class WriteableCacheFile : public RandomAccessCacheFile {
 public:
  WriteableCacheFile(Env* const env, CacheWriteBufferAllocator& alloc,
                     Writer& writer, const std::string& dir,
                     const uint32_t cache_id, const uint32_t max_size,
                     const std::shared_ptr<Logger> & log)
    : RandomAccessCacheFile(env, dir, cache_id, log),
      alloc_(alloc),
      writer_(writer),
      max_size_(max_size) {}

  virtual ~WriteableCacheFile();

  bool Create();

  bool Read(const LBA& lba, Slice* key, Slice* block, char* scratch)  override {
    ReadLock _(&rwlock_);
    const bool closed = eof_ && bufs_.empty();
    if (closed) {
      return RandomAccessCacheFile::Read(lba, key, block, scratch);
    }

    return ReadImpl(lba, key, block, scratch);
  }

  bool Append(const Slice&, const Slice&, LBA*) override;

  bool Eof() const { return eof_; }

 private:
  friend class ThreadedWriter;

  bool ReadImpl(const LBA& lba, Slice* key, Slice* block, char* scratch);
  bool ReadBuffer(const LBA& lba, char* data);

  bool ExpandBuffer(const size_t size);
  void DispatchBuffer();
  void BufferWriteDone(CacheWriteBuffer* buf);
  void ClearBuffers();
  void Close();

  CacheWriteBufferAllocator& alloc_;    // Buffer provider
  Writer& writer_;                      // File writer thread
  std::unique_ptr<WritableFile> file_;  // RocksDB Env file abstraction
  std::vector<CacheWriteBuffer*> bufs_; // Written buffers
  uint32_t size_ = 0;                   // Size of the file
  const uint32_t max_size_;             // Max size of the file
  bool eof_ = false;                    // End of file
  uint32_t disk_woff_ = 0;              // Offset 
  size_t buf_woff_ = 0;                 // off into bufs_ to write
  size_t buf_doff_ = 0;                 // off into bufs_ to dispatch
  size_t pending_ios_ = 0;              // Number of ios to disk in-progress
};

} // namespace rocksdb
