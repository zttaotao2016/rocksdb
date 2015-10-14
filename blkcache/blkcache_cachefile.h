#pragma once

#include <memory>
#include <list>
#include <string>
#include "include/rocksdb/env.h"
#include "include/rocksdb/comparator.h"
#include "db/skiplist.h"
#include "blkcache/persistent_blkcache.h"
#include "blkcache/blkcache_buffer.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace rocksdb {

class WriteableCacheFile;

/**
 *
 *
 */
class Writer {
 public:

  virtual ~Writer() {}

  virtual void Write(WriteableCacheFile* file, WriteBuffer* buf) = 0;
  virtual void Stop() = 0;
};

/**
 * class BlockCacheFile
 *
 */
class BlockCacheFile
{
 public:

  BlockCacheFile(Env* const env, const std::string& dir,
                 const uint32_t cache_id)
    : env_(env),
      dir_(dir),
      cache_id_(cache_id)
  {}

  virtual ~BlockCacheFile() {}

  virtual bool Append(const Slice& key, const Slice& val, LBA* const lba) = 0;

  virtual bool Read(const LBA& lba, Slice* key, Slice* block, char* scratch) = 0;

  std::string Path() const {
    return dir_ + "/" + std::to_string(cache_id_);
  }

  uint32_t cacheid() const { return cache_id_; }

 protected:

  Env* const env_;
  const std::string dir_;
  const uint32_t cache_id_;
};

/**
 * class RandomAccessFile
 *
 */
class RandomAccessCacheFile : public BlockCacheFile {
 public:

  RandomAccessCacheFile(Env* const env, const std::string& dir,
                        const uint32_t cache_id, const shared_ptr<Logger>& log)
    : BlockCacheFile(env, dir, cache_id),
      log_(log)
  {}

  virtual ~RandomAccessCacheFile() {}

  bool Open();

  bool Read(const LBA& lba, Slice* key, Slice* block, char* scratch) override;

  bool Append(const Slice&, const Slice&, LBA*) override {
    assert(!"Not implemented");
    return false;
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
 */
class WriteableCacheFile : public RandomAccessCacheFile {
 public:

  WriteableCacheFile(Env* const env, WriteBufferAllocator& alloc,
                     Writer& writer, const std::string& dir,
                     const uint32_t cache_id, const uint32_t max_size,
                     const std::shared_ptr<Logger> & log)
    : RandomAccessCacheFile(env, dir, cache_id, log),
      alloc_(alloc),
      writer_(writer),
      size_(0),
      max_size_(max_size),
      eof_(false),
      disk_woff_(0),
      buf_woff_(0),
      buf_doff_(0),
      is_io_pending_(false) {}

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
  void BufferWriteDone(WriteBuffer* buf);
  void Close();

  WriteBufferAllocator& alloc_;
  Writer& writer_;
  std::unique_ptr<WritableFile> file_;
  std::vector<WriteBuffer*> bufs_;
  uint32_t size_;
  const uint32_t max_size_;
  bool eof_;
  uint32_t disk_woff_;
  size_t buf_woff_;
  size_t buf_doff_;
  bool is_io_pending_;
};

} // namespace rocksdb
