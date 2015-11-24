#include <utility>

#include "cache/blockcache.h"

using namespace rocksdb;

//
// BlockCacheImpl
//
Status BlockCacheImpl::Open() {
  Status status;

  WriteLock _(&lock_);

  assert(!size_);

  //
  // Create directory
  //
  status = opt_.env->CreateDirIfMissing(opt_.path);
  if (!status.ok()) {
    Error(opt_.log, "Error creating directory %s. %s", opt_.path.c_str(),
          status.ToString().c_str());
    return status;
  }

  //
  // Create directory
  //
  status = opt_.env->CreateDirIfMissing(GetCachePath());
  if (!status.ok()) {
    Error(opt_.log, "Error creating directory %s. %s",
          GetCachePath().c_str(), status.ToString().c_str());
    return status;
  }

  Info(opt_.log, "Resetting directory %s", opt_.path.c_str());

  assert(!cacheFile_);
  NewCacheFile();
  assert(cacheFile_);

  return Status::OK();
}

Status BlockCacheImpl::Close() {
  // stop the writer before
  writer_.Stop();
  WriteLock _(&lock_);
  // clear all metadata
  metadata_.Clear();
  return Status::OK();
}

Status BlockCacheImpl::Insert(const Slice& key, void* buf,
                              const size_t size) {
  // pre-condition
  assert(buf);
  assert(size);
  assert(cacheFile_);

  WriteLock _(&lock_);

  LBA lba;
  if (metadata_.Lookup(key, &lba)) {
    // the key already exisits, this is duplicate insert
    return Status::OK();
  }

  while (!cacheFile_->Append(key, Slice((char*) buf, size), &lba)) {
    if (!cacheFile_->Eof()) {
      Debug(opt_.log, "Error inserting to cache file %d", cacheFile_->cacheid());
      return Status::TryAgain();
    }

    assert(cacheFile_->Eof());
    NewCacheFile();
  }

  BlockInfo* info = new BlockInfo(key, lba);
  cacheFile_->Add(info);
  bool status = metadata_.Insert(info);
  (void) status;
  assert(status);

  return Status::OK();
}

bool BlockCacheImpl::Lookup(const Slice& key, unique_ptr<char[]>* val,
                            size_t* size)
{
  ReadLock _(&lock_);

  LBA lba;
  bool status;
  status = metadata_.Lookup(key, &lba);
  if (!status) {
    Info(opt_.log, "Error looking up index for key %s", key.ToString().c_str());
    return status;
  }

  BlockCacheFile* const file = metadata_.Lookup(lba.cache_id_);
  assert(file);
  if (!file) {
    Error(opt_.log, "Error looking up cache file %d", lba.cache_id_);
    return false;
  }

  assert(file->refs_);

  unique_ptr<char[]> scratch(new char[lba.size_]);
  Slice blk_key;
  Slice blk_val;

  status = file->Read(lba, &blk_key, &blk_val, scratch.get());
  --file->refs_;
  if (!status) {
    assert(!"Unexpected error looking up cache");
    return false;
  }

  assert(blk_key == key);

  val->reset(new char[blk_val.size()]);
  memcpy(val->get(), blk_val.data(), blk_val.size());
  *size = blk_val.size();

  return true;
}

bool BlockCacheImpl::LookupKey(const Slice& key) {
  ReadLock _(&lock_);

  LBA lba;
  return metadata_.Lookup(key, &lba);
}

bool BlockCacheImpl::Erase(const Slice& key) {
  WriteLock _(&lock_);
  BlockInfo* info = metadata_.Remove(key);
  assert(info);
  delete info;
  return true;
}

void BlockCacheImpl::NewCacheFile() {
  lock_.AssertHeld();

  Info(opt_.log, "Creating cache file %d", writerCacheId_);

  writerCacheId_++;

  cacheFile_ = new WriteableCacheFile(opt_.env, bufferAllocator_, writer_,
                                      GetCachePath(), writerCacheId_,
                                      opt_.cache_file_size, opt_.log);
  bool status;
  status = cacheFile_->Create();
  assert(status);

  // insert to cache files tree
  status = metadata_.Insert(cacheFile_);
  (void) status;
  assert(status);
}

bool BlockCacheImpl::Reserve(const size_t size) {
  WriteLock _(&lock_);
  assert(size_ <= opt_.cache_size);

  if (size + size_ <= opt_.cache_size) {
    // there is enough space to write
    size_ += size;
    return true;
  }

  assert(size + size_ >= opt_.cache_size);
  // there is not enough space to fit the requested data
  // we can clear some space by evicting cold data

  while (size + size_ > opt_.cache_size * 0.9) {
    unique_ptr<BlockCacheFile> f(metadata_.Evict());
    if (!f) {
      // nothing is evictable
      return false;
    }
    assert(!f->refs_);
    size_t file_size;
    if (!f->Delete(&file_size).ok()) {
      // unable to delete file
      return false;
    }

    assert(file_size <= size_);
    size_ -= file_size;
  }

  size_ += size;
  assert(size_ <= opt_.cache_size * 0.9);
  return true;
}

//
// RocksBlockCache
//
RocksBlockCache::RocksBlockCache(const std::shared_ptr<BlockCacheImpl>& impl) {
  cache_ = impl;
}

RocksBlockCache::RocksBlockCache(Env* env, const std::string path) {
  std::shared_ptr<Logger> log;
  Status s = env->NewLogger(path + "/cache.log", &log);
  assert(s.ok());
  BlockCacheOptions opt(env, path, /*size=*/ UINT64_MAX, log);
  cache_.reset(new BlockCacheImpl(opt));
  assert(cache_->Open().ok());
}

RocksBlockCache::~RocksBlockCache() {
  cache_->Close();
}

RocksBlockCache::Handle* RocksBlockCache::Insert(const Slice& key, void* value,
                                                 const size_t size,
                                                 void (*deleter)(const Slice&, void*)) {
  return new DataHandle(key, (char*) value, size, deleter);
}

Cache::Handle* RocksBlockCache::InsertBlock(const Slice& key, Block* block,
                                            void (*deleter)(const Slice&, void*)) {
  assert(cache_);
  assert(block);

  // At this point we don't support hash index or prefix index
  assert(!block->HasIndex());
  assert(block->compression_type() == kNoCompression);
  assert(block->size());

  if (!cache_->Insert(key, (void*) block->data(), block->size()).ok()) {
    return nullptr;
  }

  return new BlockHandle(key, block,  deleter);
}

Cache::Handle* RocksBlockCache::Lookup(const Slice& key) {
  assert(cache_);

  unique_ptr<char[]> data;
  size_t size;
  if (!cache_->Lookup(key, &data, &size)) {
    return nullptr;
  }

  Block* block = Block::NewBlock(std::move(data), size);
  assert(block->size() == size);
  auto* h = new BlockHandle(key, block);
  return h;
}

void RocksBlockCache::Release(Cache::Handle* handle) {
  assert(handle);
  HandleBase* h = (HandleBase*) handle;
  delete h;
}

void* RocksBlockCache::Value(Cache::Handle* handle) {
  assert(handle);
  return ((HandleBase*) handle)->value();
}
