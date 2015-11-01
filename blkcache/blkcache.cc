#include <utility>

#include "blkcache/blkcache.h"

using namespace rocksdb;
using std::unique_ptr;

Status BlockCacheImpl::Open() {
  Status status;

  WriteLock _(&lock_);

  assert(!size_);

  //
  // Create directory
  //
  status = env_->CreateDirIfMissing(opt_.path);
  if (!status.ok()) {
    Error(opt_.info_log, "Error creating directory %s. %s", opt_.path.c_str(),
          status.ToString().c_str());
    return status;
  }

  //
  // Create directory
  //
  status = env_->CreateDirIfMissing(GetCachePath());
  if (!status.ok()) {
    Error(opt_.info_log, "Error creating directory %s. %s",
          GetCachePath().c_str(), status.ToString().c_str());
    return status;
  }

  Info(opt_.info_log, "Resetting directory %s", opt_.path.c_str());

  assert(!cacheFile_);
  NewCacheFile();
  assert(cacheFile_);

  return Status::OK();
}

Status BlockCacheImpl::Close() {
  WriteLock _(&lock_);
  writer_.Stop();
  return Status::OK();
}

Status BlockCacheImpl::Insert(const Slice& key, void* buf, const uint16_t size,
                             LBA* lba) {
  // pre-condition
  assert(buf);
  assert(size);
  assert(lba);
  assert(cacheFile_);

  WriteLock _(&lock_);

  while (!cacheFile_->Append(key, Slice((char*) buf, size), lba)) {
    if (!cacheFile_->Eof()) {
      Debug(log_, "Error inserting to cache file %d", cacheFile_->cacheid());
      return Status::TryAgain();
    }

    assert(cacheFile_->Eof());
    NewCacheFile();
  }

  BlockInfo* info = new BlockInfo(key, *lba);
  cacheFile_->Add(info);
  bool ok = metadata_.Insert(info);
  assert(ok);

  return Status::OK();
}

bool BlockCacheImpl::Lookup(const Slice& key, unique_ptr<char>* val,
                           uint32_t* size)
{
  ReadLock _(&lock_);

  LBA lba;
  bool status = metadata_.Lookup(key, &lba);
  if (!status) {
    Error(log_, "Error looking up index for key %s", key.ToString().c_str());
    return status;
  }

  BlockCacheFile* const file = metadata_.Lookup(lba.cache_id_);
  assert(file);
  if (!file) {
    Error(log_, "Error looking up cache file %d", lba.cache_id_);
    return false;
  }

  unique_ptr<char> scratch(new char[lba.size_]);
  Slice blk_key;
  Slice blk_val;
  if (!file->Read(lba, &blk_key, &blk_val, scratch.get())) {
    assert(!"Unexpected error looking up cache");
    Error(log_, "Error looking up cache %d key %s", file->cacheid(),
          key.ToString().c_str());
    return false;
  }

  assert(blk_key == key);

  val->reset(new char[blk_val.size()]);
  memcpy(val->get(), blk_val.data(), blk_val.size());
  *size = blk_val.size();

  return true;
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

  Info(log_, "Creating cache file %d", writerCacheId_);

  writerCacheId_++;

  cacheFile_ = new WriteableCacheFile(env_, bufferAllocator_, writer_,
                                      GetCachePath(), writerCacheId_,
                                      opt_.maxCacheFileSize, log_);
  assert(cacheFile_->Create());

  // insert to cache files tree
  bool status = metadata_.Insert(cacheFile_);
  assert(status);
}

bool BlockCacheImpl::Reserve(const size_t size) {
  WriteLock _(&lock_);
  assert(size_ <= opt_.max_size_);

  if (size + size_ <= opt_.max_size_) {
    // there is enough space to write
    size_ += size;
    return true;
  }

  assert(!"here");

  assert(size + size_ >= opt_.max_size_);
  // there is not enough space to fit the requested data
  // we can clear some space by evicting cold data

  while (size + size_ > opt_.max_size_ * 0.9) {
    unique_ptr<BlockCacheFile> f(metadata_.Evict());
    if (!f) {
      // nothing is evictable
      return false;
    }
    assert(f->evictable_);
    size_t file_size;
    if (!f->Delete(&file_size).ok()) {
      // unable to delete file
      return false;
    }

    assert(file_size <= size_);
    size_ -= file_size;
  }

  size_ += size;
  assert(size_ <= opt_.max_size_ * 0.9);
  return true;
}

//
// RocksBlockCache
//
RocksBlockCache::RocksBlockCache(Env* env,
                                 const std::string path) {
  BlockCacheImpl::Options opt;

  env->NewLogger(path + "/cache.log", &opt.info_log);
  opt.path = path;

  cache_.reset(new BlockCacheImpl(env, opt));
  assert(cache_->Open().ok());
}

RocksBlockCache::~RocksBlockCache() {
  cache_->Close();
}

Cache::Handle* RocksBlockCache::Insert(const Slice& key, void* value,
                                const size_t size,
                                void (*deleter)(const Slice&, void*)) {
  assert(cache_);
  // assert(!deleter);

  LBA lba;
  if (!cache_->Insert(key, value, size, &lba).ok()) {
    Error(log_, "Error inserting to cache. key=%s", key.ToString().c_str());
    return nullptr;
  }

  auto* ret = new BlkCacheHandle();
  ret->Assign(key, (char*) value, size);

  return ret;
}

Cache::Handle* RocksBlockCache::Lookup(const Slice& key) {
  assert(cache_);

  unique_ptr<char> data;
  uint32_t size;
  if (!cache_->Lookup(key, &data, &size)) {
    Error(log_, "Error looking up key %s", key.ToString().c_str());
    return nullptr;
  }

  auto* ret = new BlkCacheHandle();
  ret->Copy(key, data.get(), size);

  data.release();

  return ret;
}

void RocksBlockCache::Release(Cache::Handle* handle) {
  assert(handle);
  delete (BlkCacheHandle*) handle;
}

void* RocksBlockCache::Value(Cache::Handle* handle) {
  assert(handle);
  return ((BlkCacheHandle*) handle)->data_;
}


 

