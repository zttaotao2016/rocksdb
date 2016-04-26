#include <functional>

#include "cache/blockcache_metadata.h"

using namespace rocksdb;

bool BlockCacheMetadata::Insert(BlockCacheFile* file) {
  return cache_file_index_.Insert(file);
}

BlockCacheFile* BlockCacheMetadata::Lookup(const uint32_t cache_id) {
  BlockCacheFile* ret = nullptr;
  BlockCacheFile lookup_key(cache_id);
  bool status = cache_file_index_.Find(&lookup_key, &ret);
  if (status) {
    assert(ret->refs_);
    return ret;
  }
  return nullptr;
}

BlockCacheFile* BlockCacheMetadata::Evict() {
  using std::placeholders::_1;
  auto fn = std::bind(&BlockCacheMetadata::RemoveAllKeys, this, _1);
  return cache_file_index_.Evict(fn);
}

void BlockCacheMetadata::Clear() {
  cache_file_index_.Clear(&DeleteBlockCacheFile);
  block_index_.Clear(&DeleteBlockInfo);
}

bool BlockCacheMetadata::Insert(BlockInfo* binfo) {
  return block_index_.Insert(binfo);
}

bool BlockCacheMetadata::Lookup(const Slice& key, LBA* lba) {
  BlockInfo lookup_key(key);
  BlockInfo* block;
  port::RWMutex* rlock = nullptr;
  if (!block_index_.Find(&lookup_key, &block, &rlock)) {
    return false;
  }

  ReadUnlock _(rlock);
  assert(block->key_ == key.ToString());
  if (lba) {
    *lba = block->lba_;
  }
  return true;
}

BlockInfo* BlockCacheMetadata::Remove(const Slice& key) {
  BlockInfo lookup_key(key);
  BlockInfo* binfo = nullptr;
  bool status = block_index_.Erase(&lookup_key, &binfo);
  (void) status;
  assert(status);
  return binfo;
}

void BlockCacheMetadata::RemoveAllKeys(BlockCacheFile* f) {
  for (BlockInfo* binfo : f->block_infos()) {
    BlockInfo* tmp = nullptr;
    bool status = block_index_.Erase(binfo, &tmp);
    (void) status;
    assert(status);
    assert(tmp == binfo);
    delete binfo;
  }
  f->block_infos().clear();
}
