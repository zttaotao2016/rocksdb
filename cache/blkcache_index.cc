#include <cache/blkcache_index.h>

using namespace rocksdb;

bool SimpleBlockLookupIndex::Insert(BlockInfo* binfo) {
  index_.Insert(binfo);
  return true;
}

bool SimpleBlockLookupIndex::Lookup(const Slice& key, LBA* lba) {
  BlockInfo n(key);
  ReadLock _(index_.GetMutex(&n));
  BlockInfo* ret;
  if (!index_.Find(&n, &ret)) {
    return false;
  }

  assert(ret->key_size_ == key.size());
  assert(memcmp(ret->key_, key.data(), key.size()) == 0);
  *lba = ret->lba_;
  return true;
}

BlockInfo* SimpleBlockLookupIndex::Remove(const Slice& key) {
  BlockInfo n(key);
  BlockInfo* ret = nullptr;
  bool status = index_.Erase(&n, &ret);
  (void) status;
  assert(status);
  return ret;
}

void SimpleBlockLookupIndex::RemoveAllKeys(BlockCacheFile* f) {
  for (BlockInfo* binfo : f->block_infos()) {
    bool status = index_.Erase(binfo, nullptr);
    (void) status;
    assert(status);
  }
}
