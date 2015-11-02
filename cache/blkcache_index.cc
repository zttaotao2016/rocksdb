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

  *lba = ret->lba_;
  return true;
}

BlockInfo* SimpleBlockLookupIndex::Remove(const Slice& key) {
  BlockInfo n(key);
  BlockInfo* ret;
  bool status = index_.Erase(&n, &ret);
  assert(status);
  return ret;
}

void SimpleBlockLookupIndex::RemoveAllKeys(BlockCacheFile* f) {
  for (BlockInfo* binfo : f->block_infos()) {
    bool status = index_.Erase(binfo, nullptr);
    assert(status);
  }
}
