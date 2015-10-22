#include <blkcache/blkcache_index.h>

using namespace rocksdb;

bool SimpleBlockLookupIndex::Insert(BlockInfo* binfo) {
  index_.Insert(binfo);
  return true;
}

bool SimpleBlockLookupIndex::Lookup(const Slice& key, LBA* lba) {
  BlockInfo n(key);
  ReadLock _(index_.GetMutex(&n));
  auto* ret = index_.Find(&n);
  if (!ret) {
    return false;
  }

  *lba = ret->lba_;
  return true;
}

BlockInfo* SimpleBlockLookupIndex::Remove(const Slice& key) {
  BlockInfo n(key);
  BlockInfo* ret = index_.Erase(&n);
  assert(ret);
  return ret;
}


