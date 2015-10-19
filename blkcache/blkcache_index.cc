#include <blkcache/blkcache_index.h>

using namespace rocksdb;

bool SimpleBlockLookupIndex::Insert(const Slice& key, const LBA& lba) {
  WriteLock _(&rwlock_);
  auto status = index_.insert(std::make_pair(key, lba));
  assert(status.second);
  return status.second;
}

bool SimpleBlockLookupIndex::Lookup(const Slice& key, LBA* lba) {
  ReadLock _(&rwlock_);

  auto it = index_.find(key);
  if (it == index_.end()) {
    return false;
  }

  assert(key == it->first);
  *lba = it->second;

  return true;
}

bool SimpleBlockLookupIndex::Remove(const Slice& key) {
  WriteLock _(&rwlock_);
  const size_t count = index_.erase(key);
  assert(count <= 1);
  return count;
}


