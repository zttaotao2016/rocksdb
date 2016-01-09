#include <iostream>
#include "cache/cache_volatile.h"

using namespace rocksdb;

void VolatileCache::DeleteCacheData(VolatileCache::CacheData* data) {
  assert(data);
  delete data;
}

//
// VolatileCache implementation
//
VolatileCache::~VolatileCache() {
  index_.Clear(&DeleteCacheData);
}

Status VolatileCache::Insert(const Slice& page_key, const void* data,
                             const size_t size) {
  // precondition
  assert(data);
  assert(size);

  // clear up space for insertion
  size_ += size;
  while (size_ > max_size_) {
    // TODO: Replace this with condition variable
    Evict();
  }

  // insert order: LRU, followed by index
  std::string key = std::move(page_key.ToString());
  std::string value(reinterpret_cast<const char*>(data), size);
  std::unique_ptr<CacheData> cache_data(
    new CacheData(std::move(key), std::move(value)));
  bool status = index_.Insert(cache_data.get());
  if (status) {
    cache_data.release();
    return Status::OK();
  }

  // failed to insert to cache, block already in cache
  return Status::TryAgain("key already exists in volatile cache");
}

Status VolatileCache::Lookup(const Slice& page_key,
                             std::unique_ptr<char[]>* result,
                             size_t* size) {
  CacheData key(std::move(page_key.ToString()));
  CacheData* kv;
  bool status = index_.Find(&key, &kv);
  if (status) {
    // set return data
    result->reset(new char[kv->value.size()]);
    memcpy(result->get(), kv->value.c_str(), kv->value.size());
    *size = kv->value.size();
    // drop the reference on cache data
    kv->refs_--;
    return Status::OK();
  }

  if (next_tier_) {
    return next_tier_->Lookup(page_key, result, size);
  }

  return Status::NotFound("key not found in volatile cache");
}


bool VolatileCache::Erase(const Slice& key) {
  assert(!"not supported");
  return true;
}

/*
 * private member functions
 */
bool VolatileCache::Evict() {
  CacheData* edata = index_.Evict();
  if (!edata) {
    // not able to evict any object
    return false;
  }

  // push the evicted object to the next level
  if (next_tier_) {
    next_tier_->Insert(Slice(edata->key), edata->value.c_str(),
                       edata->value.size());
  }

  // adjust size and destroy data
  size_ -= edata->value.size();
  delete edata;

  return true;
}
