#include "cache/cache_volatile.h"

using namespace rocksdb;

//
// VolatileCache implementation
//
VolatileCache::~VolatileCache() {
  index_.Clear(&DeleteCacheObj);
}

Cache::Handle* VolatileCache::Insert(const Slice& key, void* value,
                                     size_t charge, deleter_t deleter) {
  while (size_ + charge > max_size_) {
    // TODO: Replace this with condition variable
    Evict();
  }

  // pre-condition
  assert(value);
  assert(charge);
  assert(deleter);

  CacheObject* obj = new PtrRef(key, value, charge, deleter);
  assert(obj);

  // inc ref count
  assert(!obj->refs_);
  ++obj->refs_;

  //insert order: LRU, followed by index
  bool status = index_.Insert(obj);
  if (status) {
    size_ += charge;
    return obj;
  }

  // unable to insert, data already exisits in the cache
  assert(!status);
  --obj->refs_;
  assert(!obj->refs_);

  return obj;
}


Cache::Handle* VolatileCache::InsertBlock(const Slice& key, Block* block,
                                         deleter_t deleter) {
  while (size_ + block->size() > max_size_) {
    // TODO: Replace this with condition variable
    Evict();
  }

  CacheObject* obj = nullptr;
#ifndef NDEBUG
  // Debug check to make sure that the block already in cache is identical to
  // the one we were asked to insert
  CacheObject lookup_key(key);
  if (index_.Find(&lookup_key, &obj)) {
    // key already exists in the system
    assert(obj->Size() == block->size());
    assert(memcmp(obj->Value(), block->data(), obj->Size()) == 0);
    ++obj->refs_;
    return obj;
  }
#endif
  // pre-condition
  assert(block);
  assert(block->data());
  assert(block->size());
  assert(deleter);

  // allocate
  obj = new BlockData(key, block, deleter);
  assert(obj);

  // inc ref
  assert(!obj->refs_);
  ++obj->refs_;

  // insert order: LRU, followed by index
  bool status = index_.Insert(obj);
  if (status) {
    size_ += block->size();
    return obj;
  }

  // failed to insert to cache, block already in cache
  // decrement the ref and return the handle
  assert(!status);
  --obj->refs_;
  assert(!obj->refs_);

  return obj;
}

static void DeleteBlock(const Slice&, void* data) {
  Block* block = (Block*) data;
  delete block;
}

Cache::Handle* VolatileCache::Lookup(const Slice& key) {
  CacheObject* obj = nullptr;
  {
    CacheObject lookup_key(key);
    ReadLock _(index_.GetMutex(&lookup_key));
    bool status = index_.Find(&lookup_key, &obj);
    if (status) {
      // inc ref
      ++obj->refs_;
      return obj;
    }
  }


  assert(!obj);
  if (next_tier_) {
    // lookup in the secondary cache;
    std::unique_ptr<char[]> data;
    uint32_t size;
    if (!next_tier_->Lookup(key, &data, &size)) {
      // data is not there in the secondary tier
      return nullptr;
    }

    assert(data);
    assert(size);

    // Insert the data to the primary cache and return result
    Block *block = Block::NewBlock(std::move(data), size);
    assert(block);
    assert(block->size() == size);
    return InsertBlock(key, block, &DeleteBlock);
  }

  return nullptr;
}

void VolatileCache::Erase(const Slice& key) {
  // erase from index
  CacheObject* obj = EraseFromIndex(key);
  assert(obj);
  if (!obj) {
    return;
  }

  assert(size_ >= obj->Size());
  size_ -= obj->Size();
  delete obj;
}

void VolatileCache::Release(Cache::Handle* handle) {
  assert(handle);
  auto* obj = (CacheObject*) handle;
  if (!obj->refs_) {
    // this was a handle that does not have identity in the cache
    // we need to destruct
    delete obj;
  } else {
    --obj->refs_;
  }
}

void* VolatileCache::Value(Cache::Handle* handle) {
  assert(handle);
  auto* obj = (CacheObject*) handle;
  return (void*) obj->Value();
}

/*
 * private member functions
 */
VolatileCache::CacheObject* VolatileCache::EraseFromIndex(const Slice& key) {
  CacheObject lookup_key(key);
  CacheObject* obj = nullptr;
  bool status = index_.Erase(&lookup_key, &obj);
  assert(status);
  assert(obj);

  if (status && obj) {
    assert(!obj->refs_);
  }
  return obj;
}

bool VolatileCache::Evict() {
  CacheObject* obj = index_.Evict();
  if (!obj) {
    return false;
  }

  if (next_tier_ && obj->Serializable()) {
    Block* block = (Block*) obj->Value();
    assert(block);
    if (!next_tier_->LookupKey(obj->Key())) {
      // insert only if the key does not already exists
      // This scenario can manifest since we insert to volatile cache after
      // reading from secondary cache
      next_tier_->Insert(obj->Key(), (void*) block->data(), block->size());
    }
  }

  assert(size_ >= obj->Size());
  size_ -= obj->Size();
  delete obj;

  return true;
}
