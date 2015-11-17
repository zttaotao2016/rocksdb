#include "cache/cache_volatile.h"

using namespace rocksdb;

VolatileCache::~VolatileCache() {
  WriteLock _(&rwlock_);
  // TODO: Super ugly way to clear data, find a better way
  while (!lru_list_.IsEmpty()) {
    lru_list_.Pop();
  }
  index_.Clear(&DeleteCacheObj);
}

Cache::Handle* VolatileCache::Insert(const Slice& key, void* value,
                                     size_t charge, deleter_t deleter) {
  WriteLock _(&rwlock_);

  while (size_ + charge > max_size_) {
    // TODO: Replace this with condition variable
    Evict();
  }

  // pre-condition
  assert(value);
  assert(charge);
  assert(deleter);

  // allocate
  CacheObject* obj = new PtrRef(key, value, charge, deleter);
  assert(obj);

  // inc ref count
  assert(!obj->refs_);
  ++obj->refs_;

  //insert order: LRU, followed by index
  lru_list_.Push(obj);
  bool status = index_.Insert(obj);
  assert(status);
  (void) status;

  size_ += charge;

  return obj;
}

Cache::Handle* VolatileCache::InsertBlock(const Slice& key, Block* block,
                                         deleter_t deleter) {
  WriteLock _(&rwlock_);

  while (size_ + block->size() > max_size_) {
    // TODO: Replace this with condition variable
    Evict();
  }

  // pre-condition
  assert(block);
  assert(block->data());
  assert(block->size());
  assert(deleter);

  // allocate
  CacheObject* obj = new BlockData(key, block, deleter);
  assert(obj);

  // inc ref
  assert(!obj->refs_);
  ++obj->refs_;

  // insert order: LRU, followed by index
  lru_list_.Push(obj);
  bool status = index_.Insert(obj);
  assert(status);
  (void) status;

  size_ += block->size();

  return obj;
}

static void DeleteBlock(const Slice&, void* data) {
  Block* block = (Block*) data;
  delete block;
}

#include <iostream>
Cache::Handle* VolatileCache::Lookup(const Slice& key) {
  CacheObject* obj = nullptr;

  {
    ReadLock _(&rwlock_);

    // lookup in cache
    CacheObject lookup_key(key);
    bool status = index_.Find(&lookup_key, &obj);
    if (status) {
      // inc ref
      ++obj->refs_;
      // Touch in LRU
      lru_list_.Touch(obj);
      return obj;
    }

    // data is not found
    assert(!status);
  }

  if (!obj && next_tier_) {
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
  WriteLock _(&rwlock_);

  // erase from index
  CacheObject* obj = EraseFromIndex(key);
  assert(obj);
  if (!obj) {
    return;
  }

  // erase from LRU
  lru_list_.Unlink(obj);

  assert(size_ >= obj->Size());
  size_ -= obj->Size();
  delete obj;
}

void VolatileCache::Release(Cache::Handle* handle) {
  assert(handle);
  auto* obj = (CacheObject*) handle;
  assert(obj->refs_);
  --obj->refs_;
}

void* VolatileCache::Value(Cache::Handle* handle) {
  assert(handle);
  auto* obj = (CacheObject*) handle;
  assert(obj->refs_);
  return (void*) obj->Value();
}

/*
 * private member functions
 */
VolatileCache::CacheObject* VolatileCache::EraseFromIndex(const Slice& key) {
  rwlock_.AssertHeld();

  CacheObject lookup_key(key);
  CacheObject* obj = nullptr;
  bool status = index_.Erase(&lookup_key, &obj);
  assert(status);
  assert(obj);
  assert(!obj->refs_);
  return obj;
}

bool VolatileCache::Evict() {
  rwlock_.AssertHeld();

  if (lru_list_.IsEmpty()) {
    return false;
  }

  CacheObject* obj = lru_list_.Pop();
  assert(obj);
  if (!obj) {
    return false;
  }

  CacheObject* ret = EraseFromIndex(obj->Key());
  assert(ret == obj);

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

  size_ -= obj->Size();
  delete obj;

  return true;
}
