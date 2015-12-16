#include <iostream>
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
  assert(status);
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
  {
    // Debug check to make sure that the block already in cache is identical to
    // the one we were asked to insert
    CacheObject lookup_key(key);
    if (index_.Find(&lookup_key, &obj)) {
      // key already exists in the system
      assert(obj->Key() == key.ToString());
      assert(obj->Size() == block->size());
      Block* obj_block = (Block*) obj->Value();
      assert(memcmp(obj_block->data(), block->data(), obj->Size()) == 0);
      assert(obj->refs_);
      --obj->refs_;
    }
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
    bool status = index_.Find(&lookup_key, &obj);
    if (status) {
      assert(obj->refs_);
      stats_.cache_hits_++;
      return obj;
    }
  }

  assert(!obj);
  if (next_tier_) {
    // lookup in the secondary cache;
    std::unique_ptr<char[]> data;
    size_t size;
    if (!next_tier_->Lookup(key, &data, &size).ok()) {
      // data is not there in the secondary tier
      stats_.cache_misses_++;
      return nullptr;
    }

    assert(data);
    assert(size);

    // Insert the data to the primary cache and return result
    Block *block = Block::NewBlock(std::move(data), size);
    assert(block);
    assert(block->size() == size);
    stats_.cache_hits_++;
    return InsertBlock(key, block, &DeleteBlock);
  }

  stats_.cache_misses_++;
  return nullptr;
}

void VolatileCache::Erase(const Slice& key) {
  assert(!"not supported");
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
bool VolatileCache::Evict() {
  CacheObject* obj = index_.Evict();
  if (!obj) {
    return false;
  }

  if (next_tier_ && obj->Serializable()) {
    Block* block = (Block*) obj->Value();
    assert(block);
    next_tier_->Insert(obj->Key(), (void*) block->data(),
                       static_cast<uint32_t>(block->size()));
  }

  assert(size_ >= obj->Size());
  size_ -= obj->Size();
  delete obj;

  return true;
}
