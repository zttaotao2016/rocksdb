#pragma once

#include "cache/cache_tier.h"

namespace rocksdb {

/**
 * SecondaryTier implementation does not adhere to the cache interface. It is
 * not possible to plug in a secondary cache implementation into RocksDB for
 * testing.
 *
 * PrimaryTierCloak provides wrapper implementation to make a secondary cache
 * implementation look like a primary cache
 */
class SecondaryCacheTierCloak : public PrimaryCacheTier {
 public:
  explicit SecondaryCacheTierCloak(std::unique_ptr<SecondaryCacheTier>&& cache)
    : cache_(std::move(cache)) {}

  virtual ~SecondaryCacheTierCloak() {}

  /*
   * Handle abstraction to support raw pointers and blocks
   */
  typedef void (*deleter_t)(const Slice&, void*);

  struct HandleBase : Handle {
    explicit HandleBase(const Slice& key, const size_t size,
                        deleter_t deleter)
      : key_(std::move(key.ToString())),
        size_(size),
        deleter_(deleter) {
    }

    virtual ~HandleBase() {}

    virtual void* value() = 0;

    std::string key_;
    const size_t size_ = 0;
    deleter_t deleter_ = nullptr;
  };

  /*
   * Handle for raw pointers
   */
  struct DataHandle : HandleBase
  {
    explicit DataHandle(const Slice& key, char* const data = nullptr,
                        const size_t size = 0,
                        const deleter_t deleter = nullptr)
      : HandleBase(key, size, deleter)
      , data_(data) {}

    virtual ~DataHandle() {
      assert(deleter_);
      (*deleter_)(key_, data_);
    }

    void* value() override { return data_; }

    char* data_ = nullptr;
  };

  /*
   * Handle for blocks
   */
  struct BlockHandle : HandleBase
  {
    explicit BlockHandle(const Slice& key, Block* const block,
                         const deleter_t deleter = nullptr)
      : HandleBase(key, block->size(), deleter)
      , block_(block) {
      assert(block);
    }

    virtual ~BlockHandle() {
      if (deleter_) {
        (*deleter_)(key_, block_);
      } else {
        delete block_;
      }
    }

    void* value() override { return block_; }

    Block* block_ = nullptr;
  };

  // Pretend like inserting
  Handle* Insert(const Slice& key, void* value, const size_t size,
                 deleter_t deleter) {
    return new DataHandle(key, (char*) value, size, deleter);
  }

  // Insert data to secondary tier, but return fake handle
  Handle* InsertBlock(const Slice& key, Block* block, deleter_t deleter) {
    assert(cache_);
    assert(block);

    // At this point we don't support hash index or prefix index
    assert(!block->HasIndex());
    assert(block->compression_type() == kNoCompression);
    assert(block->size());

    if (!cache_->Insert(key, (void*) block->data(), block->size()).ok()) {
      return nullptr;
    }

    return new BlockHandle(key, block,  deleter);
  }

  // Lookup a key. Fetch from secondary cache and reconstruct block
  Handle* Lookup(const Slice& key) {
    assert(cache_);

    unique_ptr<char[]> data;
    size_t size;
    if (!cache_->Lookup(key, &data, &size)) {
      return nullptr;
    }

    Block* block = Block::NewBlock(std::move(data), size);
    assert(block->size() == size);
    auto* h = new BlockHandle(key, block);
    return h;
  }

  // delete the returned handle
  void Release(Cache::Handle* handle) {
    assert(handle);
    HandleBase* h = (HandleBase*) handle;
    delete h;
  }

  // Fetch value from handle based on the type
  void* Value(Cache::Handle* handle) {
    assert(handle);
    return ((HandleBase*) handle)->value();
  }

  // Erase key from secondary tier
  void Erase(const Slice& key) override {
    cache_->Erase(key);
  }

  Status Close() override {
    cache_->Close();
    return Status::OK();
  }

  // Return the size of the handle
  size_t GetUsage(Handle* handle) const override {
    return ((HandleBase*) handle)->size_;
  }

  // Not supported. Secondary cache does not support this
  uint64_t NewId() override { assert(!"not supported"); }

  void SetCapacity(size_t capacity) override {}
  size_t GetCapacity() const override { return 0; }

  // Not implemented override
  size_t GetUsage() const override { assert(!"not implemented"); }
  size_t GetPinnedUsage() const override { assert(!"not implemented"); }
  void ApplyToAllCacheEntries(void (*)(void*, size_t), bool) override {
    assert(!"not implemented");
  }

  // Print Stats
  std::string PrintStats() override {
    return cache_->PrintStats();
  }

  //
  // Test only functionalities
  //
  void Flush_TEST() override {
    cache_->Flush_TEST();
  }

 private:
  std::unique_ptr<SecondaryCacheTier> cache_;
};

}  // namespace rocksdb
