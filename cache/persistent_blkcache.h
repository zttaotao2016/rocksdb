#pragma once

#include <iostream>
#include <memory>
#include "include/rocksdb/status.h"

namespace rocksdb {

struct LogicalBlockAddress {
  LogicalBlockAddress() {}
  LogicalBlockAddress(const uint32_t cache_id, const uint32_t off,
                      const uint16_t size)
      : cache_id_(cache_id), off_(off), size_(size) {}

  uint32_t cache_id_ = 0;
  uint32_t off_ = 0;
  uint16_t size_ = 0;
};

typedef LogicalBlockAddress LBA;

class PersistentBlockCache {
 public:
  typedef LogicalBlockAddress LBA;

  virtual ~PersistentBlockCache() {}

  virtual Status Open() = 0;

  virtual Status Close() = 0;

  virtual Status Insert(const Slice& key, void* data, const uint32_t size,
                        LBA* lba) = 0;

  virtual bool Lookup(const Slice & key, std::unique_ptr<char[]>* val,
                      uint32_t* size) = 0;

  virtual bool Erase(const Slice& key) = 0;

  virtual bool Reserve(const size_t size) = 0;
};


}  // namespace rocksdb
