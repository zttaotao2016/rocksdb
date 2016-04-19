// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <memory>
#include <stdint.h>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/statistics.h"

namespace rocksdb {

// PageCache
//
// Page cache interface for caching IO pages from the storage medium
class PageCache {
 public:
  enum class Type {
    // Cache blocks from device as-is (compressed/uncompressed)
    RAW,
    // Cache decompressed pages
    UNCOMPRESSED
  };

  virtual ~PageCache() {}

  // Insert to page cache
  //
  // page_key   Identifier to identify a page uniquely across restarts
  // data       Page data
  // size       Size of the page
  virtual Status Insert(const Slice& page_key, const void* data,
                        const size_t size) = 0;

  // Lookup page cache by page identifier
  //
  // page_key   Page identifier
  // buf        Buffer where the data should be copied
  // size       Size of the page
  virtual Status Lookup(const Slice& page_key, std::unique_ptr<char[]>* data,
                        size_t* size) = 0;

  void EnableRawCache() {
    type_ = Type::RAW;
  }

  const Type& type() const {
    return type_;
  }

 protected:
  Type type_ = Type::UNCOMPRESSED;
};

}  // namespace rocksdb
