//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "cache/blockcache.h"
#include "util/testharness.h"
#include "util/arena.h"
#include "db/db_test_util.h"

using namespace std;

namespace rocksdb {

struct HashTableTest : public testing::Test {
  struct Node {
    Node(const uint64_t key = 0, const std::string& val = std::string())
      : key_(key), val_(val) {}

    uint64_t key_;
    std::string val_;
  };

  struct Equal {
    bool operator()(const Node& lhs, const Node& rhs) {
      return lhs.key_ == rhs.key_;
    }
  };

  struct Hash {
    uint64_t operator()(const Node& node) {
      return std::hash<uint64_t>()(node.key_);
    }
  };

  HashTable<Node, Hash, Equal> map_;
};

TEST_F(HashTableTest, TestInsert) {
  const uint64_t max_keys = 1024 * 1024;

  // insert
  for (uint64_t k = 0; k < max_keys; ++k) {
    map_.Insert(Node(k, std::string(1000, k % 255)));
  }

  // verify
  for (uint64_t k = 0; k < max_keys; ++k) {
    Node node(k, std::string()), val;
    port::RWMutex* rlock;
    assert(map_.Find(k, &val, &rlock));
    rlock->ReadUnlock();
    assert(val.val_ == std::string(1000, k % 255));
  }
}

TEST_F(HashTableTest, TestErase) {
  const uint64_t max_keys = 1024 * 1024;
  // insert
  for (uint64_t k = 0; k < max_keys; ++k) {
    map_.Insert(Node(k, std::string(1000, k % 255)));
  }

  // erase a few keys randomly
  std::set<uint64_t> erased;
  for (int i = 0; i < 1024; ++i) {
    uint64_t k = random() % max_keys;
    if (erased.find(k) != erased.end()) {
      continue;
    }
    assert(map_.Erase(k, /*ret=*/ nullptr));
    erased.insert(k);
  }

  // verify
  for (uint64_t k = 0; k < max_keys; ++k) {
    Node node(k, std::string()), val;
    port::RWMutex* rlock = nullptr;
    bool status = map_.Find(k, &val, &rlock);
    if (erased.find(k) == erased.end()) {
      assert(status);
      rlock->ReadUnlock();
      assert(val.val_ == std::string(1000, k % 255));
    } else {
      assert(!status);
    }
  }
}


}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
