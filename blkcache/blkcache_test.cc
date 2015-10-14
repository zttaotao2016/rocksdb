//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "blkcache/blkcache.h"
#include "util/testharness.h"
#include "util/arena.h"

using namespace std;

namespace rocksdb {

class ConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::INFO_LEVEL) {}

  void Logv(const char* format, va_list ap) override {
    MutexLock _(&lock_);
    vprintf(format, ap);
    printf("\n");
  }

  port::Mutex lock_;
};

class NVMBlockCacheTest : public testing::Test {
 public:

  NVMBlockCacheTest()
      : env_(Env::Default()),
        path_(test::TmpDir(env_) + "/nvm_block_cache_test") {

    Status s;

    NVMBlockCache::Options opt;

    opt.writeBufferSize = 1 * 1024 * 1024;
    opt.writeBufferCount = 100;
    opt.maxCacheFileSize =  12 * 1024* 1024;
    opt.max_bufferpool_size_ = opt.writeBufferSize * opt.writeBufferCount;

    opt.path = path_;
    auto logfile = path_ + "/nvm_block_cache_test.log";
    log_.reset(new ConsoleLogger());
    // s = env_->NewLogger(logfile, &log_);
    opt.info_log = log_;
    assert(s.ok());

    Log(InfoLogLevel::INFO_LEVEL, log_, "Test output directory %s",
        opt.path.c_str());

    cache_.reset(new NVMBlockCache(env_, opt));

    s = cache_->Open();
    assert(s.ok());
  }

  virtual ~NVMBlockCacheTest() {
    Status s = cache_->Close();
    assert(s.ok());

    log_->Flush();
  }

 protected:

  std::unique_ptr<NVMBlockCache> cache_;
  Arena arena_;
  Env* env_;
  const std::string path_;
  shared_ptr<Logger> log_;
};

TEST_F(NVMBlockCacheTest, Insert) {
  const string prefix = "key_prefix_";

  NVMBlockCache::LBA lba;

  const size_t max_keys = 10 * 1024;

  for (size_t i = 0; i < max_keys; i++) {
    static char data[4 * 1024];
    memset(data, '0' + (i % 10), sizeof(data));
    auto k = prefix + std::to_string(i);
    Slice key(k);
    while(!cache_->Insert(key, data, sizeof(data), &lba).ok());
  }

  for (size_t i = 0; i <  max_keys; i++) {
    static char edata[4 * 1024];
    memset(edata, '0' + (i % 10), sizeof(edata));
    auto k = prefix + std::to_string(i);
    Slice key(k);
    unique_ptr<char> block;
    uint32_t block_size;
    ASSERT_TRUE(cache_->Lookup(key, &block, &block_size));
    ASSERT_TRUE(block_size == sizeof(edata));
    ASSERT_TRUE(memcmp(edata, block.get(), sizeof(edata)) == 0);
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
