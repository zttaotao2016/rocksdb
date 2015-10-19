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
#include "db/db_test_util.h"

using namespace std;

namespace rocksdb {

class ConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::DEBUG_LEVEL) {}

  void Logv(const char* format, va_list ap) override {
    MutexLock _(&lock_);
    vprintf(format, ap);
    printf("\n");
  }

  port::Mutex lock_;
};

class BlockCacheImplTest : public testing::Test {
 public:

  BlockCacheImplTest()
      : env_(Env::Default()),
        path_(test::TmpDir(env_) + "/nvm_block_cache_test") {

    Status s;

    BlockCacheImpl::Options opt;

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

    cache_.reset(new BlockCacheImpl(env_, opt));

    s = cache_->Open();
    assert(s.ok());
  }

  virtual ~BlockCacheImplTest() {
    Status s = cache_->Close();
    assert(s.ok());

    log_->Flush();
  }

 protected:

  std::unique_ptr<BlockCacheImpl> cache_;
  Arena arena_;
  Env* env_;
  const std::string path_;
  shared_ptr<Logger> log_;
};

TEST_F(BlockCacheImplTest, Insert) {
  const string prefix = "key_prefix_";

  BlockCacheImpl::LBA lba;

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

class BlkcacheDBTest : public DBTestBase {
 public:
  BlkcacheDBTest() : DBTestBase("/blkcache_db_test") {}

  shared_ptr<Cache> NewBlkCache() {
    return shared_ptr<Cache>(
      new RocksBlockCache(Env::Default(),  test::TmpDir(env_) + "/blkcache",
                          shared_ptr<Logger>(new ConsoleLogger())));
  }
};

static long TestGetTickerCount(const Options& options, Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}

TEST_F(BlkcacheDBTest, CompressedCache) {
  if (!Snappy_Supported()) {
    return;
  }
  int num_iter = 10 * 1024;

  // Run this test three iterations.
  // Iteration 1: only a uncompressed block cache
  // Iteration 2: only a compressed block cache
  // Iteration 3: both block cache and compressed cache
  // Iteration 4: both block cache and compressed cache, but DB is not
  // compressed
  for (int iter = 0; iter < 4; iter++) {
    Options options;
    options.write_buffer_size = 64*1024;        // small write buffer
    options.statistics = rocksdb::CreateDBStatistics();
    options = CurrentOptions(options);

    BlockBasedTableOptions table_options;
    switch (iter) {
      case 0:
        // only uncompressed block cache
        table_options.block_cache = NewBlkCache();
        table_options.block_cache_compressed = nullptr;
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 1:
        // no block cache, only compressed cache
        table_options.no_block_cache = true;
        table_options.block_cache = nullptr;
        table_options.block_cache_compressed = NewLRUCache(8*1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 2:
        // both compressed and uncompressed block cache
        table_options.block_cache = NewBlkCache();
        table_options.block_cache_compressed = NewLRUCache(8*1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        break;
      case 3:
        // both block cache and compressed cache, but DB is not compressed
        // also, make block cache sizes bigger, to trigger block cache hits
        table_options.block_cache = NewBlkCache();
        table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
        options.table_factory.reset(NewBlockBasedTableFactory(table_options));
        options.compression = kNoCompression;
        break;
      default:
        ASSERT_TRUE(false);
    }
    CreateAndReopenWithCF({"pikachu"}, options);
    // default column family doesn't have block cache
    Options no_block_cache_opts;
    no_block_cache_opts.statistics = options.statistics;
    no_block_cache_opts = CurrentOptions(no_block_cache_opts);
    BlockBasedTableOptions table_options_no_bc;
    table_options_no_bc.no_block_cache = true;
    no_block_cache_opts.table_factory.reset(
        NewBlockBasedTableFactory(table_options_no_bc));
    ReopenWithColumnFamilies({"default", "pikachu"},
        std::vector<Options>({no_block_cache_opts, options}));

    Random rnd(301);

    // Write 8MB (80 values, each 100K)
    ASSERT_EQ(NumTableFilesAtLevel(0, 1), 0);
    std::vector<std::string> values;
    std::string str;
    for (int i = 0; i < num_iter; i++) {
      if (i % 4 == 0) {        // high compression ratio
        str = RandomString(&rnd, 1000);
      }
      values.push_back(str);
      ASSERT_OK(Put(1, Key(i), values[i]));
    }

    // flush all data from memtable so that reads are from block cache
    ASSERT_OK(Flush(1));

    for (int i = 0; i < num_iter; i++) {
      ASSERT_EQ(Get(1, Key(i)), values[i]);
    }

    // check that we triggered the appropriate code paths in the cache
    switch (iter) {
      case 0:
        // only uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 1:
        // no block cache, only compressed cache
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 2:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        break;
      case 3:
        // both compressed and uncompressed block cache
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_HIT), 0);
        ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
        // compressed doesn't have any hits since blocks are not compressed on
        // storage
        ASSERT_EQ(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_HIT), 0);
        break;
      default:
        ASSERT_TRUE(false);
    }

    options.create_if_missing = true;
    DestroyAndReopen(options);
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
