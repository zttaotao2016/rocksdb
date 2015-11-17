//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>

#include "include/rocksdb/cache.h"
#include "cache/blkcache.h"
#include "cache/cache_volatile.h"
#include "util/testharness.h"
#include "util/arena.h"
#include "db/db_test_util.h"
#include "table/block_builder.h"

using namespace std;

namespace rocksdb {

class ConsoleLogger : public Logger {
 public:
  using Logger::Logv;
  ConsoleLogger() : Logger(InfoLogLevel::ERROR_LEVEL) {}

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
        path_(test::TmpDir(env_) + "/cache_test_") {
    Create();
  }

  void Create(const uint32_t max_file_size = 12 * 1024 * 1024,
              const uint64_t max_size = UINT64_MAX) {
    Status s;
    BlockCacheImpl::Options opt;

    opt.writeBufferSize = 1 * 1024 * 1024;
    opt.writeBufferCount = 100;
    opt.maxCacheFileSize =  max_file_size;
    opt.max_bufferpool_size_ = opt.writeBufferSize * opt.writeBufferCount;
    opt.max_size_ = max_size;

    opt.path = path_;
    auto logfile = path_ + "/test.log";
    log_.reset(new ConsoleLogger());
    // env_->NewLogger(logfile, &log_);
    opt.info_log = log_;
    assert(s.ok());

    Log(InfoLogLevel::INFO_LEVEL, log_, "Test output directory %s",
        opt.path.c_str());

    if (cache_) {
      cache_->Close();
    }

    cache_.reset(new BlockCacheImpl(env_, opt));
    s = cache_->Open();
    assert(s.ok());
  }

  virtual ~BlockCacheImplTest() {
    Status s = cache_->Close();
    assert(s.ok());
    log_->Flush();
  }

  void Insert(const size_t nthreads, const size_t max_keys) {
    key_ = 0;
    max_keys_ = max_keys;

    for (size_t i = 0; i < nthreads; i++) {
      env_->StartThread(&BlockCacheImplTest::InsertMain, this);
    }

    while (key_ < max_keys_) {
      sleep(1);
    }
  }

  void InsertBlocks(const size_t nthreads, const size_t max_keys) {
    key_ = 0;
    max_keys_ = max_keys;

    for (size_t i = 0; i < nthreads; i++) {
      env_->StartThread(&BlockCacheImplTest::InsertBlocksMain, this);
    }

    while (key_ < max_keys_) {
      sleep(1);
    }
  }

  void ThreadedVerify(const size_t nthreads) {
    key_ = 0;

    for (size_t i = 0; i < nthreads; i++) {
      env_->StartThread(&BlockCacheImplTest::VerifyMain, this);
    }

    while (key_ < max_keys_) {
      sleep(1);
    }
  }

 protected:
  std::shared_ptr<TieredCache> NewTieredCache() {
    auto* p = new VolatileCache(10 * 4096);
    auto pcache = std::unique_ptr<PrimaryCacheTier>(p);

    BlockCacheImpl::Options opt;
    opt.path = path_;
    auto logfile = opt.path + "/test.log";
    opt.info_log = std::shared_ptr<Logger>(new ConsoleLogger());
    auto* s = new BlockCacheImpl(env_, opt);
    assert(s->Open().ok());
    auto scache = std::unique_ptr<SecondaryCacheTier>(s);
    return std::shared_ptr<TieredCache>(
            new TieredCache(std::move(pcache), std::move(scache)));
  }

  std::string PaddedNumber(const size_t data, const size_t pad_size) {
    assert(pad_size);

    char ret[pad_size];
    int pos = pad_size - 1;
    size_t count = 0;
    size_t t = data;

    while (t) {
      count++;
      ret[pos--] = '0' + t % 10;
      t = t / 10;
    }

    while (pos >= 0) {
      ret[pos--] = '0';
    }

    assert(count <= pad_size);
    assert(pos == -1);

    return std::string(ret, pad_size);
  }

  void Verify(const bool relaxed = false) {
    key_ = 0;
    const string prefix = "key_prefix_";
    while (true) {
      size_t i = key_++;
      if (i > max_keys_) {
        break;
      }

      char edata[4 * 1024];
      memset(edata, '0' + (i % 10), sizeof(edata));
      auto k = prefix + PaddedNumber(i, /*count=*/ 8);
      Slice key(k);
      unique_ptr<char[]> block;
      uint32_t block_size;
      if (relaxed && !cache_->Lookup(key, &block, &block_size)) {
        continue;
      }
      ASSERT_TRUE(cache_->Lookup(key, &block, &block_size));
      ASSERT_TRUE(block_size == sizeof(edata));
      ASSERT_TRUE(memcmp(edata, block.get(), sizeof(edata)) == 0);
    }
  }

  void Insert() {
    const string prefix = "key_prefix_";

    while (true) {
      size_t i = key_++;
      if (i > max_keys_) {
        break;
      }

      char data[4 * 1024];
      memset(data, '0' + (i % 10), sizeof(data));
      auto k = prefix + PaddedNumber(i, /*count=*/ 8);
      Slice key(k);
      while(!cache_->Insert(key, data, sizeof(data)).ok()) {
        sleep(1);
      }
    }
  }

  static void DeleteBlock(const Slice& key, void* data) {
    assert(data);
    auto* block = (Block*) data;
    delete block;
  }

  void InsertBlocks() {
    const string prefix = "key_prefix_";
    while (true) {
      size_t i = key_++;
      if (i > max_keys_) {
        break;
      }

      BlockBuilder bb(/*restarts=*/ 1);
      std::string key(1024, i % 255);
      std::string val(4096, i % 255);
      bb.Add(Slice(key), Slice(val));
      Slice block_template = bb.Finish();

      const size_t block_size = block_template.size();
      unique_ptr<char[]> data(new char[block_size]);
      memcpy(data.get(), block_template.data(), block_size);

      auto k = prefix + PaddedNumber(i, /*count=*/ 8);
      Slice block_key(k);

      Block* block = Block::NewBlock(std::move(data), block_size);
      assert(block);
      assert(block->size());

      Cache::Handle* h = nullptr;
      while(!(h = block_cache_->InsertBlock(block_key, block, &DeleteBlock))) {
        sleep(1);
      }
      assert(block_cache_->Value(h) == block);
      block_cache_->Release(h);
    }
  }

  void VerifyBlocks(const bool relaxed = false) {
    key_ = 0;
    const string prefix = "key_prefix_";
    while (true) {
      size_t i = key_++;
      if (i > max_keys_) {
        break;
      }

      BlockBuilder bb(/*restarts=*/ 1);
      std::string key(1024, i % 255);
      std::string val(4096, i % 255);
      bb.Add(Slice(key), Slice(val));
      Slice block_template = bb.Finish();

      auto k = prefix + PaddedNumber(i, /*count=*/ 8);
      Slice block_key(k);
      Cache::Handle* h = block_cache_->Lookup(block_key);
      ASSERT_TRUE(h);
      ASSERT_TRUE(block_cache_->Value(h));
      Block* block = (Block*) block_cache_->Value(h);
      ASSERT_EQ(block->size(), block_template.size());
      ASSERT_TRUE(memcmp(block->data(), block_template.data(),
                         block->size()) == 0);
      block_cache_->Release(h);
    }
  }

  static void InsertMain(void* arg) {
    BlockCacheImplTest* self = (BlockCacheImplTest*) arg;
    self->Insert();
  }

  static void InsertBlocksMain(void* arg) {
    BlockCacheImplTest* self = (BlockCacheImplTest*) arg;
    self->InsertBlocks();
  }

  static void VerifyMain(void* arg) {
    BlockCacheImplTest* self = (BlockCacheImplTest*) arg;
    self->Verify();
  }

  std::shared_ptr<BlockCacheImpl> cache_;
  std::shared_ptr<Cache> block_cache_;
  Arena arena_;
  Env* env_;
  const std::string path_;
  shared_ptr<Logger> log_;

  atomic<size_t> key_;
  size_t max_keys_;
};

TEST_F(BlockCacheImplTest, Insert) {
  const size_t max_keys = 10 * 1024;
  Insert(/*nthreads=*/ 1, max_keys);
  Verify();
  ThreadedVerify(/*nthreads=*/ 5);
}

TEST_F(BlockCacheImplTest, BlockInsert) {
  block_cache_ = std::make_shared<RocksBlockCache>(cache_);
  const size_t max_keys = 10 * 1024;
  InsertBlocks(/*nthreads=*/ 1, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, VolatileCacheBlockInsert) {
  block_cache_ = std::make_shared<VolatileCache>();
  const size_t max_keys = 10 * 1024;
  InsertBlocks(/*nthreads=*/ 1, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, TieredCacheBlockInsert) {
  auto tcache = NewTieredCache();
  block_cache_ = tcache->GetCache();
  const size_t max_keys = 10 * 1024;
  InsertBlocks(/*nthreads=*/ 1, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, InsertWithEviction) {
  Create(/*max_file_size=*/ 12 * 1024 * 1024,
         /*max_size=*/ 200 * 1024 * 1024);
  const size_t max_keys = 100 * 1024;
  Insert(/*nthreads=*/ 1, max_keys);
  Verify(/*relaxed=*/ true);
}

TEST_F(BlockCacheImplTest, MultiThreadedInsert) {
  const size_t max_keys = 5 * 10 * 1024;
  Insert(/*nthreads=*/ 5, max_keys);
  Verify();
  ThreadedVerify(/*nthreads=*/ 5);
}

TEST_F(BlockCacheImplTest, MultithreadedBlockInsert) {
  block_cache_ = std::make_shared<RocksBlockCache>(cache_);
  const size_t max_keys = 5 * 10 * 1024;
  InsertBlocks(/*nthreads=*/ 5, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, MultithreadedVolatileCacheBlockInsert) {
  block_cache_ = std::make_shared<VolatileCache>();
  const size_t max_keys = 5 * 10 * 1024;
  InsertBlocks(/*nthreads=*/ 5, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, Insert1M) {
  const size_t max_keys = 1024 * 1024;
  Insert(/*nthreads=*/ 10, max_keys);
  ThreadedVerify(/*nthreads=*/ 10);
}

TEST_F(BlockCacheImplTest, Insert1MWithEviction) {
  Create(/*max_file_size=*/ 12 * 1024 * 1024,
         /*max_size=*/ 1024 * 1024 * 1024);
  const size_t max_keys = 1024 * 1024;
  Insert(/*nthreads=*/ 1, max_keys);
  Verify(/*relaxed=*/ true);
}

static long TestGetTickerCount(const Options& options, Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}

class BlkCacheDBTest : public DBTestBase {
 public:
  BlkCacheDBTest() : DBTestBase("/cache_test") {}

  void InitTieredCache() {
    auto* p = new VolatileCache(10 * 4096);
    auto pcache = std::unique_ptr<PrimaryCacheTier>(p);

    BlockCacheImpl::Options opt;
    opt.path = dbname_;
    auto logfile = dbname_ + "/test.log";
    opt.info_log = std::unique_ptr<Logger>(new ConsoleLogger());
    auto* s = new BlockCacheImpl(env_, opt);
    assert(s->Open().ok());
    auto scache = std::unique_ptr<SecondaryCacheTier>(s);
    tcache_.reset(new TieredCache(std::move(pcache), std::move(scache)));
  }

  static shared_ptr<Cache> NewRocksBlockCache(BlkCacheDBTest* self) {
    return shared_ptr<Cache>(
      new RocksBlockCache(Env::Default(),  test::TmpDir(self->env_) + "/"));
  }

  static shared_ptr<Cache> NewVolatileCache(BlkCacheDBTest*) {
    return shared_ptr<Cache>(new VolatileCache());
  }

  static shared_ptr<Cache> NewTieredCache(BlkCacheDBTest* self) {
    self->InitTieredCache();
    return self->tcache_->GetCache();
  }

  void RunTest(shared_ptr<Cache> (*NewBlkCache)(BlkCacheDBTest*)) {
    if (!Snappy_Supported()) {
      return;
    }
    int num_iter = 100 * 1024;

    // Run this test three iterations.
    // Iteration 1: only a uncompressed block cache
    // Iteration 2: only a compressed block cache
    // Iteration 3: both block cache and compressed cache
    // Iteration 4: both block cache and compressed cache, but DB is not
    // compressed
    for (int iter = 0; iter < 3; iter++) {
      Options options;
      options.write_buffer_size = 64*1024;        // small write buffer
      options.statistics = rocksdb::CreateDBStatistics();
      options = CurrentOptions(options);

      BlockBasedTableOptions table_options;
      switch (iter) {
        case 0:
          // only uncompressed block cache
          table_options.block_cache = (*NewBlkCache)(this);
          table_options.block_cache_compressed = nullptr;
          options.table_factory.reset(NewBlockBasedTableFactory(table_options));
          break;
        case 1:
          // both compressed and uncompressed block cache
          table_options.block_cache = (*NewBlkCache)(this);
          table_options.block_cache_compressed = NewLRUCache(8*1024);
          options.table_factory.reset(NewBlockBasedTableFactory(table_options));
          break;
        case 2:
          // both block cache and compressed cache, but DB is not compressed
          // also, make block cache sizes bigger, to trigger block cache hits
          table_options.block_cache = (*NewBlkCache)(this);
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
          // both compressed and uncompressed block cache
          ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_MISS), 0);
          ASSERT_GT(TestGetTickerCount(options, BLOCK_CACHE_COMPRESSED_MISS), 0);
          break;
        case 2:
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

  std::shared_ptr<TieredCache> tcache_;
};

TEST_F(BlkCacheDBTest, BlockCacheTest) {
  RunTest(&NewRocksBlockCache);
}

TEST_F(BlkCacheDBTest, VolatileCacheTest) {
  RunTest(&NewVolatileCache);
}

TEST_F(BlkCacheDBTest, TieredCacheTest) {
  RunTest(&NewTieredCache);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
