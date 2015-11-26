//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>
#include <thread>
#include <functional>
#include <memory>

#include "include/rocksdb/cache.h"
#include "cache/blockcache.h"
#include "cache/cache_volatile.h"
#include "cache/cache_util.h"
#include "cache/cache_tier_testhelper.h"
#include "util/testharness.h"
#include "util/arena.h"
#include "db/db_test_util.h"
#include "table/block_builder.h"

using namespace std;

namespace rocksdb {

// create block cache
std::unique_ptr<SecondaryCacheTier> NewBlockCache(
  Env* const env, const std::string& path,
  const uint64_t max_size = UINT64_MAX) {
  const uint32_t max_file_size = 12 * 1024 * 1024;
  std::shared_ptr<Logger> log(new ConsoleLogger());
  BlockCacheOptions opt(env, path, max_size, log);
  opt.cache_file_size = max_file_size;
  std::unique_ptr<SecondaryCacheTier> scache(new BlockCacheImpl(opt));
  Status s = scache->Open();
  assert(s.ok());
  return std::move(scache);
}

// create a new cache tier
std::unique_ptr<TieredCache> NewTieredCache(
  Env* const env, const std::string& path,
  const uint64_t max_scache_size = UINT64_MAX) {
  // create primary cache
  std::unique_ptr<PrimaryCacheTier> pcache(new VolatileCache(10 * 4096));
  // create secondary cache
  auto scache = std::move(NewBlockCache(env, path, max_scache_size));
  // create tier out of the two caches
  std::unique_ptr<TieredCache> tcache(
    new TieredCache(std::move(pcache), std::move(scache)));
  return std::move(tcache);
}

/**
 * Unit tests for testing block cache
 */
class BlockCacheImplTest : public testing::Test {
 public:
  explicit BlockCacheImplTest()
      : env_(Env::Default()),
        path_(test::TmpDir(env_) + "/cache_test") {
  }

  virtual ~BlockCacheImplTest() {
    if (scache_ ){
      Status s = scache_->Close();
      assert(s.ok());
    }
  }

 protected:
  void Flush() {
    if (pcache_) {
      pcache_->Flush_TEST();
    }

    if (scache_) {
      scache_->Flush_TEST();
    }
  }

  template<class T>
  std::list<std::thread> SpawnThreads(const size_t n, const T& fn) {
    std::list<std::thread> threads;
    for (size_t i = 0; i < n; i++) {
      std::thread th(fn);
      threads.push_back(std::move(th));
    }
    return std::move(threads);
  }

  void Join(std::list<std::thread>&& threads) {
    for (auto& th : threads) {
      th.join();
    }
    threads.clear();
  }

  void Insert(const size_t nthreads, const size_t max_keys) {
    key_ = 0;
    max_keys_ = max_keys;
    // spawn threads
    auto fn = std::bind(&BlockCacheImplTest::InsertImpl, this);
    auto threads = std::move(SpawnThreads(nthreads, fn));
    // join with threads
    Join(std::move(threads));
    // Flush cache
    Flush();
  }

  void Verify(const size_t nthreads = 1, const bool eviction_enabled = false) {
    key_ = 0;
    // spawn threads
    auto fn = std::bind(&BlockCacheImplTest::VerifyImpl, this,
                        eviction_enabled);
    auto threads = std::move(SpawnThreads(nthreads, fn));
    // join with threads
    Join(std::move(threads));
  }

  void InsertBlocks(const size_t nthreads, const size_t max_keys) {
    key_ = 0;
    max_keys_ = max_keys;
    // spawn threads
    auto fn = std::bind(&BlockCacheImplTest::InsertBlocksImpl, this);
    auto threads = std::move(SpawnThreads(nthreads, fn));
    // join with threads
    Join(std::move(threads));
    // Flush
    Flush();
  }

  void VerifyBlocks(const size_t nthreads = 1,
                    const bool eviction_enabled = false) {
    key_ = 0;
    // spawn threads
    auto fn = std::bind(&BlockCacheImplTest::VerifyBlocksImpl, this,
                        eviction_enabled);
    auto threads = std::move(SpawnThreads(nthreads, fn));
    // join with threads
    Join(std::move(threads));
  }

  // pad 0 to numbers
  std::string PaddedNumber(const size_t data, const size_t pad_size) {
    assert(pad_size);
    char ret[pad_size];
    int pos = static_cast<int>(pad_size) - 1;
    size_t count = 0;
    size_t t = data;
    // copy numbers
    while (t) {
      count++;
      ret[pos--] = '0' + t % 10;
      t = t / 10;
    }
    // copy 0s
    while (pos >= 0) {
      ret[pos--] = '0';
    }
    // post condition
    assert(count <= pad_size);
    assert(pos == -1);
    return std::string(ret, pad_size);
  }

  void InsertImpl() {
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
      while(!scache_->Insert(key, data, sizeof(data)).ok()) {
        sleep(1);
      }
    }
  }

  void VerifyImpl(const bool eviction_enabled = false) {
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
      size_t block_size;

      if (eviction_enabled) {
        if (!scache_->Lookup(key, &block, &block_size)) {
          // assume that the key is evicted
          continue;
        }
      }

      ASSERT_TRUE(scache_->Lookup(key, &block, &block_size));
      ASSERT_TRUE(block_size == sizeof(edata));
      ASSERT_TRUE(memcmp(edata, block.get(), sizeof(edata)) == 0);
    }
  }

  static void DeleteBlock(const Slice& key, void* data) {
    assert(data);
    auto* block = (Block*) data;
    delete block;
  }

  void InsertBlocksImpl() {
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
      while(!(h = pcache_->InsertBlock(block_key, block, &DeleteBlock))) {
        sleep(1);
      }
      assert(pcache_->Value(h) == block);
      pcache_->Release(h);
    }
  }

  void VerifyBlocksImpl(const bool relaxed = false) {
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
      Cache::Handle* h = pcache_->Lookup(block_key);
      if (!h && relaxed) {
        continue;
      }
      ASSERT_TRUE(h);
      ASSERT_TRUE(pcache_->Value(h));
      Block* block = (Block*) pcache_->Value(h);
      ASSERT_EQ(block->size(), block_template.size());
      ASSERT_TRUE(memcmp(block->data(), block_template.data(),
                         block->size()) == 0);
      pcache_->Release(h);
    }
  }

  Env* env_;
  const std::string path_;
  shared_ptr<Logger> log_;
  std::shared_ptr<SecondaryCacheTier> scache_;
  std::shared_ptr<PrimaryCacheTier> pcache_;
  atomic<size_t> key_{0}; 
  size_t max_keys_ = 0;
};

TEST_F(BlockCacheImplTest, Insert) {
  scache_ = std::move(NewBlockCache(env_, path_));
  const size_t max_keys = 10 * 1024;
  Insert(/*nthreads=*/ 1, max_keys);
  Verify();
  Verify(/*nthreads=*/ 5);
}

TEST_F(BlockCacheImplTest, BlockInsert) {
  auto scache = std::move(NewBlockCache(env_, path_));
  pcache_ = std::make_shared<SecondaryCacheTierCloak>(std::move(scache));
  const size_t max_keys = 10 * 1024;
  InsertBlocks(/*nthreads=*/ 1, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, VolatileCacheBlockInsert) {
  pcache_ = std::make_shared<VolatileCache>();
  const size_t max_keys = 10 * 1024;
  InsertBlocks(/*nthreads=*/ 1, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, TieredCacheBlockInsert) {
  auto tcache = std::move(NewTieredCache(env_, path_));
  pcache_ = tcache->GetCache();
  const size_t max_keys = 10 * 1024;
  InsertBlocks(/*nthreads=*/ 1, max_keys);
  VerifyBlocks();
}

TEST_F(BlockCacheImplTest, InsertWithEviction) {
  scache_ = std::move(NewBlockCache(env_, path_,
                                    /*max_size=*/ 200 * 1024 * 1024));
  const size_t max_keys = 100 * 1024;
  Insert(/*nthreads=*/ 1, max_keys);
  Verify(/*n=*/ 1, /*eviction_enabled=*/ true);
}

TEST_F(BlockCacheImplTest, VolatileBlockInsertWithEviction) {
  pcache_ = std::make_shared<VolatileCache>(1 * 1024 * 1024);
  const size_t max_keys = 10 * 1024;
  InsertBlocks(/*nthreads=*/ 1, max_keys);
  VerifyBlocks(/*n=*/ 1, /*relaxed=*/ true);
}

TEST_F(BlockCacheImplTest, MultiThreadedInsert) {
  scache_ = std::move(NewBlockCache(env_, path_));
  const size_t max_keys = 5 * 10 * 1024;
  Insert(/*nthreads=*/ 5, max_keys);
  Verify();
  Verify(/*nthreads=*/ 5);
}

TEST_F(BlockCacheImplTest, MultithreadedBlockInsert) {
  auto scache = std::move(NewBlockCache(env_, path_));
  pcache_ = std::make_shared<SecondaryCacheTierCloak>(std::move(scache));

  const size_t max_keys = 5 * 10 * 1024;
  InsertBlocks(/*nthreads=*/ 5, max_keys);
  VerifyBlocks();
  VerifyBlocks(/*nthread=*/ 5);
}

TEST_F(BlockCacheImplTest, MultithreadedVolatileCacheBlockInsert) {
  pcache_ = std::make_shared<VolatileCache>();
  const size_t max_keys = 5 * 10 * 1024;
  InsertBlocks(/*nthreads=*/ 5, max_keys);
  VerifyBlocks();
  VerifyBlocks(/*nthread=*/ 5);
}

TEST_F(BlockCacheImplTest, Insert1M) {
  scache_ = std::move(NewBlockCache(env_, path_));
  const size_t max_keys = 1024 * 1024;
  Insert(/*nthreads=*/ 10, max_keys);
  Verify();
  Verify(/*nthreads=*/ 10);
}

TEST_F(BlockCacheImplTest, BlockInsert1M) {
  auto scache = std::move(NewBlockCache(env_, path_));
  pcache_ = std::make_shared<SecondaryCacheTierCloak>(std::move(scache));

  const size_t max_keys = 1 * 1024 * 1024;
  InsertBlocks(/*nthreads=*/ 10, max_keys);
  VerifyBlocks();
  VerifyBlocks(/*nthread=*/ 10);
}

TEST_F(BlockCacheImplTest, VolatileCacheBlockInsert1M) {
  pcache_ = std::make_shared<VolatileCache>();
  const size_t max_keys = 1 * 1024 * 1024;
  InsertBlocks(/*nthreads=*/ 10, max_keys);
  VerifyBlocks();
  VerifyBlocks(/*nthread=*/ 10);
}

TEST_F(BlockCacheImplTest, Insert1MWithEviction) {
  scache_ = std::move(NewBlockCache(env_, path_,
                                    /*max_size=*/ 1024 * 1024 * 1024));
  const size_t max_keys = 1024 * 1024;
  Insert(/*nthreads=*/ 10, max_keys);
  Verify(/*n=*/ 1, /*eviciton_enabled=*/ true);
  Verify(/*n=*/ 10, /*eviciton_enabled=*/ true);
}

TEST_F(BlockCacheImplTest, VolatileCacheBlockInsert1MWithEviction) {
  pcache_ = std::make_shared<VolatileCache>(/*size=*/ 16 * 1024);
  const size_t max_keys = 1 * 1024 * 1024;
  InsertBlocks(/*nthreads=*/ 10, max_keys);
  VerifyBlocks(/*n=*/ 1, /*eviction_enabled=*/ true);
  VerifyBlocks(/*n=*/ 10, /*eviction_enabled=*/ true);
}

static long TestGetTickerCount(const Options& options, Tickers ticker_type) {
  return options.statistics->getTickerCount(ticker_type);
}

class BlkCacheDBTest : public DBTestBase {
 public:
  BlkCacheDBTest() : DBTestBase("/cache_test") {}

  std::shared_ptr<PrimaryCacheTier> MakeSecondaryCacheTierCloak() {
    std::unique_ptr<SecondaryCacheTier> scache(
      std::move(NewBlockCache(env_,  test::TmpDir(env_) + "/block_cache")));
    return std::make_shared<SecondaryCacheTierCloak>(std::move(scache));
  }

  std::shared_ptr<PrimaryCacheTier> MakeVolatileCache() {
    return std::shared_ptr<PrimaryCacheTier>(new VolatileCache());
  }

  std::shared_ptr<PrimaryCacheTier> MakeTieredCache() {
    tcache_ = std::move(NewTieredCache(env_, dbname_));
    return tcache_->GetCache();
  }

  void RunTest(std::function<std::shared_ptr<PrimaryCacheTier>()> new_cache) {
    if (!Snappy_Supported()) {
      return;
    }
    int num_iter = 100 * 1024;

    auto block_cache = new_cache();

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
          table_options.block_cache = block_cache;
          table_options.block_cache_compressed = nullptr;
          options.table_factory.reset(NewBlockBasedTableFactory(table_options));
          break;
        case 1:
          // both compressed and uncompressed block cache
          table_options.block_cache = block_cache;
          table_options.block_cache_compressed = NewLRUCache(8*1024);
          options.table_factory.reset(NewBlockBasedTableFactory(table_options));
          break;
        case 2:
          // both block cache and compressed cache, but DB is not compressed
          // also, make block cache sizes bigger, to trigger block cache hits
          table_options.block_cache = block_cache;
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
      block_cache->Flush_TEST();

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

    block_cache->Flush_TEST();
  }

  std::unique_ptr<TieredCache> tcache_;
};

TEST_F(BlkCacheDBTest, BlockCacheTest) {
  RunTest(std::bind(&BlkCacheDBTest::MakeSecondaryCacheTierCloak, this));
}

TEST_F(BlkCacheDBTest, VolatileCacheTest) {
  RunTest(std::bind(&BlkCacheDBTest::MakeVolatileCache, this));
}

TEST_F(BlkCacheDBTest, TieredCacheTest) {
  RunTest(std::bind(&BlkCacheDBTest::MakeTieredCache, this));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
