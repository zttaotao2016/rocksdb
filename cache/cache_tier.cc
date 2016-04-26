#include "rocksdb/cache_tier.h"

#include "cache/blockcache.h"
#include "cache/cache_volatile.h"

using namespace rocksdb;

std::unique_ptr<TieredCache> TieredCache::New(const size_t mem_size,
                                              const BlockCacheOptions& opt) {
  std::unique_ptr<TieredCache> tcache(new TieredCache());
  // create primary tier
  assert(mem_size);
  auto pcache = std::shared_ptr<CacheTier>(new VolatileCache(mem_size));
  tcache->AddTier(pcache);
  // create secondary tier
  auto scache = std::shared_ptr<CacheTier>(new BlockCacheImpl(opt));
  tcache->AddTier(scache);

  Status s = tcache->Open();
  assert(s.ok());
  return tcache;
}

