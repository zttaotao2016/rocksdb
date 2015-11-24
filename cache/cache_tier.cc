#include <cache/cache_tier.h>
#include <cache/cache_volatile.h>
#include <cache/blockcache.h>

using namespace rocksdb;

Status TieredCache::NewTieredCache(const size_t mem_size,
                                   const BlockCacheOptions& opt,
                                   std::shared_ptr<TieredCache>* tcache) {
  // create primary tier
  assert(mem_size);
  auto pcache = std::unique_ptr<PrimaryCacheTier>(new VolatileCache(mem_size));
  // create secondary tier
  auto scache = std::unique_ptr<SecondaryCacheTier>(new BlockCacheImpl(opt));
  Status s = scache->Open();
  assert(s.ok());
  if (!s.ok()) {
    return s;
  }

  assert(tcache);
  (*tcache).reset(new TieredCache(std::move(pcache), std::move(scache)));

  return Status::OK();
}

