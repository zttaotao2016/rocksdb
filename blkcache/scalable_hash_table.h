#pragma once

#include <list>

namespace rocksdb {

#ifdef PARANOID
#define paranoid(x) assert(x)
#else
#define paranoid(x)
#endif

template<class T, class Hash, class Equal>
class ScalableHashTable
{
 public:

  ScalableHashTable(const size_t capacity = 1024 * 1024,
                    const float load_factor = 2.0, const size_t nlocks = 256)
    : capacity_(capacity),
      load_factor_(load_factor) {
    // initialize the spine
    const size_t nbuckets = capacity / load_factor;
    buckets_.resize(nbuckets);
    // initialize locks
    locks_.resize(nlocks);
    for (size_t i = 0; i < nlocks; ++i) {
      locks_[i] = new port::RWMutex();
    }
  }

  virtual ~ScalableHashTable() {
    for (auto* lock : locks_) {
      delete lock;
    }
    locks_.clear();
  }

  bool Insert(const T& t) {
    const uint64_t h = Hash()(t);

    WriteLock _(locks_[h % locks_.size()]);

    auto& list = buckets_[h % buckets_.size()].list_;
    if (Find(list, t) != list.end()) {
      return false;
    }

    list.push_back(t);
    return true;
  }


  bool Find(const T& t, T* ret) {
    GetMutex(t)->AssertHeld();
    const uint64_t h = Hash()(t);

    auto& bucket = buckets_[h % buckets_.size()];
    auto& list = bucket.list_;
    auto it = Find(list, t);
    if (it == list.end()) {
      return false;;
    }

    if (ret) {
      *ret = *it;
    }

    return true;
  }

  bool Erase(const T& t, T* ret) {
    const uint64_t h = Hash()(t);
    WriteLock _(locks_[h % locks_.size()]);

    auto& bucket = buckets_[h % buckets_.size()];
    auto& list = bucket.list_;
    auto it = Find(list, t);
    if (it == list.end()) {
      return false;;
    }

    if (ret) {
      *ret = *it;
    }

    list.erase(it);
    return true;
  }

  port::RWMutex* GetMutex(const T& t) {
    paranoid(locks_.size());
    return locks_[Hash()(t) % locks_.size()];
  }

 private:

  typename std::list<T>::iterator Find(std::list<T>& list, const T& t) {
    for (auto it = list.begin(); it != list.end(); ++it) {
      if (Equal()(*it, t)) {
        return it;
      }
    }
    return list.end();
  }

  struct Bucket {
    Bucket()
      : count_(0) {
    }

    size_t count_;
    std::list<T> list_;
  };

  const size_t capacity_;
  const float load_factor_;
  std::vector<Bucket> buckets_;
  std::vector<port::RWMutex*> locks_;
};

}  // namespace rocksdb
