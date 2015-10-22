#pragma once

#include <list>

namespace rocksdb {

template<class T, class Hash, class Equal>
class ScalableHashTable
{
 public:

  ScalableHashTable(const size_t capacity = 1000000,
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

  void Insert(const T& t) {
    assert(!Find(t));
    WriteLock _(GetMutex(t));
    buckets_[BucketIndex(t)].list_.push_back(t);
  }

  T Find(const T& t) {
    GetMutex(t)->AssertHeld();
    typename std::list<T>::iterator it;
    if (!Find(t, it)) {
      return nullptr;
    }
    return *it;
  }

  T Erase(const T& t) {
    WriteLock _(GetMutex(t));
    auto& bucket = buckets_[BucketIndex(t)];
    auto it = Find(t, bucket.list_);
    if (it == bucket.list_.end()) {
      return nullptr;
    }
    T ret = *it;
    bucket.list_.erase(it);
    return ret;
  }

  port::RWMutex* GetMutex(const T& t) {
    auto h = Hash()(t);
    assert(locks_.size());
    return locks_[h % locks_.size()];
  }

 private:

  size_t BucketIndex(const T& t) {
    assert(buckets_.size());
    return Hash()(t) % buckets_.size();
  }

  bool Find(const T& t, typename std::list<T>::iterator& it)  {
    auto& bucket = buckets_[BucketIndex(t)];
    it = Find(t, bucket.list_);
    if (it == bucket.list_.end()) {
      return false;
    }
    return true;
  }

  typename std::list<T>::iterator Find(const T& t, std::list<T>& list) {
    for (auto it = list.begin(); it != list.end(); ++it) {
      if (Equal()(*it, t)) {
        return it;
      }
    }
    return list.end();
  }

  struct Bucket
  {
    Bucket() : count_(0) {}

    size_t count_;
    std::list<T> list_;
  };

  const size_t capacity_;
  const float load_factor_;
  std::vector<Bucket> buckets_;
  std::vector<port::RWMutex*> locks_;
};

}  // namespace rocksdb
