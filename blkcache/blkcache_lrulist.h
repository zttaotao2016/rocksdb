#pragma once

namespace rocksdb {

template<class T>
struct LRUElement
{
  LRUElement(const bool evictable = true)
    : next_(nullptr),
      prev_(nullptr),
      evictable_(evictable) {}

  T* next_;
  T* prev_;
  bool evictable_;
};

template<class T>
class LRUList
{
public:

  LRUList() : head_(nullptr), tail_(nullptr) {}

  virtual ~LRUList() {
    MutexLock _(&lock_);
    assert(!head_);
    assert(!tail_);
  }

  inline void Push(T* const t) {
    MutexLock _(&lock_);
    // list pre-condition
    assert((!head_ && !tail_) || (head_ && tail_));
    assert(!head_ || !head_->prev_);
    assert(!tail_ || !tail_->next_);
    // arg pre-condition
    assert(t);
    assert(!t->next_);
    assert(!t->prev_);

    t->next_ = head_;
    if (head_) {
      head_->prev_ = t;
    }

    head_ = t;
    if (!tail_) {
      tail_ = t;
    }
  }

  inline void Unlink(T* const t) {
    MutexLock _(&lock_);
    UnlinkImpl(t);
  }

  inline T* Pop() {
    MutexLock _(&lock_);
    assert(tail_ && head_);
    assert(!tail_->next_);
    assert(!head_->prev_);

    T* t = head_;
    while (t && !t->evictable_) {
      t = t->next_;
    }

    if (t) {
      assert(t->evictable_);
      UnlinkImpl(t);
    }

    return t;
  }

  inline void Touch(T* const t) {
    MutexLock _(&lock_);
    UnlinkImpl(t);
    PushBackImpl(t);
  }

  inline bool IsEmpty() const {
    MutexLock _(&lock_);
    return !head_ && !tail_;
  }

private:

  void UnlinkImpl(T* const t) {
    lock_.AssertHeld();
    assert(t);
    assert(head_ && tail_);
    assert(t->prev_ || head_ == t);
    assert(t->next_ || tail_ == t);

    if (t->prev_) {
      t->prev_->next_ = t->next_;
    }
    if (t->next_) {
      t->next_->prev_ = t->prev_;
    }

    if (tail_ == t) {
      tail_ = tail_->prev_;
    }
    if (head_ == t) {
      head_ = head_->next_;
    }

    t->next_ = t->prev_ = nullptr;
  }

  inline void PushBack(T* const t) {
    MutexLock _(&lock_);
    PushBackImpl(t);
  }

  inline void PushBackImpl(T* const t) {
    lock_.AssertHeld();
    // list pre-condition
    assert((!head_ && !tail_) || (head_ && tail_));
    assert(!head_ || !head_->prev_);
    assert(!tail_ || !tail_->next_);
    // arg pre-condition
    assert(t);
    assert(!t->next_);
    assert(!t->prev_);

    t->prev_ = tail_;
    if (tail_) {
      tail_->next_ = t;
    }

    tail_ = t;
    if (!head_) {
      head_ = tail_;
    }
  }


  mutable port::Mutex lock_;
  T* head_;           // front (cold)
  T* tail_;           // back (hot)
};

}
