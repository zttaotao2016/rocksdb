#include "cache/hash_table.h"
#include "cache/hash_table_test.h"
#include "include/rocksdb/env.h"
#include "port/port_posix.h"
#include "util/mutexlock.h"

#include <gflags/gflags.h>
#include <unordered_map>
#include <functional>

using namespace rocksdb;
using namespace std;

DEFINE_int32(nsec, 10, "nsec");
DEFINE_int32(nthread_write, 1, "insert %");
DEFINE_int32(nthread_read, 1, "lookup %");
DEFINE_int32(nthread_erase, 1, "erase %");

//
// SimpleHashMap -- unordered_map implementation
//
class SimpleHashMap : public HashTableImpl<int, string> {
 public:

  bool Insert(const int& key, const string& val) override {
   WriteLock _(&rwlock_);
   map_.insert(make_pair(key, val));
   return true;
  }

  bool Erase(const int& key) override {
    WriteLock _(&rwlock_);
    auto it = map_.find(key);
    if (it == map_.end()) {
      return false;
    }
    map_.erase(it);
    return true;
  }

  bool Lookup(const int& key, string* val) override {
    ReadLock _(&rwlock_);
    return map_.find(key) != map_.end();
  }

 private:

  port::RWMutex rwlock_;
  std::unordered_map<int, string> map_;
};

//
// ScalableHashMap
//
class ScalableHashMap : public HashTableImpl<int, string> {
 public:

  bool Insert(const int& key, const string& val) override {
    Node n(key, val);
    return impl_.Insert(n);
  }

  bool Erase(const int& key) override {
    Node n(key, string());
    return impl_.Erase(n, nullptr);
  }

  bool Lookup(const int& key, string* val) override {
    Node n(key, string());
    ReadLock _(impl_.GetMutex(n));
    return impl_.Find(n, nullptr);
  }

 private:

  struct Node {
    Node(const int key, const string& val)
      : key_(key)
      , val_(val) {
    }

    int key_;
    string val_;
  };

  struct Hash {
    uint64_t operator()(const Node& node) {
      return std::hash<uint64_t>()(node.key_);
    }
  };

  struct Equal {
    bool operator()(const Node& lhs, const Node& rhs) {
      return lhs.key_ == rhs.key_;
    }
  };

  HashTable<Node, Hash, Equal> impl_;
};


//
// main
//
int
main(int argc, char** argv) {
  google::SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                          " [OPTIONS]...");
  google::ParseCommandLineFlags(&argc, &argv, false);

  //
  // Micro benchmark unordered_map
  //
  printf("Micro benchmarking std::unordered_map");
  {
    SimpleHashMap impl;
    HashTableTest _(&impl, FLAGS_nsec, FLAGS_nthread_write, FLAGS_nthread_read,
                    FLAGS_nthread_erase, /*benchmark=*/ true);
   }

  //
  // Micro benchmark scalable hash table
  //
  printf("Micro benchmarking scalable hash map");
  {
    ScalableHashMap impl;
    HashTableTest _(&impl, FLAGS_nsec, FLAGS_nthread_write, FLAGS_nthread_read,
                    FLAGS_nthread_erase, /*benchmark=*/ true);
  }

  return 0;
}
