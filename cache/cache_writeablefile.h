#pragma once

#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <memory>

#include "include/rocksdb/env.h"

namespace rocksdb {

class CacheWritableFile : public WritableFile {
 public:
  CacheWritableFile() : fd_(-1) {}

  virtual ~CacheWritableFile() {
    if (fd_ > 0) {
      Close();
    }
  }

  Status Create(const std::string& filepath) {
    fd_ = open(filepath.c_str(), O_TRUNC | O_CREAT | O_RDWR, 0644);
    assert(fd_ != -1);
    if (fd_ == -1) {
      return Status::IOError(strerror(errno));
    }

    posix_fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);

    return Status::OK();
  }

  Status Append(const Slice& data) override {
    int status = write(fd_, data.data(), data.size());
    assert(status == (int) data.size());
    if (status != (int) data.size()) {
      return Status::IOError(strerror(errno));
    }
    return Status::OK();
  }

  Status Close() override {
    int status;
    status = fsync(fd_);
    assert(status == 0);
    status = close(fd_);
    assert(status == 0);
    fd_ = -1;
    return Status::OK();
  }

  Status Flush() {
    assert(!"not supported");
    return Status::NotSupported();
  }

  virtual Status Sync() {
    assert(!"not supported");
    return Status::NotSupported();
  }

 private:
  int fd_;
};

class CacheRandomAccessFile : public RandomAccessFile {
 public:
  CacheRandomAccessFile() : fd_(-1) {}
  virtual ~CacheRandomAccessFile() {
    int status = close(fd_);
    assert(status == 0);
  }

  Status Open(const std::string& filepath) {
    fd_ = open(filepath.c_str(), O_RDONLY, S_IRWXU);
    assert(fd_ != -1);
    if (fd_ == -1) {
      return Status::IOError(strerror(errno));
    }

    posix_fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
    return Status::OK();
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    size_t status = pread(fd_, scratch, n, offset);
    assert(status == n);
    if (status != n) {
      return Status::IOError(strerror(errno));
    }
    *result = Slice(scratch, n);
    return Status::OK();
  }

 private:
  int fd_;
};

}
