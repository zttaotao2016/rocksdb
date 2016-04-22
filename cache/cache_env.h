#pragma once

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "rocksdb/env.h"
#include "util/io_posix.h"
#include "util/io_posix.h"

namespace rocksdb {
//
// Direct IO file implementation for Linux based systems
//
class DirectIOFile {
 public:
  explicit DirectIOFile(const std::string& fname)
    : fname_(fname) {}

  virtual ~DirectIOFile() {
    Close();
  }

  virtual Status OpenForReading() {
    return Open(O_RDONLY);
  }

  virtual Status OpenForWriting() {
    return Open(O_WRONLY | O_TRUNC | O_CREAT | O_APPEND);
  }

  virtual Status Append(const Slice& data) {
    // direct IO preconditions
    assert(IsSectorAligned(data.size()));
    assert(IsPageAligned(data.data()));

    size_t written = 0;
    while (written < data.size()) {
      int status = write(fd_, (char*) data.data() + written,
                         data.size() - written);
      assert(status >= 0);
      if (status < 0) {
        break;
      }
      written += status;
    }
    return written < data.size() ? Status::IOError(strerror(errno))
                                 : Status::OK();
  }

  virtual Status PositionedAppend(const Slice& data, uint64_t offset) {
    // direct io pre-conditions
    assert(IsSectorAligned(offset));
    assert(IsSectorAligned(data.size()));
    assert(IsPageAligned(data.data()));

    size_t written = 0;
    while (written < data.size()) {
      int status = pwrite(fd_, (char*) data.data() + written,
                          data.size() - written, offset + written);
      assert(status >= 0);
      if (status < 0) {
        break;
      }
      written += status;
    }
    return written < data.size() ? Status::IOError(strerror(errno))
                                 : Status::OK();
  }

  virtual Status Read(const Slice& data, const uint64_t offset,
                      const size_t size) {
    if (IsSectorAligned(offset) && IsSectorAligned(size)
        && IsPageAligned(data.data())) {
      return ReadAligned(data, offset, size);
    }

    return ReadUnaligned(data, offset, size);
  }


  virtual Status Truncate(uint64_t size) {
    int status = ftruncate(fd_, /*size=*/ size);
    if (status == 0) {
      return Status::OK();
    }
    return Status::IOError(strerror(errno));
  }

  virtual Status Close() {
    if (fd_ != -1) {
      close(fd_);
      fd_ = -1;
    }
    return Status::OK();
  }

  virtual uint64_t GetFileSize() {
    struct stat s;
    if (fstat(fd_, &s) == 0) {
      return 0;
    }
    return s.st_size;
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const {
    return PosixHelper::GetUniqueIdFromFile(fd_, id, max_size);
  }

  virtual Status Flush() {
    if (fsync(fd_) != 0) {
      return Status::IOError(strerror(errno));
    }
    return Status::OK();
  }

 private:
  static const size_t SECTOR_SIZE = 512;
  static const size_t PAGE_SIZE = 4 * 1024;

  virtual Status Open(int flags) {
#ifndef LINUX_RAMFS
    flags |= O_DIRECT;
#endif
    fd_ = open(fname_.c_str(), flags, 0644);
    assert(fd_ > 0);

    if (fd_ < 0) {
      return Status::IOError(strerror(errno));
    }

    return Status::OK();
  }

  static size_t Upper(const size_t size, const size_t fac) {
    if (size % fac == 0) {
      return size;
    }

    return size + (fac - size % fac);
  }

  static size_t Lower(const size_t size, const size_t fac) {
    if (size % fac == 0) {
      return size;
    }

    return size - (size % fac);
  }

  static std::unique_ptr<void, void(&)(void*)> NewAligned(const size_t size) {
    void* ptr;
    if (posix_memalign(&ptr, 4 * 1024, size) != 0) {
      return std::unique_ptr<char, void(&)(void*)>(nullptr, free);
    }

    std::unique_ptr<void, void(&)(void*)> uptr(ptr, free);
    return std::move(uptr);
  }

  bool IsSectorAligned(const size_t off) const {
    return off % SECTOR_SIZE == 0;
  }

  bool IsPageAligned(const void* ptr) const {
    return uintptr_t(ptr) % (PAGE_SIZE) == 0;
  }

  virtual Status ReadAligned(const Slice& data, const uint64_t offset,
                             const size_t size) {
    assert(IsSectorAligned(offset));
    assert(IsSectorAligned(size));
    assert(IsPageAligned(data.data()));

    size_t bytes_read = 0;
    while (bytes_read < data.size()) {
      int status = pread(fd_, (char*) data.data() + bytes_read,
                         data.size() - bytes_read, offset + bytes_read);
      assert(status >= 0);
      if (status < 0) {
        break;
      }
      bytes_read += status;
    }
    return bytes_read < data.size() ? Status::IOError(strerror(errno))
                                    : Status::OK();
  }

  virtual Status ReadUnaligned(const Slice& data, const uint64_t offset,
                               const size_t size) {
    assert(!IsSectorAligned(offset) || !IsSectorAligned(size)
           || !IsPageAligned(data.data()));

    const uint64_t aligned_off =  Lower(offset, SECTOR_SIZE);
    const size_t aligned_size = Upper(size + (offset - aligned_off),
                                      SECTOR_SIZE);
    auto scratch = NewAligned(aligned_size);

    assert(IsSectorAligned(aligned_off));
    assert(IsSectorAligned(aligned_size));
    assert(scratch);
    assert(IsPageAligned(scratch.get()));
    assert(offset + size <= aligned_off + aligned_size);

    if (!scratch) {
      return Status::IOError("Unable to allocate");
    }

    Status s = Read(Slice((char*) scratch.get(), aligned_size), aligned_off,
                    aligned_size);
    if (!s.ok()) {
      return s;
    }

    memcpy((char*) data.data(),(char*)(scratch.get()) + (offset % SECTOR_SIZE),
           size);
    return Status::OK();
  }

  const std::string fname_;
  int fd_ = -1;
};

//
// DirectIO WriteableFile implementation
//
class DirectIOWritableFile : public WritableFile {
 public:
  explicit DirectIOWritableFile(const std::string& fname)
    : file_(fname) {}

  virtual ~DirectIOWritableFile() {
    Flush();
    Close();
  }

  virtual Status Open() {
    return file_.OpenForWriting();
  }

  bool UseOSBuffer() const override {
    return false;
  }

  size_t GetRequiredBufferAlignment() const override {
    return 4 * 1024;
  }

  Status Append(const Slice& data) override {
    return file_.Append(data);
  }

  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    return file_.PositionedAppend(data, offset);
  }

  Status Truncate(uint64_t size) override {
    return file_.Truncate(size);
  }

  Status Close() override {
    return file_.Close();
  }

  Status Flush() override {
    return file_.Flush();
  }

  uint64_t GetFileSize() override {
    return file_.GetFileSize();
  }

  Status Sync() override {
    return Status::NotSupported();
  }

  Status Fsync() override {
    return Status::NotSupported();
  }

  bool IsSyncThreadSafe() const override {
    return false;
  }

  bool UseDirectIO() const override {
    return true;
  }

  void SetIOPriority(Env::IOPriority /* pri */) override {
    /* not supported */
  }

  Env::IOPriority GetIOPriority() override {
    return Env::IOPriority::IO_HIGH;
  }

  void GetPreallocationStatus(size_t* /* block_size */,
                              size_t* /* last_allocated_block */) override {
    /* not supported */
  }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    return file_.GetUniqueId(id, max_size);
  }

  Status InvalidateCache(size_t offset, size_t length) override {
    return Status::NotSupported();
  }

  Status RangeSync(uint64_t offset, uint64_t nbytes) override {
    return Status::NotSupported();
  }

 private:
  DirectIOFile file_;
};

//
// DirectIO Sequential File implementation
//
class DirectIOSequentialFile : public SequentialFile {
 public:
  explicit DirectIOSequentialFile(const std::string& fname) 
    : file_(fname) {}

  virtual ~DirectIOSequentialFile() {}

  virtual Status Open() {
    return file_.OpenForReading();
  }

  Status Read(size_t n, Slice* result, char* scratch) override {
    const size_t off = off_.fetch_add(n);
    Status s = file_.Read(Slice(scratch, n), off, n);
    *result = Slice(scratch, n);
    return s;
  }

  Status Skip(uint64_t n) override {
    off_ += n;
    return Status::OK();
  }

 private:
  DirectIOFile file_;
  std::atomic<size_t> off_{0};    // read offset
};

//
// Direct IO random access file implementation
//
class DirectIORandomAccessFile : public RandomAccessFile {
 public:
  explicit DirectIORandomAccessFile(const std::string& fname)
    : file_(fname) {}

  virtual ~DirectIORandomAccessFile() {}

  virtual Status Open() {
    return file_.OpenForReading();
  }

  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s = file_.Read(Slice(scratch, n), offset, n);
    *result = Slice(scratch, n);
    return s;
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const {
    return file_.GetUniqueId(id, max_size);
  };

 public:
  mutable DirectIOFile file_;
};

//
// Direct IO Env implementation for *nix operating system variants
//
class DirectIOEnv : public EnvWrapper {
public:
  explicit DirectIOEnv(Env* env, const bool direct_read = true)
    : EnvWrapper(env), direct_read_(direct_read) {}

  virtual ~DirectIOEnv() {}

  virtual Status NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) {
    if (direct_read_) {
      std::unique_ptr<DirectIOSequentialFile> file(
        new DirectIOSequentialFile(fname));
      Status s = file->Open();
      if (s.ok()) {
        *result = std::move(file);
      }
      return s;
    } else {
      return target()->NewSequentialFile(fname, result, options);
    }
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) {
    if (direct_read_) {
      std::unique_ptr<DirectIORandomAccessFile> file(
        new DirectIORandomAccessFile(fname));
      Status s = file->Open();
      if (s.ok()) {
        *result = std::move(file);
      }
      return s;
    } else {
      return target()->NewRandomAccessFile(fname, result, options);
    }
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) {
    std::unique_ptr<DirectIOWritableFile> file(
      new DirectIOWritableFile(fname));
    Status s = file->Open();
    if (s.ok()) {
      *result = std::move(file);
    }
    return s;
  }

private:
  const bool direct_read_ = true;
};

}
