#pragma once

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <rocksdb/env.h>

namespace rocksdb {

class CacheWritableFile : public WritableFile {
 public:
  explicit CacheWritableFile(const std::string& fname)
    : fname_(fname) {}

  virtual ~CacheWritableFile() {
    Flush();
    Close();
  }

  Status Open() {
    int flags = O_WRONLY | O_TRUNC | O_CREAT;
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

  virtual bool UseOSBuffer() const {
    return false;
  }

  virtual size_t GetRequiredBufferAlignment() const {
    return 4 * 1024;
  }

  virtual Status Append(const Slice& data) {
    // direct IO preconditions
    assert(data.size() % 512 == 0);
    assert(uintptr_t(data.data()) % (4 * 1024) == 0);

    size_t written = 0;
    while (written < data.size()) {
      int status = write(fd_, data.data(),  data.size());
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
    assert(offset % 512 == 0);
    assert(data.size() % 512 == 0);
    assert(uintptr_t(data.data()) % (4 * 1024) == 0);

    size_t written = 0;
    while (written < data.size()) {
      int status = pwrite(fd_, data.data(), data.size(), offset + written);
      assert(status >= 0);
      if (status < 0) {
        break;
      }
      written += status;
    }
    return written < data.size() ? Status::IOError(strerror(errno))
                                 : Status::OK();
  }

  virtual Status Truncate(uint64_t size) {
    int status = ftruncate(fd_, /*size=*/ 0);
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

  virtual Status Flush() {
    return Status::NotSupported();
  }

  virtual uint64_t GetFileSize() {
    struct stat s;
    if (fstat(fd_, &s) == 0) {
      return 0;
    }
    return s.st_size;
  }

  virtual Status Sync() {
    return Status::NotSupported();
  }

  virtual Status Fsync() {
    return Status::NotSupported();
  }

  virtual bool IsSyncThreadSafe() const {
    return false;
  }

  virtual bool UseDirectIO() const {
    return true;
  }

  virtual void SetIOPriority(Env::IOPriority /* pri */) {
    /* not supported */
  }

  virtual Env::IOPriority GetIOPriority() {
    return Env::IOPriority::IO_HIGH;
  }


  void SetPreallocationBlockSize(size_t size) {
    /* not supported */
  }

  virtual void GetPreallocationStatus(size_t* /* block_size */,
                                      size_t* /* last_allocated_block */) {
    /* not supported */
  }

  virtual size_t GetUniqueId(char* id, size_t max_size) const {
    return 0;
  }

  virtual Status InvalidateCache(size_t offset, size_t length) {
    return Status::NotSupported();
  }

  virtual Status RangeSync(uint64_t offset, uint64_t nbytes) {
    return Status::NotSupported();
  }

  void PrepareWrite(size_t /* offset */, size_t /* len */) {
    /* not supported */
  }

private:
  const std::string fname_;     // file name
  int fd_ = -1;                 // file descriptor
};


class CacheEnv : public Env {
public:
  explicit CacheEnv(Env* env)
    : env_(env) {}

  virtual ~CacheEnv() {}

  virtual Status NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) {
    return env_->NewSequentialFile(fname, result, options);
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) {
    return env_->NewRandomAccessFile(fname, result, options);
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) {
    std::unique_ptr<CacheWritableFile> file(new CacheWritableFile(fname));
    Status s = file->Open();
    if (s.ok()) {
      *result = std::move(file);
    }
    return s;
  }

  virtual Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   unique_ptr<WritableFile>* result,
                                   const EnvOptions& options) {
    return env_->ReuseWritableFile(fname, old_fname, result, options);
  }

  virtual Status NewDirectory(const std::string& name,
                              unique_ptr<Directory>* result) {
    return env_->NewDirectory(name, result);
  }

  virtual Status FileExists(const std::string& fname) {
    return env_->FileExists(fname);
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) {
    return env_->GetChildren(dir, result);
  }

  virtual Status DeleteFile(const std::string& fname) {
    return env_->DeleteFile(fname);
  }

  virtual Status CreateDir(const std::string& dirname) {
    return env_->CreateDir(dirname);
  }

  virtual Status CreateDirIfMissing(const std::string& dirname) {
    return env_->CreateDirIfMissing(dirname);
  }

  virtual Status DeleteDir(const std::string& dirname) {
    return env_->DeleteDir(dirname);
  }

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) {
    return env_->GetFileSize(fname, file_size);
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) {
    return env_->GetFileModificationTime(fname, file_mtime);
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& target) {
    return env_->RenameFile(src, target);
  }

  virtual Status LinkFile(const std::string& src, const std::string& target) {
    return env_->LinkFile(src, target);
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) {
    return env_->LockFile(fname, lock);
  }

  virtual Status UnlockFile(FileLock* lock) {
    return env_->UnlockFile(lock);
  }

  virtual void Schedule(void (*function)(void* arg), void* arg,
                        Priority pri = LOW, void* tag = nullptr) {
    return env_->Schedule(function, arg, pri, tag);
  }

  virtual int UnSchedule(void* arg, Priority pri) {
    return UnSchedule(arg, pri);
  }

  virtual void StartThread(void (*function)(void* arg), void* arg) {
    env_->StartThread(function, arg);
  }

  virtual void WaitForJoin() {
    env_->WaitForJoin();
  }

  virtual unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const {
    return env_->GetThreadPoolQueueLen(pri);
  }

  virtual Status GetTestDirectory(std::string* path) {
    return env_->GetTestDirectory(path);
  }

  virtual Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result) {
    return env_->NewLogger(fname, result);
  }

  virtual uint64_t NowMicros() {
    return env_->NowMicros();
  }

  virtual uint64_t NowNanos() {
    return env_->NowNanos();
  }

  virtual void SleepForMicroseconds(int micros) {
    return env_->SleepForMicroseconds(micros);
  }

  virtual Status GetHostName(char* name, uint64_t len) {
    return env_->GetHostName(name, len);
  }

  virtual Status GetCurrentTime(int64_t* unix_time) {
    return env_->GetCurrentTime(unix_time);
  }

  virtual Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path) {
    return env_->GetAbsolutePath(db_path, output_path);
  }

  virtual void SetBackgroundThreads(int number, Priority pri = LOW) {
    return env_->SetBackgroundThreads(number, pri);
  }

  virtual void IncBackgroundThreadsIfNeeded(int number, Priority pri) {
    return env_->IncBackgroundThreadsIfNeeded(number, pri);
  }

  virtual void LowerThreadPoolIOPriority(Priority pool = LOW) {
    return env_->LowerThreadPoolIOPriority(pool);
  }

  virtual std::string TimeToString(uint64_t time) {
    return env_->TimeToString(time);
  }

  virtual std::string GenerateUniqueId() {
    return env_->GenerateUniqueId();
  }

  virtual EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,
                                         const DBOptions& db_options) const {
    return env_->OptimizeForLogWrite(env_options, db_options);
  }

  virtual EnvOptions OptimizeForManifestWrite(
    const EnvOptions& env_options) const {
    return env_->OptimizeForManifestWrite(env_options);
  }

  virtual Status GetThreadList(std::vector<ThreadStatus>* thread_list) {
    return env_->GetThreadList(thread_list);
  }

  virtual ThreadStatusUpdater* GetThreadStatusUpdater() const {
    return env_->GetThreadStatusUpdater();
  }

  virtual uint64_t GetThreadID() const {
    return env_->GetThreadID();
  }

private:
  Env* env_ = nullptr;
};

}
