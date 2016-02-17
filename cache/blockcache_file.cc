#include <memory>
#include <iostream>

#include "util/crc32c.h"
#include "cache/blockcache_file.h"

using namespace rocksdb;

using std::unique_ptr;

//
// File creation factories
//
Status NewCacheWritableFile(Env* const env, const std::string & filepath,
                            std::unique_ptr<WritableFile>* file) {
  Status s;

  EnvOptions opt;
  opt.use_os_buffer = true;
  s = env->NewWritableFile(filepath, file, opt);

  return s;
}

Status NewCacheRandomAccessFile(Env* const env, const std::string & filepath,
                                std::unique_ptr<RandomAccessFile>* file) {
  Status s;

  EnvOptions opt;
  opt.use_os_buffer = true;
  s = env->NewRandomAccessFile(filepath, file, opt);

  return s;
}

//
// BlockCacheFile
//
Status BlockCacheFile::Delete(size_t* size) {
  Status status = env_->GetFileSize(Path(), size);
  if (!status.ok()) {
    return status;
  }

  return env_->DeleteFile(Path());
}

//
// CacheRecord
//
struct CacheRecordHeader {
  CacheRecordHeader() {}
  CacheRecordHeader(const uint32_t magic, const uint32_t key_size,
                    const uint32_t val_size)
    : magic_(magic),
      crc_(0),
      key_size_(key_size),
      val_size_(val_size) {}

  uint32_t magic_;
  uint32_t crc_;
  uint32_t key_size_;
  uint32_t val_size_;
};

struct CacheRecord
{
  CacheRecord() {}
  CacheRecord(const Slice& key, const Slice& val)
    : hdr_(MAGIC, static_cast<uint32_t>(key.size()),
           static_cast<uint32_t>(val.size())),
      key_(key),
      val_(val) {
    hdr_.crc_ = ComputeCRC();
  }

  uint32_t ComputeCRC() const;
  bool Serialize(std::vector<CacheWriteBuffer*> & bufs, size_t& woff);
  bool Deserialize(const Slice& buf);

  static size_t CalcSize(const Slice& key, const Slice& val) {
    return sizeof(CacheRecordHeader) + key.size() + val.size();
  }

  static const uint32_t MAGIC = 0xfefa;

  bool Append(std::vector<CacheWriteBuffer*>& bufs, size_t& woff,
              const char* data, const size_t size);

  CacheRecordHeader hdr_;
  Slice key_;
  Slice val_;
};

static_assert(sizeof(CacheRecordHeader) == 16, "DataHeader is not aligned");

uint32_t CacheRecord::ComputeCRC() const
{
  uint32_t crc = 0;
  CacheRecordHeader tmp = hdr_;
  tmp.crc_ = 0;
  crc = crc32c::Extend(crc, (char*) &tmp, sizeof(tmp));
  crc = crc32c::Extend(crc, (char*) key_.data(), key_.size());
  crc = crc32c::Extend(crc, (char*) val_.data(), val_.size());

  return crc;
}

bool CacheRecord::Serialize(std::vector<CacheWriteBuffer*>& bufs, size_t& woff)
{
  assert(bufs.size());
  return Append(bufs, woff, (char*) &hdr_, sizeof(hdr_))
         && Append(bufs, woff, (char*) key_.data(), key_.size())
         && Append(bufs, woff, (char*) val_.data(), val_.size());
}

bool CacheRecord::Append(std::vector<CacheWriteBuffer*>& bufs, size_t& woff,
                         const char* data, const size_t data_size) {
  assert(woff < bufs.size());

  const char* p = data;
  size_t size = data_size;

  for (size_t i = woff; size && i < bufs.size(); ++i) {
    CacheWriteBuffer* buf = bufs[i];
    const size_t free = buf->Free();
    if (size <= free) {
      buf->Append(p, size);
      size = 0;
    } else {
      buf->Append(p, free);
      p += free;
      size -= free;
      assert(!buf->Free());
      assert(buf->Used() == buf->Capacity());
    }

    if (!buf->Free()) {
      woff += 1;
    }
  }

  assert(!size);

  return !size;
}

bool CacheRecord::Deserialize(const Slice& data)
{
  if (data.size() < sizeof(CacheRecordHeader)) {
    return false;
  }

  memcpy(&hdr_, data.data(), sizeof(hdr_));

  if (hdr_.key_size_ + hdr_.val_size_ + sizeof(hdr_) != data.size()) {
    return false;
  }

  key_ = Slice(data.data_ + sizeof(hdr_), hdr_.key_size_);
  val_ = Slice(key_.data_ + hdr_.key_size_, hdr_.val_size_);

  if (!(hdr_.magic_ == MAGIC && ComputeCRC() == hdr_.crc_)) {
    std::cerr << "** magic " << hdr_.magic_ << " " << MAGIC << std::endl;
    std::cerr << "** key_size " << hdr_.key_size_ << std::endl;
    std::cerr << "** val_size " << hdr_.val_size_ << std::endl;
    std::cerr << "** key " << key_.ToString() << std::endl;
    std::cerr << "** val " << val_.ToString() << std::endl;
    for (size_t i = 0; i < hdr_.val_size_; ++i) {
      std::cerr << (uint8_t) val_.data()[i] << ".";
    }
    std::cerr << std::endl;
    std::cerr << "** cksum " << hdr_.crc_ << " " << ComputeCRC() << std::endl;
  }

  return hdr_.magic_ == MAGIC && ComputeCRC() == hdr_.crc_;
}

//
// RandomAccessFile
//

bool RandomAccessCacheFile::Open()
{
  WriteLock _(&rwlock_);
  return OpenImpl();
}

bool RandomAccessCacheFile::OpenImpl() {
  rwlock_.AssertHeld();

  Debug(log_, "Opening cache file %s", Path().c_str());

  Status status = NewCacheRandomAccessFile(env_, Path(), &file_);
  if (!status.ok()) {
    Error(log_, "Error opening random access file %s. %s", Path().c_str(),
          status.ToString().c_str());
    return false;
  }

  return true;
}

bool RandomAccessCacheFile::Read(const LBA& lba, Slice* key, Slice* val,
                                 char* scratch) {
  ReadLock _(&rwlock_);

  assert(lba.cache_id_ == cache_id_);
  assert(file_);

  Slice result;
  Status s = file_->Read(lba.off_, lba.size_, &result, scratch);
  if (!s.ok()) {
    Error(log_, "Error reading from file %s. %s", Path().c_str(),
          s.ToString().c_str());
    return false;
  }

  assert(result.data() == scratch);

  return ParseRec(lba, key, val, scratch);
}

bool RandomAccessCacheFile::ParseRec(const LBA& lba, Slice* key, Slice* val,
                                     char* scratch) {
  Slice data(scratch, lba.size_);

  CacheRecord rec;
  if (!rec.Deserialize(data)) {
    assert(!"Error deserializing data");
    Error(log_, "Error de-serializing record from file %s off %d",
          Path().c_str(), lba.off_);
    return false;
  }

  *key = Slice(rec.key_);
  *val = Slice(rec.val_);

  return true;
}

//
// WriteableCacheFile
//

WriteableCacheFile::~WriteableCacheFile() {
  WriteLock _(&rwlock_);
  if (!eof_) {
    // This file never flushed. We give priority to shutdown since this is a
    // cache
    // TODO: Figure a way to flush the pending data
    assert(file_);

    assert(refs_ == 1);
    --refs_;
  }
  ClearBuffers();
}

bool WriteableCacheFile::Create() {
  WriteLock _(&rwlock_);

  Debug(log_, "Creating new cache %s (max size is %d B)", Path().c_str(),
        max_size_);

  Status s = env_->FileExists(Path());
  if (s.ok()) {
    Warn(log_, "File %s already exists. %s", Path().c_str(),
         s.ToString().c_str());
  }

  s = NewCacheWritableFile(env_, Path(), &file_);
  if (!s.ok()) {
    Warn(log_, "Unable to create file %s. %s", Path().c_str(),
         s.ToString().c_str());
    return false;
  }

  assert(!refs_);
  ++refs_;

  return true;
}

bool WriteableCacheFile::Append(const Slice& key, const Slice& val,
                                LBA* lba) {
  WriteLock _(&rwlock_);

  if (eof_) {
    return false;
  }

  size_t rec_size = CacheRecord::CalcSize(key, val);

  if (!ExpandBuffer(rec_size)) {
    Debug(log_, "Error expanding buffers. size=%d", rec_size);
    return false;
  }

  lba->cache_id_ = cache_id_;
  lba->off_ = disk_woff_;
  lba->size_ = rec_size;

  CacheRecord rec(key, val);
  if (!rec.Serialize(bufs_, buf_woff_)) {
    assert(!"Error serializing record");
    return false;
  }

  disk_woff_ += rec_size;
  eof_ = disk_woff_ >= max_size_;

  DispatchBuffer();

  return true;
}

bool WriteableCacheFile::ExpandBuffer(const size_t size) {
  rwlock_.AssertHeld();
  assert(!eof_);

  size_t free = 0;
  for (size_t i = buf_woff_; i < bufs_.size(); ++i) {
    free += bufs_[i]->Free();
    if (size <= free) {
      return true;
    }
  }

  assert(free < size);
  while (free < size) {
    CacheWriteBuffer* const buf = alloc_.Allocate();
    if (!buf) {
      Debug(log_, "Unable to allocate buffers");
      return false;
    }

    size_ += buf->Free();
    free += buf->Free();
    bufs_.push_back(buf);
  }

  assert(free >= size);
  return true;
}

void WriteableCacheFile::DispatchBuffer() {
  rwlock_.AssertHeld();

  assert(bufs_.size());
  assert(buf_doff_ <= buf_woff_);
  assert(buf_woff_ <= bufs_.size());

  if (pending_ios_) {
    return;
  }

  if (!eof_ && buf_doff_ == buf_woff_) {
    // dispatch buffer is pointing to write buffer and we haven't hit eof
    return;
  }

  assert(eof_ || buf_doff_ < buf_woff_);
  assert(buf_doff_ < bufs_.size());
  assert(file_);

  auto* buf = bufs_[buf_doff_];
  const uint64_t file_off = buf_doff_ * alloc_.BufferSize(); 

  assert(!buf->Free() || (eof_ && buf_doff_ == buf_woff_
                          && buf_woff_ < bufs_.size()));
  // we have reached end of file, and there is space in the last buffer
  // pad it with zero for direct IO
  buf->FillTrailingZeros();

  assert(buf->Used() % (4 * 1024) == 0);

  writer_.Write(this, buf, file_off);
  pending_ios_++;
  buf_doff_++;
}

void WriteableCacheFile::BufferWriteDone(CacheWriteBuffer* const buf) {
  WriteLock _(&rwlock_);

  assert(bufs_.size());

  pending_ios_--;

  if (buf_doff_ < bufs_.size()) {
    DispatchBuffer();
  }

  if (eof_ && buf_doff_ >= bufs_.size() && !pending_ios_) {
    // end-of-file reached, all buffers are dispatched and all IOs are complete
    Close();
    OpenImpl();
  }
}

bool WriteableCacheFile::ReadImpl(const LBA& lba, Slice* key, Slice* block,
                                  char* scratch)
{
  if (!ReadBuffer(lba, scratch)) {
    Error(log_, "Error reading from buffer. cache=%d off=%d", cache_id_,
          lba.off_);
    return false;
  }

  return ParseRec(lba, key, block, scratch);
}

bool WriteableCacheFile::ReadBuffer(const LBA& lba, char* data)
{
  rwlock_.AssertHeld();

  assert(lba.off_ < disk_woff_);

  char* tmp = data;
  size_t pending_nbytes = lba.size_;
  size_t start_idx = lba.off_ / alloc_.BufferSize();
  size_t start_off = lba.off_ % alloc_.BufferSize();

  assert(start_idx <= buf_woff_);

  for (size_t i = start_idx; pending_nbytes && i < bufs_.size(); ++i) {
    assert(i <= buf_woff_);
    auto* buf = bufs_[i];
    assert(i == buf_woff_ || !buf->Free());
    size_t nbytes = pending_nbytes > (buf->Used() - start_off) ?
                                (buf->Used() - start_off) : pending_nbytes;
    memcpy(tmp, buf->Data() + start_off, nbytes);

    pending_nbytes -= nbytes;
    start_off = 0;
    tmp += nbytes;
  }

  assert(!pending_nbytes);
  if (pending_nbytes) {
    return false;
  }

  assert(tmp == data + lba.size_);

  return true;
}

void WriteableCacheFile::Close() {
  assert(size_ >= max_size_);
  assert(disk_woff_ >= max_size_);
  assert(buf_doff_ == bufs_.size());

  Info(log_, "Closing file %s. size=%d written=%d", Path().c_str(), size_,
       disk_woff_);

  ClearBuffers();
  file_.reset();

  assert(refs_);
  --refs_;
}

void WriteableCacheFile::ClearBuffers() {
  for (size_t i = 0; i < bufs_.size(); ++i) {
    alloc_.Deallocate(bufs_[i]);
  }

  bufs_.clear();
}
