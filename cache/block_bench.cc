#include <functional>
#include <iostream>
#include <thread>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <cache/cache_util.h>
#include <util/histogram.h>


using namespace std;

using GFLAGS::ParseCommandLineFlags;
using GFLAGS::RegisterFlagValidator;
using GFLAGS::SetUsageMessage;

DEFINE_string(path, "<path>", "Path to device");
DEFINE_int32(size, 1, "Size in GB");
DEFINE_int32(read_numthreads, 1, "Number of threads for reading");
DEFINE_int32(read_iosize, 4 * 1024, "IO size of reads");
DEFINE_int32(write_numthreads, 1, "Number of threads for writing");
DEFINE_int32(write_iosize, 4 * 1024, "IO size of write");
DEFINE_string(read_pattern, "random", "Read pattern -- seq/random");
DEFINE_string(write_pattern, "random", "Write pattern -- seq/random");
DEFINE_int32(nsec, 60, "Number of seconds to run the test");
DEFINE_bool(disable_directio, false, "Disable direct IO");

namespace rocksdb {

static int fd;
static Timer timer; 
static HistogramImpl stat_read_response;
static HistogramImpl stat_write_reponse;
static std::function<uint64_t(uint64_t)> read_next_off;
static std::function<uint64_t(uint64_t)> write_next_off;
static std::atomic<uint64_t> read_ios{0};
static std::atomic<uint64_t> write_ios{0};

size_t RoundUp(const size_t val, const size_t alignment) {
  if (val % alignment) {
    return  val + (alignment - val % alignment);
  } else {
    return val;
  }
}

void Open() {
  int flags = O_RDWR | O_CREAT;
  if (!FLAGS_disable_directio) {
    flags |= O_DIRECT;
  }

  fd = open(FLAGS_path.c_str(), flags, 0644);
  if (fd == -1) {
    cerr << strerror(errno) << endl;
    abort();
  }

  cout << "opened " << FLAGS_path << endl;
}

void Close() {
  close(fd);
}

uint64_t RandNextOff(const uint64_t max) {
  return (rand() + 1) % max;
}

uint64_t SeqNextOff(atomic<uint64_t>& off, const uint64_t max) {
  auto ret = ++off;
  if (ret >= max) {
    ret = off = 0;
  }
  return ret;
}

uint64_t SeqReadNextOff(const uint64_t max) {
  static atomic<uint64_t> off{0};
  return SeqNextOff(off, max);
}

uint64_t SeqWriteNextOff(const uint64_t max) {
  static atomic<uint64_t> off{0};
  return SeqNextOff(off, max);
}

void Read() {
  void* buf = nullptr;
  if (posix_memalign(&buf, /*alignment=*/ 512, FLAGS_read_iosize) != 0) {
    cerr << "posix_memalign:" << strerror(errno) << endl;
    abort();
  }

  const uint64_t max_off = FLAGS_size * 1073741824ULL / FLAGS_read_iosize;

  while (timer.ElapsedSec() < size_t(FLAGS_nsec)) {
    size_t off = (rand() + 1) % max_off;
    Timer t;
    int status = pread(fd, buf, FLAGS_read_iosize, off * FLAGS_read_iosize);
    stat_read_response.Add(t.ElapsedMicroSec());
    if (status == -1) {
      cerr << "pread: " << strerror(errno) << endl;
      abort();
    }
    ++read_ios;
  }

  free(buf);
}

void Write() {
  void* buf = nullptr;
  if (posix_memalign(&buf, /*alignment=*/ 512, FLAGS_write_iosize) != 0) {
    cerr << "posix_memalign:" << strerror(errno) << endl;
    abort();
  }

  const uint64_t max_off = FLAGS_size * 1073741824ULL / FLAGS_write_iosize;

  while (timer.ElapsedSec() < size_t(FLAGS_nsec)) {
    size_t off = (rand() + 1) % max_off;
    Timer t;
    int status = pwrite(fd, buf, FLAGS_write_iosize, off* FLAGS_write_iosize);
    stat_write_reponse.Add(t.ElapsedMicroSec());
    if (status == -1) {
      cerr << "pwrite: " << strerror(errno) << endl;
      abort();
    }
    ++write_ios;
  }

  free(buf);
}

int RunBenchmark() {
  if (FLAGS_read_pattern == "random") {
    read_next_off = RandNextOff;
  } else if (FLAGS_read_pattern == "seq") {
    read_next_off = SeqReadNextOff;
  } else {
    cerr << "unknown read pattern" << endl;
    abort();
  }

  if (FLAGS_write_pattern == "random") {
    write_next_off = RandNextOff;
  } else if (FLAGS_write_pattern == "seq") {
    write_next_off = SeqWriteNextOff;
  } else {
    cerr << "unknown write pattern" << endl;
    abort();
  }

  Open();

  std::vector<std::thread> threads;
  for (auto i = 0; i < FLAGS_read_numthreads; ++i) {
    std::thread th(Read);
    threads.push_back(std::move(th));
  }

  for (auto i = 0; i < FLAGS_write_numthreads; ++i) {
    std::thread th(Write);
    threads.push_back(std::move(th));
  }

  for (auto it = threads.begin(); it != threads.end(); ++it) {
    it->join();
  }

  Close();

  const double nsec = timer.ElapsedSec();

  if (!nsec) {
    abort();
  }

  cout << "Results:" << endl
       << "========" << endl
       << "read IOps:"
       << (read_ios / nsec) << endl
       << "read MBps:"
       << (read_ios * FLAGS_read_iosize / nsec) / 1048576ULL << endl
       << "write IOps:"
       << (write_ios / nsec) << endl
       << "write MBps:"
       << (write_ios * FLAGS_write_iosize / nsec) / 1048576ULL << " MBps" << endl
       << "read response:"
       << stat_read_response.ToString() << endl
       << "write response:"
       << stat_write_reponse.ToString() << endl;

  return 0;
}

}

int
main(int argc, char** argv) {
  SetUsageMessage(std::string("\nUSAGE:\n") + std::string(argv[0]) +
                  " [OPTIONS]...");
  ParseCommandLineFlags(&argc, &argv, true);

  cout << "Config:" << endl
       << "=======" << endl
       << "path=" << FLAGS_path << endl
       << "size=" << FLAGS_size << " GB" << endl
       << "read_iosize=" << FLAGS_read_iosize << " B" << endl
       << "read_numthreads=" << FLAGS_read_numthreads << endl
       << "write_iosize=" << FLAGS_write_iosize << " B" << endl
       << "write_numthreads=" << FLAGS_write_numthreads << endl
       << "read_pattern=" << FLAGS_read_pattern << endl
       << "write_pattern=" << FLAGS_write_pattern << endl
       << "disable_directio=" << FLAGS_disable_directio << endl
       << "nsec=" << FLAGS_nsec << endl << endl;

  return rocksdb::RunBenchmark();
}
