//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <string>
#include <cmath>
#include "util/testharness.h"
#include "util/auto_roll_logger.h"
#include "rocksdb/db.h"
#include <sys/stat.h>
#include <errno.h>
#include <iostream>

using namespace std;

namespace rocksdb {

class AutoRollLoggerTest {
 public:
  static void InitTestDb() {
    string deleteCmd = "rm -rf " + kTestDir;
    ASSERT_TRUE(system(deleteCmd.c_str()) == 0);
    Env::Default()->CreateDir(kTestDir);
  }

  void RollLogFileBySizeTest(AutoRollLogger* logger,
                             size_t log_max_size,
                             const string& log_message);
  uint64_t RollLogFileByTimeTest(AutoRollLogger* logger,
                                 size_t time,
                                 const string& log_message);

  static const string kSampleMessage;
  static const string kTestDir;
  static const string kLogFile;
  static Env* env;
};

const string AutoRollLoggerTest::kSampleMessage(
    "this is the message to be written to the log file!!");
const string AutoRollLoggerTest::kTestDir(test::TmpDir() + "/db_log_test");
const string AutoRollLoggerTest::kLogFile(test::TmpDir() + "/db_log_test/LOG");
Env* AutoRollLoggerTest::env = Env::Default();

// In this test we only want to Log some simple log message with
// no format. LogMessage() provides such a simple interface and
// avoids the [format-security] warning which occurs when you
// call Log(logger, log_message) directly.
void LogMessage(Logger* logger, const char* message) {
  Log(logger, "%s", message);
}

void LogMessage(const InfoLogLevel log_level, Logger* logger,
                const char* message) {
  Log(log_level, logger, "%s", message);
}

void GetFileCreateTime(const std::string& fname, uint64_t* file_ctime) {
  struct stat s;
  if (stat(fname.c_str(), &s) != 0) {
    *file_ctime = (uint64_t)0;
  }
  *file_ctime = static_cast<uint64_t>(s.st_ctime);
}

void AutoRollLoggerTest::RollLogFileBySizeTest(AutoRollLogger* logger,
                                               size_t log_max_size,
                                               const string& log_message) {
  logger->SetInfoLogLevel(InfoLogLevel::INFO);
  // measure the size of each message, which is supposed
  // to be equal or greater than log_message.size()
  LogMessage(logger, log_message.c_str());
  size_t message_size = logger->GetLogFileSize();
  size_t current_log_size = message_size;

  // Test the cases when the log file will not be rolled.
  while (current_log_size + message_size < log_max_size) {
    LogMessage(logger, log_message.c_str());
    current_log_size += message_size;
    ASSERT_EQ(current_log_size, logger->GetLogFileSize());
  }

  // Now the log file will be rolled
  LogMessage(logger, log_message.c_str());
  // Since rotation is checked before actual logging, we need to
  // trigger the rotation by logging another message.
  LogMessage(logger, log_message.c_str());

  ASSERT_TRUE(message_size == logger->GetLogFileSize());
}

uint64_t AutoRollLoggerTest::RollLogFileByTimeTest(
    AutoRollLogger* logger, size_t time, const string& log_message) {
  uint64_t expected_create_time;
  uint64_t actual_create_time;
  uint64_t total_log_size;
  ASSERT_OK(env->GetFileSize(kLogFile, &total_log_size));
  GetFileCreateTime(kLogFile, &expected_create_time);
  logger->SetCallNowMicrosEveryNRecords(0);

  // -- Write to the log for several times, which is supposed
  // to be finished before time.
  for (int i = 0; i < 10; ++i) {
     LogMessage(logger, log_message.c_str());
     ASSERT_OK(logger->GetStatus());
     // Make sure we always write to the same log file (by
     // checking the create time);
     GetFileCreateTime(kLogFile, &actual_create_time);

     // Also make sure the log size is increasing.
     ASSERT_EQ(expected_create_time, actual_create_time);
     ASSERT_GT(logger->GetLogFileSize(), total_log_size);
     total_log_size = logger->GetLogFileSize();
  }

  // -- Make the log file expire
  sleep(time);
  LogMessage(logger, log_message.c_str());

  // At this time, the new log file should be created.
  GetFileCreateTime(kLogFile, &actual_create_time);
  ASSERT_GT(actual_create_time, expected_create_time);
  ASSERT_LT(logger->GetLogFileSize(), total_log_size);
  expected_create_time = actual_create_time;

  return expected_create_time;
}

TEST(AutoRollLoggerTest, RollLogFileBySize) {
    InitTestDb();
    size_t log_max_size = 1024 * 5;

    AutoRollLogger logger(Env::Default(), kTestDir, "", log_max_size, 0);

    RollLogFileBySizeTest(&logger, log_max_size,
                          kSampleMessage + ":RollLogFileBySize");
}

TEST(AutoRollLoggerTest, RollLogFileByTime) {
    size_t time = 1;
    size_t log_size = 1024 * 5;

    InitTestDb();
    // -- Test the existence of file during the server restart.
    ASSERT_TRUE(!env->FileExists(kLogFile));
    AutoRollLogger logger(Env::Default(), kTestDir, "", log_size, 1);
    ASSERT_TRUE(env->FileExists(kLogFile));

    RollLogFileByTimeTest(&logger, time, kSampleMessage + ":RollLogFileByTime");
}

TEST(AutoRollLoggerTest,
     OpenLogFilesMultipleTimesWithOptionLog_max_size) {
  // If only 'log_max_size' options is specified, then every time
  // when rocksdb is restarted, a new empty log file will be created.
  InitTestDb();
  // WORKAROUND:
  // avoid complier's complaint of "comparison between signed
  // and unsigned integer expressions" because literal 0 is
  // treated as "singed".
  size_t kZero = 0;
  size_t log_size = 1024;

  AutoRollLogger* logger = new AutoRollLogger(
    Env::Default(), kTestDir, "", log_size, 0);

  LogMessage(logger, kSampleMessage.c_str());
  ASSERT_GT(logger->GetLogFileSize(), kZero);
  delete logger;

  // reopens the log file and an empty log file will be created.
  logger = new AutoRollLogger(
    Env::Default(), kTestDir, "", log_size, 0);
  ASSERT_EQ(logger->GetLogFileSize(), kZero);
  delete logger;
}

TEST(AutoRollLoggerTest, CompositeRollByTimeAndSizeLogger) {
  size_t time = 1, log_max_size = 1024 * 5;

  InitTestDb();

  AutoRollLogger logger(Env::Default(), kTestDir, "", log_max_size, time);

  // Test the ability to roll by size
  RollLogFileBySizeTest(
      &logger, log_max_size,
      kSampleMessage + ":CompositeRollByTimeAndSizeLogger");

  // Test the ability to roll by Time
  RollLogFileByTimeTest( &logger, time,
      kSampleMessage + ":CompositeRollByTimeAndSizeLogger");
}

TEST(AutoRollLoggerTest, CreateLoggerFromOptions) {
  Options options;
  shared_ptr<Logger> logger;

  // Normal logger
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, "", env, options, &logger));
  ASSERT_TRUE(dynamic_cast<PosixLogger*>(logger.get()));

  // Only roll by size
  InitTestDb();
  options.max_log_file_size = 1024;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, "", env, options, &logger));
  AutoRollLogger* auto_roll_logger =
    dynamic_cast<AutoRollLogger*>(logger.get());
  ASSERT_TRUE(auto_roll_logger);
  RollLogFileBySizeTest(
      auto_roll_logger, options.max_log_file_size,
      kSampleMessage + ":CreateLoggerFromOptions - size");

  // Only roll by Time
  InitTestDb();
  options.max_log_file_size = 0;
  options.log_file_time_to_roll = 1;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, "", env, options, &logger));
  auto_roll_logger =
    dynamic_cast<AutoRollLogger*>(logger.get());
  RollLogFileByTimeTest(
      auto_roll_logger, options.log_file_time_to_roll,
      kSampleMessage + ":CreateLoggerFromOptions - time");

  // roll by both Time and size
  InitTestDb();
  options.max_log_file_size = 1024 * 5;
  options.log_file_time_to_roll = 1;
  ASSERT_OK(CreateLoggerFromOptions(kTestDir, "", env, options, &logger));
  auto_roll_logger =
    dynamic_cast<AutoRollLogger*>(logger.get());
  RollLogFileBySizeTest(
      auto_roll_logger, options.max_log_file_size,
      kSampleMessage + ":CreateLoggerFromOptions - both");
  RollLogFileByTimeTest(
      auto_roll_logger, options.log_file_time_to_roll,
      kSampleMessage + ":CreateLoggerFromOptions - both");
}

TEST(AutoRollLoggerTest, InfoLogLevel) {
  InitTestDb();
  // the lengths of DEBUG, INFO, WARN, ERROR, FATAL respectively
  const int kInfoLogLevelNameLens[5] = {5, 4, 4, 5, 5};

  size_t log_size = 8192;
  AutoRollLogger* logger =
      new AutoRollLogger(Env::Default(), kTestDir, "", log_size, 0);

  int message_length = kSampleMessage.length();
  int log_length = 0;
  int total_logname_length = 0;
  for (int log_level = InfoLogLevel::FATAL; log_level >= InfoLogLevel::DEBUG;
       log_level--) {
    logger->SetInfoLogLevel((InfoLogLevel)log_level);
    total_logname_length += kInfoLogLevelNameLens[log_level];
    for (int log_type = InfoLogLevel::DEBUG; log_type <= InfoLogLevel::FATAL;
         log_type++) {
      // log messages with log level smaller than log_level will not be logged.
      LogMessage((InfoLogLevel)log_type, logger, kSampleMessage.c_str());
    }
    // 44 is the length of the message excluding the actual
    // message and log name.
    log_length += (message_length + 44) * (InfoLogLevel::FATAL - log_level + 1);
    log_length += total_logname_length;
    ASSERT_EQ(logger->GetLogFileSize(), log_length);
  }

  // rerun the test but using different log functions.
  total_logname_length = 0;
  for (int log_level = InfoLogLevel::FATAL; log_level >= InfoLogLevel::DEBUG;
       log_level--) {
    logger->SetInfoLogLevel((InfoLogLevel)log_level);
    total_logname_length += kInfoLogLevelNameLens[log_level];

    // again, messages with level smaller than log_level will not be logged.
    Debug(logger, "%s", kSampleMessage.c_str());
    Info(logger, "%s", kSampleMessage.c_str());
    Warn(logger, "%s", kSampleMessage.c_str());
    Error(logger, "%s", kSampleMessage.c_str());
    Fatal(logger, "%s", kSampleMessage.c_str());
    // 44 is the length of the message excluding the actual
    // message and log name.
    log_length += (message_length + 44) * (InfoLogLevel::FATAL - log_level + 1);
    log_length += total_logname_length;
    ASSERT_EQ(logger->GetLogFileSize(), log_length);
  }
}

int OldLogFileCount(const string& dir) {
  std::vector<std::string> files;
  Env::Default()->GetChildren(dir, &files);
  int log_file_count = 0;

  for (std::vector<std::string>::iterator it = files.begin();
       it != files.end(); ++it) {
    uint64_t create_time;
    FileType type;
    if (!ParseFileName(*it, &create_time, &type)) {
      continue;
    }
    if (type == kInfoLogFile && create_time > 0) {
      ++log_file_count;
    }
  }

  return log_file_count;
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  return rocksdb::test::RunAllTests();
}
