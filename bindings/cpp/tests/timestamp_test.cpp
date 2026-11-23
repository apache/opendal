/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <optional>
#include <string>

#ifdef _WIN32
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <fcntl.h>
#include <sys/stat.h>
#endif

#include "gtest/gtest.h"
#include "opendal.hpp"

namespace {

std::chrono::system_clock::time_point MakeTimePoint(std::int64_t seconds,
                                                    std::int64_t nanoseconds) {
  auto duration = std::chrono::seconds(seconds) +
                  std::chrono::nanoseconds(nanoseconds);
  return std::chrono::system_clock::time_point(
      std::chrono::duration_cast<std::chrono::system_clock::duration>(
          duration));
}

class ScopedTempDir {
 public:
  ScopedTempDir() {
    static std::atomic<unsigned> next_id{0};
    auto name = "opendal-cpp-timestamp-" +
                std::to_string(std::chrono::steady_clock::now()
                                   .time_since_epoch()
                                   .count()) +
                "-" + std::to_string(next_id.fetch_add(1));
    path_ = std::filesystem::temp_directory_path() / name;
    std::filesystem::create_directories(path_);
  }

  ~ScopedTempDir() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }

  ScopedTempDir(const ScopedTempDir &) = delete;
  ScopedTempDir &operator=(const ScopedTempDir &) = delete;

  const std::filesystem::path &path() const { return path_; }

 private:
  std::filesystem::path path_;
};

// Runs a test in a fixed non-UTC timezone and restores TZ afterwards.
class ScopedTimeZone {
 public:
  explicit ScopedTimeZone(const char *timezone) {
    if (const char *current = std::getenv("TZ"); current != nullptr) {
      previous_ = current;
    }

#ifdef _WIN32
    _putenv_s("TZ", timezone);
    _tzset();
#else
    setenv("TZ", timezone, 1);
    tzset();
#endif
  }

  ~ScopedTimeZone() {
#ifdef _WIN32
    _putenv_s("TZ", previous_.has_value() ? previous_->c_str() : "");
    _tzset();
#else
    if (previous_.has_value()) {
      setenv("TZ", previous_->c_str(), 1);
    } else {
      unsetenv("TZ");
    }
    tzset();
#endif
  }

  ScopedTimeZone(const ScopedTimeZone &) = delete;
  ScopedTimeZone &operator=(const ScopedTimeZone &) = delete;

 private:
  std::optional<std::string> previous_;
};

void SetModificationTime(const std::filesystem::path &path,
                         std::chrono::system_clock::time_point time) {
#ifdef _WIN32
  auto nanoseconds_since_epoch =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          time.time_since_epoch())
          .count();
  auto filetime = nanoseconds_since_epoch / 100 + 116444736000000000LL;
  ULARGE_INTEGER value;
  value.QuadPart = static_cast<ULONGLONG>(filetime);
  FILETIME timestamp{value.LowPart, value.HighPart};

  HANDLE handle = CreateFileW(
      path.wstring().c_str(), FILE_WRITE_ATTRIBUTES,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, nullptr,
      OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  ASSERT_NE(handle, INVALID_HANDLE_VALUE);
  EXPECT_TRUE(SetFileTime(handle, nullptr, nullptr, &timestamp));
  CloseHandle(handle);
#else
  auto since_epoch = time.time_since_epoch();
  auto seconds = std::chrono::duration_cast<std::chrono::seconds>(since_epoch);
  auto nanoseconds =
      std::chrono::duration_cast<std::chrono::nanoseconds>(since_epoch -
                                                           seconds);
  if (nanoseconds.count() < 0) {
    seconds -= std::chrono::seconds(1);
    nanoseconds += std::chrono::seconds(1);
  }

  struct timespec times[2];
  times[0].tv_sec = 0;
  times[0].tv_nsec = UTIME_OMIT;
  times[1].tv_sec = static_cast<time_t>(seconds.count());
  times[1].tv_nsec = static_cast<long>(nanoseconds.count());
  ASSERT_EQ(utimensat(AT_FDCWD, path.c_str(), times, 0), 0);
#endif
}

class TimestampTest : public ::testing::Test {
 protected:
  void SetUp() override {
    op_ = opendal::Operator("fs", {{"root", dir_.path().string()}});
    ASSERT_TRUE(op_.Available());
  }

  ScopedTempDir dir_;
  opendal::Operator op_;
};

}  // namespace

TEST_F(TimestampTest, LastModifiedIsUtcWithSubseconds) {
  // UTC+9 exposes any conversion that treats the UTC timestamp as local time.
  ScopedTimeZone timezone("ZZZ-9");

  const std::string path = "timestamp.txt";
  op_.Write(path, "timestamp content");

  auto expected = MakeTimePoint(1'700'000'000, 123'456'000);
  SetModificationTime(dir_.path() / path, expected);

  auto metadata = op_.Stat(path);

  ASSERT_TRUE(metadata.LastModified().has_value());
  EXPECT_EQ(metadata.LastModified()->time_since_epoch(),
            expected.time_since_epoch());
}

TEST_F(TimestampTest, LastModifiedRoundTripsThroughConditions) {
  ScopedTimeZone timezone("ZZZ-9");

  const std::string path = "timestamp.txt";
  const std::string content = "timestamp content";
  op_.Write(path, content);

  auto expected = MakeTimePoint(1'700'000'000, 123'456'000);
  SetModificationTime(dir_.path() / path, expected);

  auto metadata = op_.Stat(path);
  ASSERT_TRUE(metadata.LastModified().has_value());

  opendal::ReadOptions options;
  options.if_unmodified_since = metadata.LastModified();
  EXPECT_EQ(op_.Read(path, options), content);

  options.if_unmodified_since =
      *metadata.LastModified() - std::chrono::seconds(1);
  EXPECT_THROW(op_.Read(path, options), std::exception);
}
