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

#include <ctime>
#include <iterator>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>

#include "framework/test_framework.hpp"

namespace opendal::test {

class OpenDALBasicTest : public ::testing::Test {
 protected:
  opendal::Operator op_;

  std::string scheme;
  std::unordered_map<std::string, std::string> config;

  // random number generator
  std::mt19937 rng;

  void SetUp() override {
    scheme = "memory";
    rng.seed(time(nullptr));

    op_ = opendal::Operator(scheme, config);
    EXPECT_TRUE(op_.Available());
  }
};

// Scenario: OpenDAL Blocking Operations
OPENDAL_TEST_F(OpenDALTest, BasicTest) {
  std::string file_path = "test";
  std::string dir_path = "test_dir/";
  std::string_view data = "abc";

  // write
  op_.Write(file_path, data);

  // read
  auto res = op_.Read(file_path);
  EXPECT_EQ(res, data);

  // check existence
  EXPECT_TRUE(op_.Exists(file_path));

  // create directory
  op_.CreateDir(dir_path);
  EXPECT_TRUE(op_.Exists(dir_path));

  // get metadata
  auto metadata = op_.Stat(file_path);
  EXPECT_EQ(metadata.type, opendal::EntryMode::FILE);
  EXPECT_EQ(metadata.content_length, data.size());

  // list entries
  auto list_file_path = dir_path + file_path;
  op_.Write(list_file_path, data);
  auto entries = op_.List(dir_path);
  EXPECT_EQ(entries.size(), 2);
  std::unordered_set<std::string> paths;
  for (const auto &entry : entries) {
    paths.insert(entry.path);
  }
  EXPECT_TRUE(paths.find(dir_path) != paths.end());
  EXPECT_TRUE(paths.find(list_file_path) != paths.end());

  // remove files and the empty directory
  op_.Remove(file_path);
  op_.Remove(list_file_path);
  op_.Remove(dir_path);
  EXPECT_FALSE(op_.Exists(file_path));
  EXPECT_FALSE(op_.Exists(list_file_path));
  EXPECT_FALSE(op_.Exists(dir_path));
}

OPENDAL_TEST_F(OpenDALTest, ReaderTest) {
  std::string file_path = "test";
  constexpr int size = 2000;
  std::string data(size, 0);

  for (auto &d : data) {
    d = rng_() % 256;
  }

  // write
  op_.Write(file_path, data);

  // get reader
  auto reader = op_.GetReader(file_path);
  // uint8_t part_data[100];
  std::string part_data(100, 0);
  reader.Seek(200, std::ios::cur);
  reader.Read(part_data.data(), 100);
  EXPECT_EQ(reader.Seek(0, std::ios::cur), 300);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(part_data[i], data[200 + i]);
  }

  std::string positional_data(100, 0);
  auto positional_read_size = reader.ReadAt(positional_data.data(), /*size=*/ 100, /*offset=*/500);
  EXPECT_EQ(positional_read_size, 100);
  EXPECT_EQ(reader.Seek(0, std::ios::cur), 300);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(positional_data[i], data[500 + i]);
  }
  reader.Seek(0, std::ios::beg);

  // reader stream
  opendal::ReaderStream stream(op_.GetReader(file_path));

  auto read_fn = [&](std::size_t to_read, std::streampos expected_tellg) {
    std::vector<char> v(to_read);
    stream.read(v.data(), v.size());
    EXPECT_TRUE(!!stream);
    EXPECT_EQ(stream.tellg(), expected_tellg);
  };

  EXPECT_EQ(stream.tellg(), 0);
  read_fn(10, 10);
  read_fn(15, 25);
  read_fn(15, 40);
  stream.get();
  EXPECT_EQ(stream.tellg(), 41);
  read_fn(1000, 1041);

  stream.seekg(0, std::ios::beg);
  std::string reader_data(std::istreambuf_iterator<char>{stream}, {});
  EXPECT_EQ(reader_data, data);
}

OPENDAL_TEST_F(OpenDALTest, WriterTest) {
  auto writer = op_.GetWriter("writer_test");
  writer.Write("hello ");
  writer.Write("world");
  writer.Flush();
  writer.Close();

  EXPECT_EQ(op_.Read("writer_test"), "hello world");
}

OPENDAL_TEST_F(OpenDALTest, ListerTest) {
  std::string dir_path = "test_dir/";
  op_.CreateDir(dir_path);
  auto test1_path = dir_path + "test1";
  op_.Write(test1_path, "123");
  auto test2_path = dir_path + "test2";
  op_.Write(test2_path, "456");

  auto lister = op_.GetLister("test_dir/");

  std::unordered_set<std::string> paths;
  for (const auto &entry : lister) {
    paths.insert(entry.path);
  }
  EXPECT_EQ(paths.size(), 3);
  EXPECT_TRUE(paths.find(dir_path) != paths.end());
  EXPECT_TRUE(paths.find(test1_path) != paths.end());
  EXPECT_TRUE(paths.find(test2_path) != paths.end());
}

TEST(OpenDALOptionsTest, ReadOptions) {
  opendal::Operator op("memory");
  op.Write("read_options", "0123456789");

  opendal::ReadOptions options;
  options.range = opendal::ReadRange::Range(2, 5);
  EXPECT_EQ(op.Read("read_options", options), "234");

  options.range = opendal::ReadRange::Offset(7);
  EXPECT_EQ(op.Read("read_options", options), "789");

  options.range = opendal::ReadRange::Suffix(2);
  EXPECT_EQ(op.Read("read_options", options), "89");
}

TEST(OpenDALOptionsTest, ReaderOptions) {
  opendal::Operator op("memory");
  op.Write("reader_options", "0123456789");

  opendal::ReaderOptions options;
  options.content_length_hint = 10;
  options.concurrent = 2;
  options.chunk = 4;
  options.prefetch = 1;

  auto reader = op.GetReader("reader_options", options);
  std::string data(4, 0);
  EXPECT_EQ(reader.Read(data.data(), data.size()), 4);
  EXPECT_EQ(data, "0123");
}

TEST(OpenDALOptionsTest, WriteOptions) {
  opendal::Operator op("memory");

  opendal::WriteOptions options;
  options.cache_control = "max-age=60";
  options.content_type = "text/plain";
  options.content_disposition = "inline";
  options.content_encoding = "identity";
  options.user_metadata = std::unordered_map<std::string, std::string>{
      {"owner", "cpp-test"},
  };
  options.if_not_exists = true;
  options.concurrent = 2;
  options.chunk = 1024;

  op.Write("write_options", "hello", options);

  auto metadata = op.Stat("write_options");
  ASSERT_TRUE(metadata.cache_control.has_value());
  EXPECT_EQ(*metadata.cache_control, "max-age=60");
  ASSERT_TRUE(metadata.content_type.has_value());
  EXPECT_EQ(*metadata.content_type, "text/plain");
  ASSERT_TRUE(metadata.content_disposition.has_value());
  EXPECT_EQ(*metadata.content_disposition, "inline");
  ASSERT_TRUE(metadata.content_encoding.has_value());
  EXPECT_EQ(*metadata.content_encoding, "identity");

  EXPECT_THROW(op.Write("write_options", "overwrite", options), std::exception);
}

TEST(OpenDALOptionsTest, WriterOptions) {
  opendal::Operator op("memory");

  opendal::WriteOptions options;
  options.content_type = "application/octet-stream";
  options.chunk = 1024;

  auto writer = op.GetWriter("writer_options", options);
  writer.Write("hello ");
  writer.Write("world");
  writer.Close();

  EXPECT_EQ(op.Read("writer_options"), "hello world");
  auto metadata = op.Stat("writer_options");
  ASSERT_TRUE(metadata.content_type.has_value());
  EXPECT_EQ(*metadata.content_type, "application/octet-stream");
}

TEST(OpenDALOptionsTest, ListOptions) {
  opendal::Operator op("memory");
  op.CreateDir("list_options/");
  op.CreateDir("list_options/nested/");
  op.Write("list_options/nested/marker", "hello");
  op.Write("list_options/nested/result", "hello");

  opendal::ListOptions options;
  options.recursive = true;
  options.limit = 16;
  options.start_after = "list_options/nested/marker";

  auto entries = op.List("list_options/", options);
  std::unordered_set<std::string> paths;
  for (const auto &entry : entries) {
    paths.insert(entry.path);
  }
  EXPECT_THAT(paths,
              ::testing::UnorderedElementsAre("list_options/nested/result"));

  auto lister = op.GetLister("list_options/", options);
  paths.clear();
  for (const auto &entry : lister) {
    paths.insert(entry.path);
  }
  EXPECT_THAT(paths,
              ::testing::UnorderedElementsAre("list_options/nested/result"));
}

TEST(OpenDALOptionsTest, ListOptionsUnsupportedByService) {
  using ::testing::HasSubstr;
  using ::testing::ThrowsMessage;

  opendal::Operator op("dashmap");
  op.Write("list_unsupported/file", "hello");

  for (bool recursive : {false, true}) {
    opendal::ListOptions start_after;
    start_after.start_after = "list_unsupported/file";
    start_after.recursive = recursive;
    EXPECT_THAT([&] { op.List("list_unsupported/", start_after); },
                ThrowsMessage<std::exception>(
                    HasSubstr("list with the arguments start_after")));
  }

  opendal::ListOptions versions;
  versions.versions = true;
  EXPECT_THAT([&] { op.List("list_unsupported/", versions); },
              ThrowsMessage<std::exception>(
                  HasSubstr("list with the arguments versions")));

  opendal::ListOptions deleted;
  deleted.deleted = true;
  EXPECT_THAT([&] { op.List("list_unsupported/", deleted); },
              ThrowsMessage<std::exception>(
                  HasSubstr("list with the arguments deleted")));
}

TEST(OpenDALOptionsTest, DeleteOptions) {
  opendal::Operator op("memory");
  op.CreateDir("delete_options/");
  op.Write("delete_options/file", "hello");

  opendal::DeleteOptions options;
  options.recursive = true;
  op.Remove("delete_options/", options);

  EXPECT_FALSE(op.Exists("delete_options/file"));
}

TEST(OpenDALOptionsTest, RemoveAll) {
  opendal::Operator op("memory");
  std::vector<std::string> paths;
  for (int index = 0; index < 8; ++index) {
    auto path = "remove_all/file_" + std::to_string(index);
    op.Write(path, "hello");
    EXPECT_TRUE(op.Exists(path));
    paths.push_back(path);
  }

  op.RemoveAll(paths);

  for (const auto &path : paths) {
    EXPECT_FALSE(op.Exists(path));
  }
}

TEST(OpenDALOptionsTest, StatOptions) {
  opendal::Operator op("memory");
  op.Write("stat_options", "hello");

  opendal::StatOptions stat_options;
  auto metadata = op.Stat("stat_options", stat_options);
  EXPECT_EQ(metadata.content_length, 5);
}

TEST(OpenDALReaderTest, ReadAndReadAtValidateArguments) {
  opendal::Operator op("memory");
  op.Write("reader_validation", "hello");

  auto reader = op.GetReader("reader_validation");
  char buf[4] = {};

  EXPECT_THROW(reader.Read(buf, -1), std::invalid_argument);
  EXPECT_THROW(reader.ReadAt(buf, -1, 0), std::invalid_argument);
  EXPECT_THROW(reader.Read(nullptr, 1), std::invalid_argument);
  EXPECT_THROW(reader.ReadAt(nullptr, 1, 0), std::invalid_argument);
  EXPECT_THROW(reader.Read(nullptr, -1), std::invalid_argument);
  EXPECT_THROW(reader.ReadAt(nullptr, -1, 0), std::invalid_argument);

  EXPECT_EQ(reader.Read(nullptr, 0), 0);
  EXPECT_EQ(reader.Read(buf, 0), 0);
  EXPECT_EQ(reader.ReadAt(nullptr, 0, 0), 0);
  EXPECT_EQ(reader.ReadAt(buf, 0, 0), 0);
  EXPECT_EQ(reader.Seek(0, std::ios::cur), 0);

  // Reader stays usable after rejected and zero-length reads.
  EXPECT_EQ(reader.Read(buf, sizeof(buf)), 4);
  EXPECT_EQ(std::string_view(buf, 4), "hell");
}

TEST(OpenDALReaderTest, ReaderStreamSeekCurWithinBuffer) {
  opendal::Operator op("memory");
  op.Write("seek_cur_buf", "0123456789");

  opendal::ReaderStream stream(op.GetReader("seek_cur_buf"));

  char buf[2] = {};
  stream.read(buf, 2);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(std::string_view(buf, 2), "01");
  EXPECT_EQ(stream.tellg(), 2);

  // Seek forward within the unread buffered bytes.
  stream.seekg(3, std::ios::cur);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 5);
  EXPECT_EQ(stream.get(), '5');
  EXPECT_EQ(stream.tellg(), 6);

  // Seek backward within the buffered bytes.
  stream.seekg(-1, std::ios::cur);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 5);
  EXPECT_EQ(stream.get(), '5');
  EXPECT_EQ(stream.tellg(), 6);

  stream.seekg(-5, std::ios::cur);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 1);
  EXPECT_EQ(stream.get(), '1');
  EXPECT_EQ(stream.tellg(), 2);

  // seekpos delegates to absolute positioning within the buffer.
  stream.seekg(std::streampos(4));
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 4);
  EXPECT_EQ(stream.get(), '4');
  EXPECT_EQ(stream.tellg(), 5);

  // Seek to exact buffer end (EOF).
  stream.seekg(5, std::ios::cur);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 10);
  EXPECT_EQ(stream.peek(), std::char_traits<char>::eof());
  stream.clear();
  EXPECT_EQ(stream.tellg(), 10);
}

TEST(OpenDALReaderTest, ReaderStreamTellgAtEof) {
  opendal::Operator op("memory");
  op.Write("eof_test", "0123456789");

  {
    opendal::ReaderStream stream(op.GetReader("eof_test"));
    std::string content(std::istreambuf_iterator<char>{stream}, {});
    EXPECT_EQ(content, "0123456789");
    EXPECT_EQ(stream.tellg(), 10);

    // Repeated underflow/peek/get calls at EOF must not advance tellg().
    for (int i = 0; i < 3; ++i) {
      EXPECT_EQ(stream.peek(), std::char_traits<char>::eof());
      stream.clear();
      EXPECT_EQ(stream.tellg(), 10);
    }
    EXPECT_EQ(stream.get(), std::char_traits<char>::eof());
    stream.clear();
    EXPECT_EQ(stream.tellg(), 10);

    // Relative seek backward from EOF works.
    stream.seekg(-2, std::ios::cur);
    ASSERT_TRUE(!!stream);
    EXPECT_EQ(stream.tellg(), 8);
    char tail[2] = {};
    stream.read(tail, 2);
    ASSERT_TRUE(!!stream);
    EXPECT_EQ(std::string_view(tail, 2), "89");
    EXPECT_EQ(stream.tellg(), 10);
  }

  {
    // Reading past EOF leaves tellg() at content length after clear().
    opendal::ReaderStream stream(op.GetReader("eof_test"));
    char buf[20] = {};
    stream.read(buf, sizeof(buf));
    EXPECT_EQ(stream.gcount(), 10);
    EXPECT_TRUE(stream.eof());
    EXPECT_TRUE(stream.fail());
    stream.clear();
    EXPECT_EQ(stream.tellg(), 10);
  }

  {
    op.Write("empty_test", "");
    opendal::ReaderStream stream(op.GetReader("empty_test"));
    EXPECT_EQ(stream.tellg(), 0);
    EXPECT_EQ(stream.peek(), std::char_traits<char>::eof());
    stream.clear();
    EXPECT_EQ(stream.tellg(), 0);
  }
}

TEST(OpenDALReaderTest, ReaderStreamFromNonZeroReaderPosition) {
  opendal::Operator op("memory");
  op.Write("nonzero_reader", "0123456789");

  {
    auto reader = op.GetReader("nonzero_reader");
    char prefix[3] = {};
    ASSERT_EQ(reader.Read(prefix, 3), 3);
    EXPECT_EQ(std::string_view(prefix, 3), "012");

    opendal::ReaderStream stream(std::move(reader));
    EXPECT_EQ(stream.tellg(), 3);

    char next[2] = {};
    stream.read(next, 2);
    ASSERT_TRUE(!!stream);
    EXPECT_EQ(std::string_view(next, 2), "34");
    EXPECT_EQ(stream.tellg(), 5);

    stream.seekg(2, std::ios::cur);
    ASSERT_TRUE(!!stream);
    EXPECT_EQ(stream.tellg(), 7);
    EXPECT_EQ(stream.get(), '7');
    EXPECT_EQ(stream.tellg(), 8);

    // Seek earlier than the ReaderStream's starting offset.
    stream.seekg(-6, std::ios::cur);
    ASSERT_TRUE(!!stream);
    EXPECT_EQ(stream.tellg(), 2);
    stream.read(next, 2);
    ASSERT_TRUE(!!stream);
    EXPECT_EQ(std::string_view(next, 2), "23");
  }

  {
    auto reader = op.GetReader("nonzero_reader");
    ASSERT_EQ(reader.Seek(6, std::ios::beg), 6);

    opendal::ReaderStream stream(std::move(reader));
    EXPECT_EQ(stream.tellg(), 6);
    std::string rest(std::istreambuf_iterator<char>{stream}, {});
    EXPECT_EQ(rest, "6789");
    EXPECT_EQ(stream.tellg(), 10);
  }
}

TEST(OpenDALReaderTest, ReaderStreamSeekBegEndErrorsAndAcrossBuffers) {
  opendal::Operator op("memory");
  constexpr std::size_t kSize = 20000;
  std::string data(kSize, '\0');
  for (std::size_t i = 0; i < kSize; ++i) {
    data[i] = static_cast<char>((i * 31 + 7) & 0x7f);
  }
  op.Write("large_seek", data);

  opendal::ReaderStream stream(op.GetReader("large_seek"));

  // Fill initial 8192-byte buffer by reading 100 bytes.
  std::string chunk(100, '\0');
  stream.read(chunk.data(), chunk.size());
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(chunk, data.substr(0, 100));
  EXPECT_EQ(stream.tellg(), 100);

  // Seek forward across the 8192-byte buffer boundary via cur.
  stream.seekg(10000, std::ios::cur);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 10100);
  chunk.resize(16);
  stream.read(chunk.data(), chunk.size());
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(chunk, data.substr(10100, 16));
  EXPECT_EQ(stream.tellg(), 10116);

  // Seek backward across the buffer boundary via cur.
  stream.seekg(-5000, std::ios::cur);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 5116);
  stream.read(chunk.data(), chunk.size());
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(chunk, data.substr(5116, 16));
  EXPECT_EQ(stream.tellg(), 5132);

  // Seek via beg and end.
  stream.seekg(0, std::ios::beg);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 0);
  stream.read(chunk.data(), chunk.size());
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(chunk, data.substr(0, 16));

  stream.seekg(19984, std::ios::beg);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 19984);
  stream.read(chunk.data(), chunk.size());
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(chunk, data.substr(19984, 16));

  stream.seekg(-12, std::ios::end);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), 19988);
  std::string tail(12, '\0');
  stream.read(tail.data(), tail.size());
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(tail, data.substr(19988, 12));

  stream.seekg(0, std::ios::end);
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(stream.tellg(), static_cast<std::streampos>(kSize));
  EXPECT_EQ(stream.peek(), std::char_traits<char>::eof());
  stream.clear();
  EXPECT_EQ(stream.tellg(), static_cast<std::streampos>(kSize));

  // Invalid seeks must set failbit (not badbit) and preserve position/buffer.
  stream.seekg(500, std::ios::beg);
  ASSERT_TRUE(!!stream);
  std::string probe(4, '\0');
  stream.read(probe.data(), probe.size());
  ASSERT_TRUE(!!stream);
  EXPECT_EQ(probe, data.substr(500, 4));
  EXPECT_EQ(stream.tellg(), 504);

  stream.seekg(-1, std::ios::beg);
  EXPECT_TRUE(stream.fail());
  EXPECT_FALSE(stream.bad());
  stream.clear();
  EXPECT_EQ(stream.tellg(), 504);
  EXPECT_EQ(stream.get(), static_cast<unsigned char>(data[504]));

  stream.seekg(-600, std::ios::cur);
  EXPECT_TRUE(stream.fail());
  EXPECT_FALSE(stream.bad());
  stream.clear();
  EXPECT_EQ(stream.tellg(), 505);
  EXPECT_EQ(stream.get(), static_cast<unsigned char>(data[505]));

  stream.seekg(-static_cast<std::streamoff>(kSize) - 1, std::ios::end);
  EXPECT_TRUE(stream.fail());
  EXPECT_FALSE(stream.bad());
  stream.clear();
  EXPECT_EQ(stream.tellg(), 506);
  EXPECT_EQ(stream.get(), static_cast<unsigned char>(data[506]));

  stream.seekg(std::streampos(-1));
  EXPECT_TRUE(stream.fail());
  EXPECT_FALSE(stream.bad());
  stream.clear();
  EXPECT_EQ(stream.tellg(), 507);

  EXPECT_EQ(stream.rdbuf()->pubseekoff(0, std::ios::beg, std::ios::out),
            std::streampos(-1));
  EXPECT_EQ(stream.tellg(), 507);

  stream.seekg((std::numeric_limits<std::streamoff>::max)(), std::ios::cur);
  EXPECT_TRUE(stream.fail());
  EXPECT_FALSE(stream.bad());
  stream.clear();
  EXPECT_EQ(stream.tellg(), 507);

  stream.seekg((std::numeric_limits<std::streamoff>::max)(), std::ios::end);
  EXPECT_TRUE(stream.fail());
  EXPECT_FALSE(stream.bad());
  stream.clear();
  EXPECT_EQ(stream.tellg(), 507);
  EXPECT_EQ(stream.get(), static_cast<unsigned char>(data[507]));
}

TEST(OpenDALHandleTest, DefaultConstructedOperatorThrows) {
  opendal::Operator op;
  EXPECT_FALSE(op.Available());

  EXPECT_THROW(op.Read("a"), std::logic_error);
  EXPECT_THROW(op.Read("a", opendal::ReadOptions{}), std::logic_error);
  EXPECT_THROW(op.Write("a", "data"), std::logic_error);
  EXPECT_THROW(op.Write("a", "data", opendal::WriteOptions{}),
               std::logic_error);
  EXPECT_THROW(op.Exists("a"), std::logic_error);
  EXPECT_THROW(op.CreateDir("dir/"), std::logic_error);
  EXPECT_THROW(op.Copy("a", "b"), std::logic_error);
  EXPECT_THROW(op.Copy("a", "b", opendal::CopyOptions{}), std::logic_error);
  EXPECT_THROW(op.Rename("a", "b"), std::logic_error);
  EXPECT_THROW(op.Rename("a", "b", opendal::RenameOptions{}), std::logic_error);
  EXPECT_THROW(op.Remove("a"), std::logic_error);
  EXPECT_THROW(op.Remove("a", opendal::DeleteOptions{}), std::logic_error);
  EXPECT_THROW(op.RemoveAll({"a"}), std::logic_error);
  EXPECT_THROW(op.Stat("a"), std::logic_error);
  EXPECT_THROW(op.Stat("a", opendal::StatOptions{}), std::logic_error);
  EXPECT_THROW(op.List("dir/"), std::logic_error);
  EXPECT_THROW(op.List("dir/", opendal::ListOptions{}), std::logic_error);
  EXPECT_THROW(op.GetLister("dir/"), std::logic_error);
  EXPECT_THROW(op.GetLister("dir/", opendal::ListOptions{}), std::logic_error);
  EXPECT_THROW(op.GetReader("a"), std::logic_error);
  EXPECT_THROW(op.GetReader("a", opendal::ReaderOptions{}), std::logic_error);
  EXPECT_THROW(op.GetWriter("a"), std::logic_error);
  EXPECT_THROW(op.GetWriter("a", opendal::WriteOptions{}), std::logic_error);
  EXPECT_THROW(op.Info(), std::logic_error);
}

TEST(OpenDALHandleTest, MovedFromHandlesThrowAndMovedToHandlesWork) {
  opendal::Operator src("memory");
  ASSERT_TRUE(src.Available());

  // Move construction
  opendal::Operator moved(std::move(src));
  EXPECT_FALSE(src.Available());
  EXPECT_TRUE(moved.Available());
  EXPECT_THROW(src.Exists("a"), std::logic_error);
  EXPECT_THROW(src.Info(), std::logic_error);

  // Move assignment
  opendal::Operator op;
  op = std::move(moved);
  EXPECT_FALSE(moved.Available());
  EXPECT_TRUE(op.Available());
  EXPECT_THROW(moved.Stat("a"), std::logic_error);

  op.Write("dir/file", "hello world");
  EXPECT_EQ(op.Read("dir/file"), "hello world");

  // Moved-from Reader
  {
    auto r1 = op.GetReader("dir/file");
    auto r2 = std::move(r1);
    char buf[5] = {};

    EXPECT_THROW(r1.Read(buf, sizeof(buf)), std::logic_error);
    EXPECT_THROW(r1.Read(nullptr, 0), std::logic_error);
    EXPECT_THROW(r1.ReadAt(buf, sizeof(buf), 0), std::logic_error);
    EXPECT_THROW(r1.ReadAt(nullptr, 0, 0), std::logic_error);
    EXPECT_THROW(r1.Seek(0, std::ios::cur), std::logic_error);
    EXPECT_THROW((void)opendal::ReaderStream(std::move(r1)), std::logic_error);

    ASSERT_EQ(r2.Read(buf, sizeof(buf)), 5);
    EXPECT_EQ(std::string_view(buf, 5), "hello");
  }

  // Moved-from Writer
  {
    auto w1 = op.GetWriter("dir/written");
    auto w2 = std::move(w1);

    EXPECT_THROW(w1.Write("abc"), std::logic_error);
    EXPECT_THROW(w1.Flush(), std::logic_error);
    EXPECT_THROW(w1.Close(), std::logic_error);

    w2.Write("committed");
    w2.Flush();
    w2.Close();
    EXPECT_EQ(op.Read("dir/written"), "committed");
  }

  // Moved-from Lister
  {
    auto l1 = op.GetLister("dir/");
    auto l2 = std::move(l1);

    EXPECT_THROW(l1.Next(), std::logic_error);
    EXPECT_THROW(l1.begin(), std::logic_error);

    std::unordered_set<std::string> paths;
    for (const auto &entry : l2) {
      paths.insert(entry.path);
    }
    EXPECT_TRUE(paths.find("dir/file") != paths.end());
    EXPECT_TRUE(paths.find("dir/written") != paths.end());
  }
}

TEST(OpenDALReaderTest, SeekRejectsOverflowWithoutMutatingPosition) {
  opendal::Operator op("memory");
  op.Write("seek_overflow", "0123456789");

  auto reader = op.GetReader("seek_overflow");
  constexpr std::streamoff kMax = (std::numeric_limits<std::streamoff>::max)();

  ASSERT_EQ(reader.Seek(3, std::ios::beg), 3);

  // Seek past INT64_MAX from end must throw and keep position unchanged.
  EXPECT_THROW(reader.Seek(kMax, std::ios::end), std::exception);
  EXPECT_EQ(reader.Seek(0, std::ios::cur), 3);

  // Seek past INT64_MAX from current position must throw and keep position.
  EXPECT_THROW(reader.Seek(kMax, std::ios::cur), std::exception);
  EXPECT_EQ(reader.Seek(0, std::ios::cur), 3);

  // Seeking exactly to INT64_MAX is representable in std::streampos.
  EXPECT_EQ(reader.Seek(kMax, std::ios::beg), std::streampos(kMax));
  EXPECT_EQ(reader.Seek(0, std::ios::cur), std::streampos(kMax));

  // Advancing by 1 past INT64_MAX must throw and keep position at INT64_MAX.
  EXPECT_THROW(reader.Seek(1, std::ios::cur), std::exception);
  EXPECT_EQ(reader.Seek(0, std::ios::cur), std::streampos(kMax));

  // Reader stays usable after rejected seeks.
  ASSERT_EQ(reader.Seek(3, std::ios::beg), 3);
  char buf[4] = {};
  ASSERT_EQ(reader.Read(buf, sizeof(buf)), 4);
  EXPECT_EQ(std::string_view(buf, 4), "3456");
}

TEST(OpenDALListerTest, IteratorEqualityAndTraversal) {
  opendal::Operator op("memory");
  op.CreateDir("iter_dir/");
  op.Write("iter_dir/a", "1");
  op.Write("iter_dir/b", "2");

  auto lister = op.GetLister("iter_dir/");
  auto it = lister.begin();
  auto end = lister.end();

  EXPECT_TRUE(it == it);
  EXPECT_FALSE(it != it);
  EXPECT_TRUE(end == end);
  EXPECT_FALSE(end != end);
  EXPECT_TRUE(it != end);
  EXPECT_FALSE(it == end);
  EXPECT_TRUE(end != it);
  EXPECT_FALSE(end == it);

  // Active iterators from distinct listers compare unequal; end iterators equal.
  auto other_lister = op.GetLister("iter_dir/");
  auto other_it = other_lister.begin();
  EXPECT_FALSE(it == other_it);
  EXPECT_TRUE(it != other_it);
  EXPECT_TRUE(end == other_lister.end());
  EXPECT_FALSE(end != other_lister.end());

  std::unordered_set<std::string> paths;
  for (; it != end; ++it) {
    EXPECT_TRUE(it == it);
    EXPECT_FALSE(it != it);
    paths.insert((*it).path);
  }

  EXPECT_TRUE(it == end);
  EXPECT_FALSE(it != end);
  EXPECT_TRUE(it == it);
  EXPECT_FALSE(it != it);
  EXPECT_THAT(paths, ::testing::UnorderedElementsAre("iter_dir/", "iter_dir/a",
                                                     "iter_dir/b"));

  // Empty listing: begin() equals end() and is reflexive.
  auto empty_lister = op.GetLister("nonexistent_prefix/");
  auto empty_it = empty_lister.begin();
  auto empty_end = empty_lister.end();
  EXPECT_TRUE(empty_it == empty_it);
  EXPECT_FALSE(empty_it != empty_it);
  EXPECT_TRUE(empty_it == empty_end);
  EXPECT_FALSE(empty_it != empty_end);
}

} // namespace opendal::test
