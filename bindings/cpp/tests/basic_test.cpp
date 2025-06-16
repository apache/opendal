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
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "framework/test_framework.hpp"

namespace opendal::test {

// Scenario: OpenDAL Blocking Operations
OPENDAL_TEST_F(OpenDALTest, BasicTest) {
  std::string file_path = "test";
  std::string file_path_copied = "test_copied";
  std::string file_path_renamed = "test_renamed";
  std::string dir_path = "test_dir/";
  std::string_view data = "abc";

  // write
  op_.write(file_path, data);

  // read
  auto res = op_.read(file_path);
  EXPECT_EQ(res, data);

  // is_exist
  EXPECT_TRUE(op_.exists(file_path));

  // create_dir
  op_.create_dir(dir_path);
  EXPECT_TRUE(op_.exists(dir_path));

  // stat
  auto metadata = op_.stat(file_path);
  EXPECT_EQ(metadata.type, opendal::EntryMode::FILE);
  EXPECT_EQ(metadata.content_length, data.size());

  // list
  auto list_file_path = dir_path + file_path;
  op_.write(list_file_path, data);
  auto entries = op_.list(dir_path);
  EXPECT_EQ(entries.size(), 2);
  std::set<std::string> paths;
  for (const auto &entry : entries) {
    paths.insert(entry.path);
  }
  EXPECT_TRUE(paths.find(dir_path) != paths.end());
  EXPECT_TRUE(paths.find(list_file_path) != paths.end());

  // remove
  op_.remove(file_path_renamed);
  op_.remove(dir_path);
  EXPECT_FALSE(op_.exists(file_path_renamed));
}

OPENDAL_TEST_F(OpenDALTest, ReaderTest) {
  std::string file_path = "test";
  constexpr int size = 2000;
  std::string data(size, 0);

  for (auto &d : data) {
    d = rng_() % 256;
  }

  // write
  op_.write(file_path, data);

  // reader
  auto reader = op_.reader(file_path);
  // uint8_t part_data[100];
  std::string part_data(100, 0);
  reader.seek(200, std::ios::cur);
  reader.read(part_data.data(), 100);
  EXPECT_EQ(reader.seek(0, std::ios::cur), 300);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(part_data[i], data[200 + i]);
  }
  reader.seek(0, std::ios::beg);

  // stream
  opendal::ReaderStream stream(op_.reader(file_path));

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

OPENDAL_TEST_F(OpenDALTest, ListerTest) {
  std::string dir_path = "test_dir/";
  op_.create_dir(dir_path);
  auto test1_path = dir_path + "test1";
  op_.write(test1_path, "123");
  auto test2_path = dir_path + "test2";
  op_.write(test2_path, "456");

  auto lister = op_.lister("test_dir/");

  std::set<std::string> paths;
  for (const auto &entry : lister) {
      paths.insert(entry.path);
  }
  EXPECT_EQ(paths.size(), 3);
  EXPECT_TRUE(paths.find(dir_path) != paths.end());
  EXPECT_TRUE(paths.find(test1_path) != paths.end());
  EXPECT_TRUE(paths.find(test2_path) != paths.end());
}

} // namespace opendal::test
