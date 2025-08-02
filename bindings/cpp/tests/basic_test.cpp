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

#include "gtest/gtest.h"
#include "opendal.hpp"

class OpendalTest : public ::testing::Test {
 protected:
  opendal::Operator op;

  std::string scheme;
  std::unordered_map<std::string, std::string> config;

  // random number generator
  std::mt19937 rng;

  void SetUp() override {
    scheme = "memory";
    rng.seed(time(nullptr));

    op = opendal::Operator(scheme, config);
    EXPECT_TRUE(op.Available());
  }
};

// Scenario: OpenDAL Blocking Operations
TEST_F(OpendalTest, BasicTest) {
  std::string file_path = "test";
  std::string file_path_copied = "test_copied";
  std::string file_path_renamed = "test_renamed";
  std::string dir_path = "test_dir/";
  std::string_view data = "abc";

  // write
  op.Write(file_path, data);

  // read
  auto res = op.Read(file_path);
  EXPECT_EQ(res, data);

  // check existence
  EXPECT_TRUE(op.Exists(file_path));

  // create directory
  op.CreateDir(dir_path);
  EXPECT_TRUE(op.Exists(dir_path));

  // get metadata
  auto metadata = op.Stat(file_path);
  EXPECT_EQ(metadata.type, opendal::EntryMode::FILE);
  EXPECT_EQ(metadata.content_length, data.size());

  // list entries
  auto list_file_path = dir_path + file_path;
  op.Write(list_file_path, data);
  auto entries = op.List(dir_path);
  EXPECT_EQ(entries.size(), 2);
  std::set<std::string> paths;
  for (const auto &entry : entries) {
    paths.insert(entry.path);
  }
  EXPECT_TRUE(paths.find(dir_path) != paths.end());
  EXPECT_TRUE(paths.find(list_file_path) != paths.end());

  // remove files
  op.Remove(file_path_renamed);
  op.Remove(dir_path);
  EXPECT_FALSE(op.Exists(file_path_renamed));
}

TEST_F(OpendalTest, ReaderTest) {
  std::string file_path = "test";
  constexpr int size = 2000;
  std::string data(size, 0);

  for (auto &d : data) {
    d = rng() % 256;
  }

  // write
  op.Write(file_path, data);

  // get reader
  auto reader = op.GetReader(file_path);
  // uint8_t part_data[100];
  std::string part_data(100, 0);
  reader.Seek(200, std::ios::cur);
  reader.Read(part_data.data(), 100);
  EXPECT_EQ(reader.Seek(0, std::ios::cur), 300);
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(part_data[i], data[200 + i]);
  }
  reader.Seek(0, std::ios::beg);

  // reader stream
  opendal::ReaderStream stream(op.GetReader(file_path));

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

TEST_F(OpendalTest, ListerTest) {
  std::string dir_path = "test_dir/";
  op.CreateDir(dir_path);
  auto test1_path = dir_path + "test1";
  op.Write(test1_path, "123");
  auto test2_path = dir_path + "test2";
  op.Write(test2_path, "456");

  auto lister = op.GetLister("test_dir/");

  std::set<std::string> paths;
  for (const auto &entry : lister) {
    paths.insert(entry.path);
  }
  EXPECT_EQ(paths.size(), 3);
  EXPECT_TRUE(paths.find(dir_path) != paths.end());
  EXPECT_TRUE(paths.find(test1_path) != paths.end());
  EXPECT_TRUE(paths.find(test2_path) != paths.end());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
