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

#include "opendal.hpp"

#include <string>
#include <vector>
#include <iostream>

int main() {
  char s[] = "memory";
  std::vector<uint8_t> data = {'a', 'b', 'c'};

  // Init operator
  opendal::Operator op = opendal::Operator(s);

  // Write data to operator
  op.write("test", data);

  // Read data from operator
  auto res = op.read("test"); // res == data

  // Using reader
  auto reader = op.reader("test");
  opendal::ReaderStream stream(reader);
  std::string res2;
  stream >> res2; // res2 == "abc"
}
