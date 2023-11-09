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

#include <vector>
#include <random>

std::vector<unsigned char> generateRandomBytes(std::size_t size) {
  // Create a random device to generate random numbers
  std::random_device rd;

  // Create a random engine and seed it with the random device
  std::mt19937_64 gen(rd());

  // Create a distribution to produce random bytes
  std::uniform_int_distribution<unsigned char> dist(0, 255);

  // Create a vector to hold the random bytes
  std::vector<unsigned char> randomBytes(size);

  // Generate random bytes and store them in the vector
  for (std::size_t i = 0; i < size; ++i) {
    randomBytes[i] = dist(gen);
  }

  return randomBytes;
}
