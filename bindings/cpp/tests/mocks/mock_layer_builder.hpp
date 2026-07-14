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

#pragma once

#include "gmock/gmock.h"
#include "layer.hpp"

namespace opendal::test {

class MockLayerBuilderMutator : public LayerBuilderMutator {
 public:
  MOCK_METHOD(void, AddTimeout, (uint64_t timeout_ns, uint64_t io_timeout_ns),
              (override));
  MOCK_METHOD(void, AddRetry,
              (bool jitter, float factor, uint64_t min_delay_ns,
               uint64_t max_delay_ns, uint64_t max_times),
              (override));
};

}  // namespace opendal::test
