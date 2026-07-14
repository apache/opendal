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

#include <chrono>
#include <memory>
#include <utility>
#include <vector>

#include "framework/test_framework.hpp"
#include "layer.hpp"
#include "mocks/mock_layer_builder.hpp"

namespace opendal::test {

class LayerTest : public ::testing::Test {};

OPENDAL_TEST_F(LayerTest, WithTimeoutForwardsNanosecondsToMutator) {
  MockLayerBuilderMutator mutator;
  const auto timeout = std::chrono::seconds(30);
  const auto io_timeout = std::chrono::seconds(5);

  EXPECT_CALL(
      mutator,
      AddTimeout(static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                           timeout)
                                           .count()),
                 static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                             io_timeout)
                                             .count())))
      .Times(1);

  WithTimeout(timeout, io_timeout)->ApplyTo(mutator);
}

OPENDAL_TEST_F(LayerTest, WithRetryForwardsConfigToMutator) {
  MockLayerBuilderMutator mutator;

  EXPECT_CALL(mutator, AddRetry(false, 2.0F, 1'000'000ULL, 10'000'000ULL, 5))
      .Times(1);

  RetryConfig config;
  config.max_times = 5;
  config.min_delay = std::chrono::milliseconds(1);
  config.max_delay = std::chrono::milliseconds(10);

  WithRetry(config)->ApplyTo(mutator);
}

OPENDAL_TEST_F(LayerTest, WithRetryEnablesJitter) {
  MockLayerBuilderMutator mutator;

  EXPECT_CALL(mutator, AddRetry(true, testing::_, testing::_, testing::_, testing::_))
      .Times(1);

  RetryConfig config;
  config.jitter = true;
  WithRetry(config)->ApplyTo(mutator);
}

OPENDAL_TEST_F(LayerTest, WithRetryUsesDefaultConfig) {
  MockLayerBuilderMutator mutator;

  EXPECT_CALL(mutator,
              AddRetry(false, 2.0F, 1'000'000'000ULL, 60'000'000'000ULL, 3))
      .Times(1);

  WithRetry()->ApplyTo(mutator);
}

OPENDAL_TEST_F(LayerTest, LayerOptionsApplyInOrder) {
  MockLayerBuilderMutator mutator;
  testing::InSequence sequence;

  EXPECT_CALL(mutator, AddTimeout(testing::_, testing::_)).Times(1);
  EXPECT_CALL(mutator, AddRetry(testing::_, testing::_, testing::_, testing::_, testing::_))
      .Times(1);

  WithTimeout(std::chrono::minutes(1), std::chrono::seconds(10))->ApplyTo(mutator);
  WithRetry()->ApplyTo(mutator);
}

OPENDAL_TEST_F(LayerTest, OperatorWithTimeoutAndRetry) {
  std::vector<std::unique_ptr<OperatorOption>> options;
  options.push_back(WithTimeout(std::chrono::minutes(1),
                                std::chrono::seconds(10)));
  options.push_back(WithRetry());

  Operator op("memory", {}, std::move(options));
  ASSERT_TRUE(op.Available());

  constexpr std::string_view data = "layer-test";
  op.Write("layer-test.txt", data);
  EXPECT_EQ(op.Read("layer-test.txt"), data);
}

OPENDAL_TEST_F(LayerTest, WithTimeoutRejectsZeroTimeout) {
  std::vector<std::unique_ptr<OperatorOption>> options;
  options.push_back(WithTimeout(std::chrono::seconds(0), std::chrono::seconds(1)));
  EXPECT_THROW(Operator("memory", {}, std::move(options)), std::invalid_argument);
}

OPENDAL_TEST_F(LayerTest, WithTimeoutRejectsZeroIoTimeout) {
  std::vector<std::unique_ptr<OperatorOption>> options;
  options.push_back(WithTimeout(std::chrono::seconds(1), std::chrono::seconds(0)));
  EXPECT_THROW(Operator("memory", {}, std::move(options)), std::invalid_argument);
}

OPENDAL_TEST_F(LayerTest, RetryMaxTimesRejectsZero) {
  RetryConfig config;
  config.max_times = 0;

  std::vector<std::unique_ptr<OperatorOption>> options;
  options.push_back(WithRetry(config));

  EXPECT_THROW(Operator("memory", {}, std::move(options)), std::invalid_argument);
}

OPENDAL_TEST_F(LayerTest, RetryMaxDelayBeforeMinDelay) {
  RetryConfig config;
  config.min_delay = std::chrono::seconds(10);
  config.max_delay = std::chrono::seconds(1);

  std::vector<std::unique_ptr<OperatorOption>> options;
  options.push_back(WithRetry(config));

  EXPECT_THROW(Operator("memory", {}, std::move(options)), std::invalid_argument);
}

OPENDAL_TEST_F(LayerTest, RetryFactorRejectsInvalidValue) {
  RetryConfig config;
  config.factor = 0.5F;

  std::vector<std::unique_ptr<OperatorOption>> options;
  options.push_back(WithRetry(config));

  EXPECT_THROW(Operator("memory", {}, std::move(options)), std::invalid_argument);
}

}  // namespace opendal::test
