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

#include <cmath>
#include <stdexcept>

#include "layer.hpp"
#include "lib.rs.h"

namespace opendal {

FfiLayerBuilderMutator::FfiLayerBuilderMutator(ffi::LayerBuilder &builder)
    : builder_(builder) {}

void FfiLayerBuilderMutator::AddTimeout(uint64_t timeout_ns,
                                        uint64_t io_timeout_ns) {
  builder_.add_timeout(timeout_ns, io_timeout_ns);
}

void FfiLayerBuilderMutator::AddRetry(bool jitter, float factor,
                                      uint64_t min_delay_ns,
                                      uint64_t max_delay_ns,
                                      uint64_t max_times) {
  builder_.add_retry(jitter, factor, min_delay_ns, max_delay_ns, max_times);
}

namespace {

class RetryMaxTimesOption final : public RetryOption {
 public:
  explicit RetryMaxTimesOption(uint64_t max_times) : max_times_(max_times) {}

  void Apply(RetryConfig &config) const override {
    if (max_times_ == 0) {
      throw std::invalid_argument("retry max times must be positive");
    }
    config.max_times = max_times_;
  }

 private:
  uint64_t max_times_;
};

class RetryFactorOption final : public RetryOption {
 public:
  explicit RetryFactorOption(float factor) : factor_(factor) {}

  void Apply(RetryConfig &config) const override {
    if (std::isnan(factor_) || std::isinf(factor_) || factor_ < 1.0f) {
      throw std::invalid_argument(
          "retry factor must be finite and greater than or equal to 1");
    }
    config.factor = factor_;
  }

 private:
  float factor_;
};

class RetryJitterOption final : public RetryOption {
 public:
  void Apply(RetryConfig &config) const override { config.jitter = true; }
};

class RetryMinDelayOption final : public RetryOption {
 public:
  explicit RetryMinDelayOption(std::chrono::nanoseconds delay)
      : delay_(delay) {}

  void Apply(RetryConfig &config) const override {
    if (delay_.count() <= 0) {
      throw std::invalid_argument("retry min delay must be positive");
    }
    config.min_delay = delay_;
  }

 private:
  std::chrono::nanoseconds delay_;
};

class RetryMaxDelayOption final : public RetryOption {
 public:
  explicit RetryMaxDelayOption(std::chrono::nanoseconds delay)
      : delay_(delay) {}

  void Apply(RetryConfig &config) const override {
    if (delay_.count() <= 0) {
      throw std::invalid_argument("retry max delay must be positive");
    }
    config.max_delay = delay_;
  }

 private:
  std::chrono::nanoseconds delay_;
};

class TimeoutOperatorOption final : public OperatorOption {
 public:
  TimeoutOperatorOption(std::chrono::nanoseconds timeout,
                        std::chrono::nanoseconds io_timeout)
      : timeout_(timeout), io_timeout_(io_timeout) {}

  void Apply(LayerBuilderMutator &builder) const override {
    if (timeout_.count() <= 0) {
      throw std::invalid_argument("timeout must be positive");
    }
    if (io_timeout_.count() <= 0) {
      throw std::invalid_argument("io timeout must be positive");
    }
    builder.AddTimeout(static_cast<uint64_t>(timeout_.count()),
                       static_cast<uint64_t>(io_timeout_.count()));
  }

 private:
  std::chrono::nanoseconds timeout_;
  std::chrono::nanoseconds io_timeout_;
};

class RetryOperatorOption final : public OperatorOption {
 public:
  explicit RetryOperatorOption(
      std::vector<std::unique_ptr<RetryOption>> options)
      : options_(std::move(options)) {}

  void Apply(LayerBuilderMutator &builder) const override {
    RetryConfig config;
    for (const auto &option : options_) {
      if (option != nullptr) {
        option->Apply(config);
      }
    }
    if (config.max_delay < config.min_delay) {
      throw std::invalid_argument(
          "retry max delay must be greater than or equal to retry min delay");
    }
    builder.AddRetry(
        config.jitter, config.factor,
        static_cast<uint64_t>(config.min_delay.count()),
        static_cast<uint64_t>(config.max_delay.count()), config.max_times);
  }

 private:
  std::vector<std::unique_ptr<RetryOption>> options_;
};

}  // namespace

std::unique_ptr<RetryOption> RetryMaxTimes(uint64_t max_times) {
  return std::make_unique<RetryMaxTimesOption>(max_times);
}

std::unique_ptr<RetryOption> RetryFactor(float factor) {
  return std::make_unique<RetryFactorOption>(factor);
}

std::unique_ptr<RetryOption> RetryJitter() {
  return std::make_unique<RetryJitterOption>();
}

std::unique_ptr<RetryOption> RetryMinDelay(std::chrono::nanoseconds delay) {
  return std::make_unique<RetryMinDelayOption>(delay);
}

std::unique_ptr<RetryOption> RetryMaxDelay(std::chrono::nanoseconds delay) {
  return std::make_unique<RetryMaxDelayOption>(delay);
}

std::unique_ptr<OperatorOption> WithTimeout(
    std::chrono::nanoseconds timeout, std::chrono::nanoseconds io_timeout) {
  return std::make_unique<TimeoutOperatorOption>(timeout, io_timeout);
}

std::unique_ptr<OperatorOption> WithRetry(
    std::vector<std::unique_ptr<RetryOption>> options) {
  return std::make_unique<RetryOperatorOption>(std::move(options));
}

std::vector<std::unique_ptr<OperatorOption>> DefaultBehaviorLayerOptions() {
  std::vector<std::unique_ptr<OperatorOption>> options;
  options.push_back(WithTimeout(std::chrono::minutes(1),
                                std::chrono::seconds(10)));
  options.push_back(WithRetry());
  return options;
}

}  // namespace opendal
