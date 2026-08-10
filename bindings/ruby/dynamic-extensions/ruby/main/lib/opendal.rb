# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# frozen_string_literal: true

require "json"
require_relative "opendal_ruby_poc"

module OpenDal
  REQUIRED_RUNTIME_PROTOCOL = Integer(
    ENV.fetch("OPENDAL_POC_REQUIRED_RUNTIME_PROTOCOL", "1"),
    10
  )

  Runtime.load(
    File.expand_path("opendal/_native/libopendal_runtime_poc.so", __dir__),
    REQUIRED_RUNTIME_PROTOCOL
  )

  class Operator
    def info
      JSON.parse(info_json)
    end
  end
end
