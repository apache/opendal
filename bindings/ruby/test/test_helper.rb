# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# frozen_string_literal: true

require "active_support"
require "minitest/autorun"
require "minitest/reporters"

Minitest::Reporters.use!([Minitest::Reporters::DefaultReporter.new(color: true)])

require "opendal"

# Uses `ActiveSupport::TestCase` for additional features including:
# - additional assertions
# - file fixtures
# - parallel worker
#
# Read more https://edgeapi.rubyonrails.org/classes/ActiveSupport/TestCase.html
class ActiveSupport::TestCase
  parallelize(workers: :number_of_processors)
end
