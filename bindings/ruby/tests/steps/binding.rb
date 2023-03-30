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

require_relative "../../lib/opendal"

Given("A new OpenDAL Blocking Operator") do
  @op = OpenDAL::Operator.new("memory", nil)
end

When("Blocking write path {string} with content {string}") do |path, content|
  @op.write(path, content)
end

Then("The blocking file {string} should exist") do |path|
  @op.stat(path)
end

Then("The blocking file {string} entry mode must be file") do |path|
  @op.stat(path).is_file == true
end

Then("The blocking file {string} content length must be {int}") do |path, length|
  @op.stat(path).content_length == length
end

Then("The blocking file {string} must have content {string}") do |path, content|
  @op.read(path) == content
end

Given("A new OpenDAL Async Operator") do
  pending # Write code here that turns the phrase above into concrete actions
end

When("Async write path {string} with content {string}") do |path, content|
  pending # Write code here that turns the phrase above into concrete actions
end

Then("The async file {string} should exist") do |path|
  pending # Write code here that turns the phrase above into concrete actions
end

Then("The async file {string} entry mode must be file") do |path|
  pending # Write code here that turns the phrase above into concrete actions
end

Then("The async file {string} content length must be {int}") do |path, length|
  pending # Write code here that turns the phrase above into concrete actions
end

Then("The async file {string} must have content {string}") do |path, content|
  pending # Write code here that turns the phrase above into concrete actions
end
