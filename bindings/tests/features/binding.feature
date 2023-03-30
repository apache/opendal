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

Feature: OpenDAL Binding

    Scenario: OpenDAL Blocking Operations
        Given A new OpenDAL Blocking Operator
        When Blocking write path "test" with content "Hello, World!"
        Then The blocking file "test" should exist
        Then The blocking file "test" entry mode must be file
        Then The blocking file "test" content length must be 13
        Then The blocking file "test" must have content "Hello, World!"

    Scenario: OpenDAL Async Operations
        Given A new OpenDAL Async Operator
        When Async write path "test" with content "Hello, World!"
        Then The async file "test" should exist
        Then The async file "test" entry mode must be file
        Then The async file "test" content length must be 13
        Then The async file "test" must have content "Hello, World!"
