// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The entry point to combine all integration tests into a single binary.
//!
//! See [this post](https://matklad.github.io/2021/02/27/delete-cargo-integration-tests.html)
//! for the rationale behind this approach.

mod cat;
mod cp;
mod edit;
mod ls;
mod mv;
mod rm;
mod stat;
mod tee;

pub mod test_utils;
