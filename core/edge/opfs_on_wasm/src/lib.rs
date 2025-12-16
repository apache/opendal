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

mod async_create_dir;
mod async_delete;
mod utils;

use opendal::{Configurator, Operator, services::OpfsConfig};
pub use utils::*;

pub static TEST_FIXTURE: Fixture = Fixture::new();

pub(crate) fn operator() -> Operator {
    let builder = OpfsConfig::default().into_builder().root("/a/b/");
    Operator::new(builder).unwrap().finish()
}
