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

/// Default scheme for d1 service.
pub const D1_SCHEME: &str = "d1";

use crate::types::DEFAULT_OPERATOR_REGISTRY;

mod backend;
mod config;
mod core;
mod deleter;
mod error;
mod model;
mod writer;

pub use backend::D1Builder as D1;
pub use config::D1Config;

#[small_ctor::ctor]
unsafe fn register_d1_service() {
    DEFAULT_OPERATOR_REGISTRY.register::<D1>(D1_SCHEME);
}
