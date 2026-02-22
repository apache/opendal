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

#![cfg(target_arch = "wasm32")]

/// Default scheme for opfs service.
pub const OPFS_SCHEME: &str = "opfs";

/// Register this service into the given registry.
pub fn register_opfs_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<Opfs>(OPFS_SCHEME);
}

macro_rules! console_debug {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        web_sys::console::log_1(&format!($($arg)*).into())
      };
}

mod backend;
mod config;
mod core;
mod error;
mod utils;
mod writer;

pub use backend::OpfsBuilder as Opfs;
pub use config::OpfsConfig;
