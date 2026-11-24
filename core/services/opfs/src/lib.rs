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

#![cfg(any(target_arch = "wasm32", test))]
#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

/// URI scheme used for service registration and scheme-driven construction.
#[cfg(target_arch = "wasm32")]
pub const OPFS_SCHEME: &str = "opfs";

/// Register this service's URI scheme or schemes with an operator registry.
///
/// Registration enables scheme-driven construction through
/// [`opendal_core::Operator::from_uri`] and
/// [`opendal_core::Operator::via_iter`]. Direct construction through
/// [`opendal_core::Operator::new`] does not require registration.
#[cfg(target_arch = "wasm32")]
pub fn register_opfs_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<Opfs>(OPFS_SCHEME);
}

#[cfg(target_arch = "wasm32")]
macro_rules! console_debug {
    ($($arg:tt)*) => {
        #[cfg(debug_assertions)]
        web_sys::console::log_1(&format!($($arg)*).into())
      };
}

#[cfg(target_arch = "wasm32")]
mod backend;
mod config;
#[cfg(target_arch = "wasm32")]
mod core;
#[cfg(target_arch = "wasm32")]
mod deleter;
mod lister;
mod reader;
mod writer;

#[cfg(target_arch = "wasm32")]
pub use backend::OpfsBuilder as Opfs;
#[cfg(target_arch = "wasm32")]
pub use config::OpfsConfig;
