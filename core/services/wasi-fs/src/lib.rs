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

//! WASI Filesystem service for OpenDAL
//!
//! This service provides filesystem access for WebAssembly components
//! running in WASI Preview 2 compatible runtimes (wasmtime, wasmer).

#![cfg(all(target_arch = "wasm32", target_os = "wasi"))]

/// Default scheme for wasi-fs service.
pub const WASI_FS_SCHEME: &str = "wasi-fs";

use opendal_core::DEFAULT_OPERATOR_REGISTRY;

mod backend;
mod config;
mod core;
mod deleter;
mod error;
mod lister;
mod reader;
mod writer;

pub use backend::WasiFsBuilder as WasiFs;
pub use config::WasiFsConfig;

#[ctor::ctor]
fn register_wasi_fs_service() {
    DEFAULT_OPERATOR_REGISTRY.register::<WasiFs>(WASI_FS_SCHEME);
}
