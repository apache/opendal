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

#![doc = include_str!("../README.md")]
#![cfg(target_os = "linux")]
#![deny(missing_docs)]

mod backend;
mod config;
mod driver;
mod ffi;
mod reader;
mod writer;

pub use backend::Hf3fsBuilder as Hf3fs;
pub use config::Hf3fsConfig;

/// URI scheme for the 3FS USRBIO service.
pub const HF3FS_SCHEME: &str = "hf3fs";

/// Register 3FS for [`opendal_core::Operator::from_uri`] and
/// [`opendal_core::Operator::via_iter`].
pub fn register_hf3fs_service(registry: &opendal_core::OperatorRegistry) {
    registry.register::<Hf3fs>(HF3FS_SCHEME);
}
