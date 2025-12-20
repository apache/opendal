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

//! GridFS service implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

mod backend;
mod config;
mod core;
mod deleter;
mod writer;

pub use backend::GridfsBuilder as Gridfs;
pub use config::GridfsConfig;

/// Default scheme for gridfs service.
pub const GRIDFS_SCHEME: &str = "gridfs";

#[ctor::ctor]
fn register_gridfs_service() {
    opendal_core::DEFAULT_OPERATOR_REGISTRY.register::<Gridfs>(GRIDFS_SCHEME);
}
