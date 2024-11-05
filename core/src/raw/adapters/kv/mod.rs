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

//! Providing Key Value Adapter for OpenDAL.
//!
//! Any services that implement `Adapter` can be used an OpenDAL Service.

mod api;
pub use api::Adapter;
pub use api::Info;
pub use api::Scan;
#[cfg(any(
    feature = "services-cloudflare-kv",
    feature = "services-etcd",
    feature = "services-nebula-graph",
    feature = "services-rocksdb",
    feature = "services-sled"
))]
pub(crate) use api::ScanStdIter;
pub use api::Scanner;

mod backend;
pub use backend::Backend;
