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

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/opendal/main/website/static/img/logo.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
//! Facade crate that re-exports all public APIs from `opendal-core` and optional services/layers.
#![deny(missing_docs)]

pub use opendal_core::*;

/// Re-export of service implementations.
pub mod services {
    pub use opendal_core::services::*;
    #[cfg(feature = "services-aliyun-drive")]
    pub use opendal_service_aliyun_drive::*;
    #[cfg(feature = "services-azblob")]
    pub use opendal_service_azblob::*;
    #[cfg(feature = "services-azdls")]
    pub use opendal_service_azdls::*;
    #[cfg(feature = "services-azfile")]
    pub use opendal_service_azfile::*;
    #[cfg(feature = "services-ghac")]
    pub use opendal_service_ghac::*;
    #[cfg(feature = "services-moka")]
    pub use opendal_service_moka::*;
    #[cfg(feature = "services-s3")]
    pub use opendal_service_s3::*;
}

/// Re-export of layers.
pub mod layers {
    pub use opendal_core::layers::*;
    #[cfg(feature = "layers-async-backtrace")]
    pub use opendal_layer_async_backtrace::*;
    #[cfg(feature = "layers-await-tree")]
    pub use opendal_layer_await_tree::*;
    #[cfg(feature = "layers-capability-check")]
    pub use opendal_layer_capability_check::*;
}
