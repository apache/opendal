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

//! Apache OpenDAL facade crate that re-exports core, layers, and services.
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/opendal/main/website/static/img/logo.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

// Re-export everything from opendal-core so existing paths keep working.
pub use opendal_core::*;
pub use opendal_core::{raw, types};

// Public modules retained in the facade crate.
#[cfg(feature = "blocking")]
pub use opendal_core::blocking;
#[cfg(docsrs)]
pub use opendal_core::docs;

/// Layers (includes critical layers re-exported and facade-only layers).
pub mod layers;
/// Storage services live in the facade crate.
pub mod services;
