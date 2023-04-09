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

//! Raw modules provide raw APIs that used by underlying services
//!
//! ## Notes
//!
//! - Only developers who want to develop new services or layers need to
//!   access raw APIs.
//! - Raw APIs should only be accessed via `opendal::raw::Xxxx`, any public
//!   API should never expose raw API directly.
//! - Raw APIs are far more less stable than public API, please don't rely on
//!   them whenever possible.

mod accessor;
pub use accessor::Accessor;
pub use accessor::AccessorCapability;
pub use accessor::AccessorHint;
pub use accessor::AccessorInfo;
pub use accessor::FusedAccessor;

mod layer;
pub use layer::Layer;
pub use layer::LayeredAccessor;

pub mod oio;

mod path;
pub use path::build_abs_path;
pub use path::build_rel_path;
pub use path::build_rooted_abs_path;
pub use path::get_basename;
pub use path::get_parent;
pub use path::normalize_path;
pub use path::normalize_root;
pub use path::validate_path;

mod operation;
pub use operation::Operation;

mod version;
pub use version::VERSION;

mod rps;
pub use rps::*;

mod http_util;
pub use http_util::*;

mod serde_util;
pub use serde_util::*;

// Expose as a pub mod to avoid confusing.
pub mod adapters;
