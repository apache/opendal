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
pub use accessor::*;

mod layer;
pub use layer::*;

mod path;
pub use path::*;

mod operation;
pub use operation::*;

mod version;
pub use version::VERSION;

mod rps;
pub use rps::*;

mod ops;
pub use ops::*;

mod http_util;
pub use http_util::*;

mod serde_util;
pub use serde_util::*;

mod chrono_util;
pub use chrono_util::*;

mod tokio_util;
pub use tokio_util::*;

// Expose as a pub mod to avoid confusing.
pub mod adapters;
pub mod oio;
