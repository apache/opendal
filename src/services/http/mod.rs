// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! HTTP Read-only backend support.
//!
//! Although HTTP backend supports `write`, `delete` and `create`, but they should
//! only be used for testing purpose. As there are no standards for HTTP services
//! to handle them.
//!
//! HTTP Method mapping:
//!
//! - `create`: HTTP PUT (test only)
//! - `read`: HTTP GET
//! - `write`: HTTP PUT  (test only)
//! - `stat`: HTTP HEAD
//! - `delete`: HTTP DELETE  (test only)
//! - `list`: List internal index.

mod backend;
mod error;

pub use backend::Backend;
pub use backend::Builder;
