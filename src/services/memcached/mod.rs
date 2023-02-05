// Copyright 2023 Datafuse Labs.
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

//! Memcached support for OpenDAL
//!
//! # Configuration
//!
//! - `root`: Set the working directory of `OpenDAL`
//! - `endpoint`: Set the network address of memcached server
//! - `default_ttl`: Set the ttl for memcached service.
//!
//! You can refer to [`MemcachedBuilder`]'s docs for more information
//!
//! # Example
//!
//! ## Via Builder
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::services::memcached;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut builder = memcached::Builder::default();
//!
//!     builder.endpoint("tcp://127.0.0.1:11211");
//!
//!     let op: Operator = Operator::create(builder)?.finish();
//!     let _: Object = op.object("test_file");
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::MemcachedBuilder;
