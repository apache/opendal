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

//! IPFS file system support based on [IPFS MFS](https://docs.ipfs.tech/concepts/file-systems/) API.
//!
//! # Configuration
//!
//! - `root`: Set the work directory for backend
//! - `endpoint`: Customizable endpoint setting
//!
//! You can refer to [`Builder`]'s docs for more information
//!
//! # Example
//!
//! ## Via Builder
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::services::Ipmfs;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // create backend builder
//!     let mut builder = Ipmfs::default();
//!
//!     // set the storage bucket for OpenDAL
//!     builder.endpoint("http://127.0.0.1:5001");
//!
//!     let op: Operator = Operator::create(builder)?.finish();
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```

mod backend;
mod builder;
pub use builder::Builder;

mod dir_stream;
mod error;
