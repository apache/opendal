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

//! IPFS file system support.
//!
//! # Configuration
//!
//! - `root`: Set the work directory for backend
//! - `endpoint`: Customizable endpoint setting
//!
//! You can refer to [`Builder`]'s docs for more information
//!
//! # Environment
//!
//! - `OPENDAL_IPFS_ROOT`    optional
//! - `OPENDAL_IPFS_ENDPOINT`  optional
//!
//! # Example
//!
//! ## Initiate via environment variables
//!
//! Set environment correctly:
//!
//! ```shell
//! export OPENDAL_IPFS_ROOT=/path/to/root
//! export OPENDAL_IPFS_ENDPOINT=http://127.0.0.1:5001
//! ```
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op: Operator = Operator::from_env(Scheme::Ipmfs)?;
//!
//!     // create an object handler to start operation on it.
//!     let _op: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Via Builder
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::services::ipmfs;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // create backend builder
//!     let mut builder = ipfs::Builder::default();
//!
//!     // set the storage bucket for OpenDAL
//!     builder.endpoint("http://127.0.0.1:5001");
//!
//!     let op: Operator = Operator::new(builder.build()?);
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Backend;

mod builder;
pub use builder::Builder;

mod dir_stream;
mod error;
