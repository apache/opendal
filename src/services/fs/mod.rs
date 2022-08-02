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

//! POSIX file system support.
//!
//! # Configuration
//!
//! - `root`: Set the work dir for backend.
//!
//! Refer to [`Builder`]'s public API docs for more information.
//!
//! # Environment
//!
//! - `OPENDAL_FS_ROOT`
//!
//! # Example
//!
//! ## Via Environment
//!
//! Set environment correctly:
//!
//! ```shell
//! export OPENDAL_FS_ROOT=/path/to/dir/
//! ```
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op: Operator = Operator::from_env(Scheme::Fs).await?;
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Via Builder
//!
//! ```
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::fs;
//! use opendal::services::fs::Builder;
//! use opendal::Accessor;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create fs backend builder.
//!     let mut builder: Builder = fs::Backend::build();
//!     // Set the root for fs, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/tmp");
//!     // Build the `Accessor`.
//!     let accessor: Arc<dyn Accessor> = builder.finish().await?;
//!
//!     // `Accessor` provides the low level APIs, we will use `Operator` normally.
//!     let op: Operator = Operator::new(accessor);
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Backend;
pub use backend::Builder;

mod dir_stream;
mod error;
