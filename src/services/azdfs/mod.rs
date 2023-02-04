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

//! Azure Data Lake Storage Gen2 Support.
//!
//! As known as `abfs`, `azdfs` or `azdls`.
//!
//! This service will visist the [ABFS](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-abfs-driver) URI supported by [Azure Data Lake Storage Gen2](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction).
//!
//! # Configuration
//!
//! - `root`: Set the work dir for backend.
//! - `filesystem`: Set the filesystem name for backend.
//! - `endpoint`: Set the endpoint for backend.
//! - `account_name`: Set the account_name for backend.
//! - `account_key`: Set the account_key for backend.
//!
//! Refer to [`Builder`]'s public API docs for more information.
//!
//! # Example
//!
//! ## Init OpenDAL Operator
//!
//! ### Via Builder
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::Azdfs;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create azblob backend builder.
//!     let mut builder = Azdfs::default();
//!     // Set the root for azblob, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/path/to/dir");
//!     // Set the filesystem name, this is required.
//!     builder.filesystem("test");
//!     // Set the endpoint, this is required.
//!     //
//!     // For examples:
//!     // - "https://accountname.dfs.core.windows.net"
//!     builder.endpoint("https://accountname.dfs.core.windows.net");
//!     // Set the account_name and account_key.
//!     //
//!     // OpenDAL will try load credential from the env.
//!     // If credential not set and no valid credential in env, OpenDAL will
//!     // send request without signing like anonymous user.
//!     builder.account_name("account_name");
//!     builder.account_key("account_key");
//!
//!     // `Accessor` provides the low level APIs, we will use `Operator` normally.
//!     let op: Operator = Operator::create(builder)?.finish();
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```
mod backend;
pub use backend::Builder;

mod dir_stream;
mod error;
