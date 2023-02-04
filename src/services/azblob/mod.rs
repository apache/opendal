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

//! Azure Storage Blob services support.
//!
//! # Configuration
//!
//! - `root`: Set the work dir for backend.
//! - `container`: Set the container name for backend.
//! - `endpoint`: Set the endpoint for backend.
//! - `account_name`: Set the account_name for backend.
//! - `account_key`: Set the account_key for backend.
//!
//! Refer to [`Builder`]'s public API docs for more information.
//!
//! # Example
//!
//! This example works on [Azurite](https://github.com/Azure/Azurite) for local developments.
//!
//! ## Start local blob service
//!
//! ```shell
//! docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite
//! az storage container create --name test --connection-string "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
//! ```
//!
//! ## Init OpenDAL Operator
//!
//! ### Via Builder
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::Azblob;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create azblob backend builder.
//!     let mut builder = Azblob::default();
//!     // Set the root for azblob, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/path/to/dir");
//!     // Set the container name, this is required.
//!     builder.container("test");
//!     // Set the endpoint, this is required.
//!     //
//!     // For examples:
//!     // - "http://127.0.0.1:10000/devstoreaccount1"
//!     // - "https://accountname.blob.core.windows.net"
//!     builder.endpoint("http://127.0.0.1:10000/devstoreaccount1");
//!     // Set the account_name and account_key.
//!     //
//!     // OpenDAL will try load credential from the env.
//!     // If credential not set and no valid credential in env, OpenDAL will
//!     // send request without signing like anonymous user.
//!     builder.account_name("devstoreaccount1");
//!     builder.account_key("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==");
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
pub use backend::AzblobBuilder;

mod dir_stream;
mod error;
