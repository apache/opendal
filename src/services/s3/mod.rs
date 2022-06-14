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

//! Aws S3 and compatible services (including minio, digitalocean space and so on) support
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::s3;
//! use opendal::services::s3::Builder;
//! use opendal::Accessor;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create s3 backend builder.
//!     let mut builder: Builder = s3::Backend::build();
//!     // Set the root for s3, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/path/to/dir");
//!     // Set the bucket name, this is required.
//!     builder.bucket("test");
//!     // Set the endpoint.
//!     //
//!     // For examples:
//!     // - "https://s3.amazonaws.com"
//!     // - "http://127.0.0.1:9000"
//!     // - "https://oss-ap-northeast-1.aliyuncs.com"
//!     // - "https://cos.ap-seoul.myqcloud.com"
//!     //
//!     // Default to "https://s3.amazonaws.com"
//!     builder.endpoint("https://s3.amazonaws.com");
//!     // Set the access_key_id and secret_access_key.
//!     //
//!     // OpenDAL will try load credential from the env.
//!     // If credential not set and no valid credential in env, OpenDAL will
//!     // send request without signing like anonymous user.
//!     builder.access_key_id("access_key_id");
//!     builder.secret_access_key("secret_access_key");
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

#[doc(hidden)]
#[cfg(feature = "testing")]
pub mod tests;
