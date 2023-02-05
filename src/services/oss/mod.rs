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

//! Aliyun Object Storage Sevice support
//!
//! # Configuration
//!
//! - `root`: Set the work dir for backend.
//! - `bucket`: Set the container name for backend.
//! - `endpoint`: Set the endpoint for backend.
//! - `presign_endpoint`: Set the endpoint for presign.
//! - `access_key_id`: Set the access_key_id for backend.
//! - `access_key_secret`: Set the access_key_secret for backend.
//! - `role_arn`: Set the role of backend.
//! - `oidc_token`: Set the oidc_token for backend.
//! - `allow_anonymous`: Set the backend access OSS in anonymous way.
//!
//! Refer to [`OssBuilder`]'s public API docs for more information.
//!
//! # Example
//!
//! ## Via Builder
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::Oss;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create OSS backend builder.
//!     let mut builder = Oss::default();
//!     // Set the root for oss, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/path/to/dir");
//!     // Set the bucket name, this is required.
//!     builder.bucket("test");
//!     // Set the endpoint.
//!     //
//!     // For example:
//!     // - "https://oss-ap-northeast-1.aliyuncs.com"
//!     // - "https://oss-hangzhou.aliyuncs.com"
//!     builder.endpoint("https://oss-cn-beijing.aliyuncs.com");
//!     // Set the access_key_id and access_key_secret.
//!     //
//!     // OpenDAL will try load credential from the env.
//!     // If credential not set and no valid credential in env, OpenDAL will
//!     // send request without signing like anonymous user.
//!     builder.access_key_id("access_key_id");
//!     builder.access_key_secret("access_key_secret");
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
pub use backend::OssBuilder;

mod dir_stream;
mod error;
