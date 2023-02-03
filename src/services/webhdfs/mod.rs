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

//! WebHDFS's RESTFul API support
//!
//! # Configurations
//!
//! - `root`: The root path of the WebHDFS service.
//! - `endpoint`: The endpoint of the WebHDFS service.
//! - `username`: The username of the WebHDFS service.
//! - `delegation`: The delegation token for WebHDFS.
//!
//! Refer to [`Builder`]'s public API docs for more information
//!
//! # Environment
//!
//! - `OPENDAL_WEBHDFS_ROOT`
//! - `OPENDAL_WEBHDFS_ENDPOINT`
//! - `OPENDAL_WEBHDFS_USERNAME`
//! - `OPENDAL_WEBHDFS_DELEGATION`
//! - `OPENDAL_WEBHDFS_CSRF`
//!
//! # Examples
//! ## Via Environment
//!
//! Set the environment variables and then use [`WebHdfs::try_from_env`]:
//!
//! ```shell
//! export OPENDAL_WEBHDFS_ROOT=/path/to/dir
//! export OPENDAL_WEBHDFS_DELEGATION=<delegation_token>
//! export OPENDAL_WEBHDFS_USERNAME=<username>
//! export OPENDAL_WEBHDFS_ENDPOINT=localhost:50070
//! export OPENDAL_WEBHDFS_CSRF=X-XSRF-HEADER
//! ```
//! ```no_run
//! use std::sync::Arc;
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op: Operator = Operator::from_env(Scheme::WebHdfs)?;
//!     let _: Object = op.object("test_file");
//!     Ok(())
//! }
//! ```
//! ## Via Builder
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::webhdfs;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut builder = webhdfs::Builder::default();
//!     // set the root for s3, all operations will happend under this root
//!     //
//!     // Note:
//!     // if the root is not exists, the builder will automatically create the
//!     // root directory for you
//!     // if the root exists and is a directory, the builder will continue working
//!     // if the root exists and is a folder, the builder will fail on building backend
//!     builder.root("/path/to/dir");
//!     // set the endpoint of webhdfs namenode, controled by dfs.namenode.http-address
//!     // default is http://127.0.0.1:9870
//!     builder.endpoint("http://127.0.0.1:9870");

//!     // set the delegation_token for builder
//!     builder.delegation("delegation_token");
//!     // or set the username for builder
//!     builder.user("username");
//!     // proxy user is also supported
//!     builder.doas("proxy_user");
//!     // if no delegation token, username and proxy user are set
//!     // the backend will query without authentications
//!
//!     // custom CSRF header is also customizable
//!     // default is X-XSRF-HEADER
//!     builder.csrf("X-XSRF-HEADER");
//!
//!     let op: Operator = Operator::new(builder.build()?);
//!
//!     // create an object handler to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Builder;
mod auth;
mod dir_stream;
mod error;
mod uri;
use uri::percent_encode_path;
