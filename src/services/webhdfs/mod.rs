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
//! ```
//! ```no_run
//! use std::sync::Arc;
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! ```
//! ## Via Builder
//! ```no_run
//! ```

mod backend;
pub use backend::Builder;
mod dir_stream;
mod error;
mod signer;
mod uri;
use uri::percent_encode_path;
