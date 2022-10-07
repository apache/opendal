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

//! Redis support for OpenDAL
//!
//! # Note
//!
//! The storage format for this service is not **stable** yet.
//!
//! PLEASE DON'T USE THIS SERVICE FOR PERSIST DATA.
//!
//! # Configuration
//!
//! - `root`: Set the working directory of `OpenDAL`
//! - `endpoint`: Set the network address of redis server
//! - `username`: Set the username of Redis
//! - `password`: Set the password for authentication
//! - `db`: Set the DB of redis
//!
//! You can refer to [`Builder`]'s docs for more information
//!
//! # Environment
//!
//! - `OPENDAL_REDIS_ROOT` optional
//! - `OPENDAL_REDIS_ENDPOINT` optional
//! - `OPENDAL_REDIS_USERNAME` optional
//! - `OPENDAL_REDIS_PASSWORD` optional
//! - `OPENDAL_REDIS_DB` optional
//!
//! # Example
//!
//! ## Initiate via environment variables:
//!
//! Set environment correctly:
//!
//! ```shell
//! export OPENDAL_REDIS_ENDPOINT=tcp://example.com
//! export OPENDAL_REDIS_ROOT=/path/to/dir
//! export OPENDAL_REDIS_USERNAME=opendal
//! export OPENDAL_REDIS_PASSWORD=example_password
//! ```
//! ```no_run
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op = Operator::from_env(Scheme::Redis);
//!
//!     // create an object handler to start operation on redis!
//!
//!     let _op: Object = op.object("hello_redis!");
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Via Builder
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::services::redis;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut builder = redis::Builder::default();
//!
//!     // this will build a Operator accessing Redis which runs on tcp://localhost:6379
//!     let op: Operator = Operator::new(builder.build());
//!     let _: Object = op.object("test_file");
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Backend;
pub use backend::Builder;
