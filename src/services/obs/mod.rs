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

//! Huawei Cloud OBS services support.
//!
//! # Configuration
//!
//! - `root`: Set the work directory for backend
//! - `bucket`: Set the container name for backend
//! - `endpoint`: Customizable endpoint setting
//! - `access_key_id`: Set the access_key_id for backend.
//! - `secret_access_key`: Set the secret_access_key for backend.
//!
//! You can refer to [`Builder`]'s docs for more information
//!
//! # Environment
//!
//! - `OPENDAL_OBS_BUCKET`    required
//! - `OPENDAL_OBS_ENDPOINT`  optional
//! - `OPENDAL_OBS_ACCESS_KEY_ID`    optional
//! - `OPENDAL_OBS_SECRET_ACCESS_KEY`  required
//!
//! # Example
//!
//! ## Initiate via environment variables
//!
//! Set environment correctly:
//!
//! ```shell
//! export OPENDAL_OBS_BUCKET=test
//! export OPENDAL_OBS_ROOT=/path/to/dir/
//! export OPENDAL_OBS_ACCESS_KEY_ID=access_key_id
//! export OPENDAL_OBS_SECRET_ACCESS_KEY=secret_access_key
//! ```
//! ```no_run
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op: Operator = Operator::from_env(Scheme::Obs)?;
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
//! use opendal::services::obs;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // create backend builder
//!     let mut builder = obs::Builder::default();
//!
//!     // set the storage bucket for OpenDAL
//!     builder.bucket("test");
//!     // Set the access_key_id and secret_access_key.
//!     //
//!     // OpenDAL will try load credential from the env.
//!     // If credential not set and no valid credential in env, OpenDAL will
//!     // send request without signing like anonymous user.
//!     builder.access_key_id("access_key_id");
//!     builder.secret_access_key("secret_access_key");
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
pub use backend::Builder;

mod dir_stream;
mod error;
