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

//! Google Cloud Storage support for OpenDAL.
//!
//! # Configuration
//!
//! - `root`: Set the work directory for backend
//! - `bucket`: Set the container name for backend
//! - `endpoint`: Customizable endpoint setting
//! - `credentials`: Credential string for GCS OAuth2
//!
//! You can refer to [`Builder`]'s docs for more information
//!
//! # Environment
//!
//! - `OPENDAL_GCS_ENDPOINT`    optional
//! - `OPENDAL_GCS_BUCKET`  required
//! - `OPENDAL_GCS_ROOT`    optional
//! - `OPENDAL_GCS_CREDENTIAL`  required
//!
//! # Example
//!
//! ## Initiate via environment variables
//!
//! Set environment correctly:
//!
//! ```shell
//! export OPENDAL_GCS_ENDPOINT=https://storage.googleapis.com  # by default
//! export OPENDAL_GCS_BUCKET=test
//! export OPENDAL_GCS_ROOT=/path/to/dir/   # if not set, will be seen as "/"
//! export OPENDAL_GCS_CREDENTIAL=secret_oauth2_credential
//! ```
//! ```no_run
//! use anyhow::Result;
//! use opendal::Operator;
//! use opendal::Scheme;
//! use opendal::Object;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op: Operator = Operator::from_env(Scheme::Gcs)?;
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
//!
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::services::gcs;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // create backend builder
//!     let mut builder = gcs::Builder::default();
//!
//!     // set the storage bucket for OpenDAL
//!     builder.bucket("test");
//!     // set the working directory root for GCS
//!     // all operations will happen within it
//!     builder.root("/path/to/dir");
//!     // set the credentials for GCS OAUTH2 authentication
//!     builder.credential("authentication token");
//!
//!     let op: Operator = Operator::new(builder.build()?);
//!     let _obj: Object = op.object("test_file");
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Backend;
pub use backend::Builder;

mod dir_stream;
mod error;
mod uri;
