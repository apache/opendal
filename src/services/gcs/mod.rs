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
//! You can refer to [`GcsBuilder`]'s docs for more information
//!
//! # Example
//!
//! ## Via Builder
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::services::Gcs;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // create backend builder
//!     let mut builder = Gcs::default();
//!
//!     // set the storage bucket for OpenDAL
//!     builder.bucket("test");
//!     // set the working directory root for GCS
//!     // all operations will happen within it
//!     builder.root("/path/to/dir");
//!     // set the credentials for GCS OAUTH2 authentication
//!     builder.credential("authentication token");
//!
//!     let op: Operator = Operator::create(builder)?.finish();
//!     let _: Object = op.object("test_file");
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::GcsBuilder;

mod dir_stream;
mod error;
mod uri;
