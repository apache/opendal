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

//! FTP support for OpenDAL.
//!
//! # Configuration
//!
//! - `endpoint`: set the endpoint for connection
//! - `root`: Set the work directory for backend
//! - `credential`:  login credentials
//! - `tls`: tls mode
//!
//! You can refer to [`Builder`]'s docs for more information
//!
//! # Example
//!
//! ## Via Builder
//!
//! ```no_run
//! use anyhow::Result;
//! use opendal::services::ftp;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // create backend builder
//!     let mut builder = ftp::Builder::default();
//!
//!     builder.endpoint("127.0.0.1");
//!
//!     let op: Operator = Operator::new(builder.build()?);
//!     let _obj: Object = op.object("test_file");
//!     Ok(())
//! }
//! ```

mod backend;
pub use backend::Builder;

mod dir_stream;
mod err;
mod util;
