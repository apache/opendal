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

//! Github Action Cache Services support.
//!
//! # Notes
//!
//! This service is mainly provided by github actions.
//!
//! Refer to [Caching dependencies to speed up workflows](https://docs.github.com/en/actions/using-workflows/caching-dependencies-to-speed-up-workflows) for more informatio.
//!
//! To make this service work as expected, please make sure the following
//! environment has been setup correctly:
//!
//! - `ACTIONS_CACHE_URL`
//! - `ACTIONS_RUNTIME_TOKEN`
//!
//! They can be exposed by following action:
//!
//! ```yaml
//! - name: Configure Cache Env
//!   uses: actions/github-script@v6
//!   with:
//!     script: |
//!       core.exportVariable('ACTIONS_CACHE_URL', process.env.ACTIONS_CACHE_URL || '');
//!       core.exportVariable('ACTIONS_RUNTIME_TOKEN', process.env.ACTIONS_RUNTIME_TOKEN || '');
//! ```
//!
//! To make `delete` work as expected, `GITHUB_TOKEN` should also be set via:
//!
//! ```yaml
//! env:
//!   GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
//! ```
//!
//! # Limitations
//!
//! Unlike other services, ghac doesn't support create empty files.
//! We provide a `enable_create_simulation()` to support this operation but may result unexpected side effects.
//!
//! Also, `ghac` is a cache service which means the data store inside could
//! be automatically evicted at any time.
//!
//! # Configuration
//!
//! - `root`: Set the work dir for backend.
//!
//! Refer to [`Builder`]'s public API docs for more information.
//!
//! # Environment
//!
//! - `OPENDAL_GHAC_ROOT`
//!
//! # Example
//!
//! ## Via Environment
//!
//! Set environment correctly:
//!
//! ```shell
//! export OPENDAL_GHAC_ROOT=/path/to/dir/
//! ```
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::Object;
//! use opendal::Operator;
//! use opendal::Scheme;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let op: Operator = Operator::from_env(Scheme::Ghac)?;
//!
//!     // Create an object handle to start operation on object.
//!     let _: Object = op.object("test_file");
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Via Builder
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use anyhow::Result;
//! use opendal::services::ghac;
//! use opendal::Object;
//! use opendal::Operator;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create ghac backend builder.
//!     let mut builder = ghac::Builder::default();
//!     // Set the root for ghac, all operations will happen under this root.
//!     //
//!     // NOTE: the root must be absolute path.
//!     builder.root("/path/to/dir");
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

mod error;
