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

//! Errors that returned by OpenDAL
//!
//! # Examples
//!
//! ```
//! # use anyhow::Result;
//! # use opendal::ObjectMode;
//! # use opendal::Operator;
//! use std::io::ErrorKind;
//! # use opendal::services::fs;
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! # let op = Operator::new(fs::Backend::build().root("/tmp").finish().await?);
//! if let Err(e) = op.object("test_file").metadata().await {
//!     if e.kind() == ErrorKind::NotFound {
//!         println!("object not exist")
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::io;

use thiserror::Error;

/// BackendError carries backend related context.
///
/// # Notes
///
/// This error is used to carry context only, and should never be returned to users.
/// Please wrap in [`std::io::Error`] instead.
#[derive(Error, Debug)]
#[error("backend error: (context: {context:?}, source: {source})")]
pub(crate) struct BackendError {
    context: HashMap<String, String>,
    source: anyhow::Error,
}

impl BackendError {
    pub fn new(context: HashMap<String, String>, source: impl Into<anyhow::Error>) -> Self {
        BackendError {
            context,
            source: source.into(),
        }
    }
}

/// ObjectError carries object related context.
///
/// # Notes
///
/// This error is used to carry context only, and should never be returned to users.
/// Please wrap in [`std::io::Error`] with correct [`std::io::ErrorKind`] instead.
#[derive(Error, Debug)]
#[error("object error: (op: {op}, path: {path}, source: {source})")]
pub(crate) struct ObjectError {
    op: &'static str,
    path: String,
    source: anyhow::Error,
}

impl ObjectError {
    pub fn new(op: &'static str, path: &str, source: impl Into<anyhow::Error>) -> Self {
        ObjectError {
            op,
            path: path.to_string(),
            source: source.into(),
        }
    }
}

/// Copied for [`io::Error::other`], should be removed after `io_error_other` stable.
pub(crate) fn other<E>(error: E) -> io::Error
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::Other, error.into())
}
