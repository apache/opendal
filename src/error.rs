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
//! # use opendal::Scheme;
//! use std::io::ErrorKind;
//! # use opendal::services::fs;
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! let op = Operator::from_env(Scheme::Fs)?;
//! if let Err(e) = op.object("test_file").metadata().await {
//!     if e.kind() == ErrorKind::NotFound {
//!         println!("object not exist")
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::fmt::Debug;
use std::fmt::{Display, Formatter};

use crate::ops::Operation;
use crate::Scheme;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// unexpected.
    ///
    /// OpenDAL don't know what happened here, and no actions other than just
    /// returning it back. For example, s3 returns an internal servie error.
    Unexpected,
    Unsupported,
    /// object is not found.
    ObjectNotFound,
    ObjectPermissionDenied,
    ObjectIsADirectory,
    ObjectNotADirectory,
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Unexpected => write!(f, "Unexpected"),
            ErrorKind::Unsupported => write!(f, "Unsupported"),
            ErrorKind::ObjectNotFound => write!(f, "ObjectNotFound"),
            ErrorKind::ObjectPermissionDenied => write!(f, "ObjectPermissionDenied"),
            ErrorKind::ObjectIsADirectory => write!(f, "ObjectIsADirectory"),
            ErrorKind::ObjectNotADirectory => write!(f, "ObjectNotADirectory"),
        }
    }
}

pub struct Error {
    kind: ErrorKind,

    operation: &'static str,
    message: String,
    context: Vec<(&'static str, String)>,
    source: Option<anyhow::Error>,

    retryable: bool,
    /// TODO: print retry related info.
    retried: bool,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "kind: {}, op: {}", self.kind, self.operation)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                self.context
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }

        write!(f, " => {}", self.message)
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "kind: {}, op: {} => {}",
            self.kind, self.operation, self.message
        )?;
        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "    {k}: {v}")?;
            }
        }
        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source: {:?}", source)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref())
    }
}

impl Error {
    pub fn new(kind: ErrorKind, operation: &'static str, message: &str) -> Self {
        Self {
            kind,

            operation,
            message: message.to_string(),
            context: Vec::default(),
            source: None,

            retryable: false,
            retried: false,
        }
    }

    pub fn with_context(mut self, key: &'static str, value: &str) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    pub fn with_source(mut self, src: anyhow::Error) -> Self {
        self.source = Some(src);
        self
    }

    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.retryable = retryable;
        self
    }

    pub fn with_retried(mut self) -> Self {
        self.retried = true;
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn retryable(&self) -> bool {
        self.retryable
    }
}

pub fn new_unsupported_object_error(service: Scheme, op: Operation, path: &str) -> Error {
    Error::new(
        ErrorKind::Unsupported,
        op.into_static(),
        "operation is unsupported for service",
    )
    .with_context("service", service.into_static())
    .with_context("path", path)
}

pub fn new_unexpected_object_error(
    service: Scheme,
    op: Operation,
    path: &str,
    message: &str,
) -> Error {
    Error::new(ErrorKind::Unexpected, op.into_static(), message)
        .with_context("service", service.into_static())
        .with_context("path", path)
}

pub fn new_unexpected_backend_error(service: Scheme, op: &'static str, message: &str) -> Error {
    Error::new(ErrorKind::Unexpected, op.into_static(), message)
        .with_context("service", service.into_static())
}
