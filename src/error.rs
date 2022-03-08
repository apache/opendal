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

use std::collections::HashMap;
use std::io;

use thiserror::Error;

// TODO: implement From<Result> for `common_exception::Result`.s
pub type Result<T> = std::result::Result<T, Error>;

/// Kind is all meaningful error kind, that means you can depend on `Kind` to
/// take some actions instead of just print. For example, you can try check
/// `ObjectNotExist` before starting a write operation.
///
/// # Style
///
/// The kind will be named as `noun-adj`. For example, `ObjectNotExist` or
/// `ObjectPermissionDenied`.
#[derive(Error, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum Kind {
    #[error("backend not supported")]
    BackendNotSupported,
    #[error("backend configuration invalid")]
    BackendConfigurationInvalid,

    #[error("object not exist")]
    ObjectNotExist,
    #[error("object permission denied")]
    ObjectPermissionDenied,

    #[error("unexpected")]
    Unexpected,
}

/// Error is the error type for the dal2 crate.
///
/// ## Style
///
/// The error will be formatted as `description: (keyA: valueA, keyB: valueB, ...)`.
#[derive(Error, Debug)]
pub enum Error {
    #[error("{kind}: (context: {context:?}, source: {source})")]
    Backend {
        kind: Kind,
        context: HashMap<String, String>,
        source: anyhow::Error,
    },

    #[error("{kind}: (op: {op}, path: {path}, source: {source})")]
    Object {
        kind: Kind,
        op: &'static str,
        path: String,
        source: anyhow::Error,
    },

    #[error("unexpected: (source: {0})")]
    Unexpected(#[from] anyhow::Error),
}

impl Error {
    pub fn kind(&self) -> Kind {
        match self {
            Error::Backend { kind, .. } => *kind,
            Error::Object { kind, .. } => *kind,
            Error::Unexpected(_) => Kind::Unexpected,
        }
    }
}

// Make it easier to convert to `std::io::Error`
impl From<Error> for io::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::Backend { .. } => io::Error::new(io::ErrorKind::Other, err),
            Error::Object { kind, .. } => match kind {
                Kind::ObjectNotExist => io::Error::new(io::ErrorKind::NotFound, err),
                Kind::ObjectPermissionDenied => {
                    io::Error::new(io::ErrorKind::PermissionDenied, err)
                }
                _ => io::Error::new(io::ErrorKind::Other, err),
            },
            Error::Unexpected(_) => io::Error::new(io::ErrorKind::Other, err),
        }
    }
}
