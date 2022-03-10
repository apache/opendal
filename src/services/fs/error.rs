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

use crate::error::Error;
use crate::error::Kind;

/// Parse all path related errors.
///
/// ## Notes
///
/// Skip utf-8 check to allow invalid path input.
pub fn parse_io_error(err: std::io::Error, op: &'static str, path: &str) -> Error {
    use std::io::ErrorKind;

    match err.kind() {
        ErrorKind::NotFound => Error::Object {
            kind: Kind::ObjectNotExist,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        },
        ErrorKind::PermissionDenied => Error::Object {
            kind: Kind::ObjectPermissionDenied,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        },
        _ => Error::Object {
            kind: Kind::Unexpected,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        },
    }
}
