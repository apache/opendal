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

use std::io::Error;

use crate::error::ObjectError;
use crate::ops::Operation;

/// Parse error response into io::Error.
///
/// # TODO
///
/// In the future, we may have our own error struct.

pub fn parse_io_error(err: Error, op: Operation, path: &str) -> Error {
    Error::new(err.kind(), ObjectError::new(op, path, err))
}
