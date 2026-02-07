// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use foyer::Error as FoyerError;

use opendal_core::Error;
use opendal_core::ErrorKind;

/// Custom error type for when fetched data exceeds size limit.
#[derive(Debug)]
pub(crate) struct FetchSizeTooLarge;

impl std::fmt::Display for FetchSizeTooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fetched data size exceeds size limit")
    }
}

impl std::error::Error for FetchSizeTooLarge {}

pub(crate) fn extract_err(e: FoyerError) -> Error {
    let e = match e.downcast::<Error>() {
        Ok(e) => return e,
        Err(e) => e,
    };
    Error::new(ErrorKind::Unexpected, e.to_string())
}
