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

use crate::raw::OpDelete;
use crate::Entry;

/// DeleteInput is the input for delete operations.
#[non_exhaustive]
#[derive(Default, Debug)]
pub struct DeleteInput {
    /// The path of the path to delete.
    pub path: String,
    /// The version of the path to delete.
    pub version: Option<String>,
}

/// IntoDeleteInput is a helper trait that makes it easier for users to play with `Deleter`.
pub trait IntoDeleteInput: Send + Sync + Unpin {
    /// Convert `self` into a `DeleteInput`.
    fn into_delete_input(self) -> DeleteInput;
}

/// Implement `IntoDeleteInput` for `DeleteInput` self.
impl IntoDeleteInput for DeleteInput {
    fn into_delete_input(self) -> DeleteInput {
        self
    }
}

/// Implement `IntoDeleteInput` for `&str` so we can use `&str` as a DeleteInput.
impl IntoDeleteInput for &str {
    fn into_delete_input(self) -> DeleteInput {
        DeleteInput {
            path: self.to_string(),
            ..Default::default()
        }
    }
}

/// Implement `IntoDeleteInput` for `String` so we can use `Vec<String>` as a DeleteInput stream.
impl IntoDeleteInput for String {
    fn into_delete_input(self) -> DeleteInput {
        DeleteInput {
            path: self,
            ..Default::default()
        }
    }
}

/// Implement `IntoDeleteInput` for `(String, OpDelete)` so we can use `(String, OpDelete)`
/// as a DeleteInput stream.
impl IntoDeleteInput for (String, OpDelete) {
    fn into_delete_input(self) -> DeleteInput {
        let (path, args) = self;

        let mut input = DeleteInput {
            path,
            ..Default::default()
        };

        if let Some(version) = args.version() {
            input.version = Some(version.to_string());
        }
        input
    }
}

/// Implement `IntoDeleteInput` for `Entry` so we can use `Lister` as a DeleteInput stream.
impl IntoDeleteInput for Entry {
    fn into_delete_input(self) -> DeleteInput {
        let (path, meta) = self.into_parts();

        let mut input = DeleteInput {
            path,
            ..Default::default()
        };

        if let Some(version) = meta.version() {
            input.version = Some(version.to_string());
        }
        input
    }
}
