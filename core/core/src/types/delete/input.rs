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
use crate::{Entry, Metadata};

/// Per-entry input for delete operations.
///
/// Each conditional field checks the file at this entry's path and follows
/// the same contract as the matching [`crate::options::DeleteOptions`] field;
/// refer there for the full behavior and error semantics.
#[non_exhaustive]
#[derive(Default, Debug)]
pub struct DeleteInput {
    /// The path of the file to delete.
    pub path: String,
    /// The version of the file to delete.
    ///
    /// Refer to [`crate::options::DeleteOptions::version`] for the full contract.
    pub version: Option<String>,
    /// Whether to perform recursive deletion.
    pub recursive: bool,
    /// Delete only when the file at this path has this exact ETag.
    ///
    /// Refer to [`crate::options::DeleteOptions::if_match`] for the full contract.
    pub if_match: Option<String>,
    /// Delete only when the file at this path does not have this ETag.
    ///
    /// Refer to [`crate::options::DeleteOptions::if_none_match`] for the full contract.
    pub if_none_match: Option<String>,
    /// Delete only when the file at this path has this exact version.
    ///
    /// Refer to [`crate::options::DeleteOptions::if_version_match`] for the full contract.
    pub if_version_match: Option<String>,
    /// Delete only when the file at this path does not have this version.
    ///
    /// Refer to [`crate::options::DeleteOptions::if_version_not_match`] for the
    /// full contract.
    pub if_version_not_match: Option<String>,
    /// Delete only when the file still has the identity recorded in this metadata.
    ///
    /// Refer to [`crate::options::DeleteOptions::if_not_changed`] for the full contract.
    pub if_not_changed: Option<Metadata>,
}

impl DeleteInput {
    /// Create a delete input for `path`.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    /// Select the file version to delete.
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Configure recursive deletion.
    pub fn with_recursive(mut self, recursive: bool) -> Self {
        self.recursive = recursive;
        self
    }

    /// Delete only when the file has this exact `etag`.
    pub fn with_if_match(mut self, etag: impl Into<String>) -> Self {
        self.if_match = Some(etag.into());
        self
    }

    /// Delete only when the file does not have this `etag`.
    pub fn with_if_none_match(mut self, etag: impl Into<String>) -> Self {
        self.if_none_match = Some(etag.into());
        self
    }

    /// Delete only when the file has this exact `version`.
    pub fn with_if_version_match(mut self, version: impl Into<String>) -> Self {
        self.if_version_match = Some(version.into());
        self
    }

    /// Delete only when the file does not have this `version`.
    pub fn with_if_version_not_match(mut self, version: impl Into<String>) -> Self {
        self.if_version_not_match = Some(version.into());
        self
    }

    /// Delete only when the file still has the identity recorded in `metadata`.
    pub fn with_if_not_changed(mut self, metadata: &Metadata) -> Self {
        self.if_not_changed = Some(metadata.clone());
        self
    }
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
        DeleteInput::new(self)
    }
}

/// Implement `IntoDeleteInput` for `String` so we can use `Vec<String>` as a DeleteInput stream.
impl IntoDeleteInput for String {
    fn into_delete_input(self) -> DeleteInput {
        DeleteInput::new(self)
    }
}

/// Implement `IntoDeleteInput` for `(String, OpDelete)` so we can use `(String, OpDelete)`
/// as a DeleteInput stream.
impl IntoDeleteInput for (String, OpDelete) {
    fn into_delete_input(self) -> DeleteInput {
        let (path, mut args) = self;

        let mut input = DeleteInput {
            path,
            recursive: args.recursive(),
            ..Default::default()
        };

        if let Some(version) = args.version() {
            input.version = Some(version.to_string());
        }
        if let Some(etag) = args.if_match() {
            input.if_match = Some(etag.to_string());
        }
        if let Some(etag) = args.if_none_match() {
            input.if_none_match = Some(etag.to_string());
        }
        if let Some(version) = args.if_version_match() {
            input.if_version_match = Some(version.to_string());
        }
        if let Some(version) = args.if_version_not_match() {
            input.if_version_not_match = Some(version.to_string());
        }
        if let Some(identity) = args.take_if_not_changed() {
            let mut metadata = Metadata::default();
            if let Some(version) = identity.version() {
                metadata = metadata.with_version(version.to_string());
            }
            if let Some(etag) = identity.etag() {
                metadata = metadata.with_etag(etag.to_string());
            }
            input.if_not_changed = Some(metadata);
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
            recursive: false,
            ..Default::default()
        };

        if let Some(version) = meta.version() {
            input.version = Some(version.to_string());
        }
        input
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_op_delete_input_preserves_if_match() {
        let input = (
            "path".to_string(),
            OpDelete::new().with_if_match("\"etag\""),
        )
            .into_delete_input();

        assert_eq!(input.if_match.as_deref(), Some("\"etag\""));
    }

    #[test]
    fn test_op_delete_input_preserves_if_not_changed() {
        let metadata = Metadata::default()
            .with_version("version".to_string())
            .with_etag("etag".to_string());
        let args = crate::options::DeleteOptions {
            if_not_changed: Some(metadata),
            ..Default::default()
        }
        .into();

        let input = ("path".to_string(), args).into_delete_input();
        let metadata = input.if_not_changed.expect("identity must be preserved");

        assert_eq!(metadata.version(), Some("version"));
        assert_eq!(metadata.etag(), Some("etag"));
    }
}
