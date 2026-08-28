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

use crate::{Entry, Metadata};

/// A complete source object accepted by composition operations.
///
/// Composition reads the whole selected object. `version`, `if_match`, and
/// `if_not_changed` identify the source; they do not apply to the destination.
#[non_exhaustive]
#[derive(Default, Debug)]
pub struct ComposeInput {
    /// The source object path.
    pub path: String,
    /// The source object version to compose.
    pub version: Option<String>,
    /// Compose only when the selected source has this exact ETag.
    pub if_match: Option<String>,
    /// Compose only when the selected source still has this metadata identity.
    pub if_not_changed: Option<Metadata>,
}

impl ComposeInput {
    /// Create a composition input for `path`.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    /// Select the source object version to compose.
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Require the selected source to have this exact ETag.
    pub fn with_if_match(mut self, etag: impl Into<String>) -> Self {
        self.if_match = Some(etag.into());
        self
    }

    /// Require the selected source to retain the identity in `metadata`.
    pub fn with_if_not_changed(mut self, metadata: &Metadata) -> Self {
        self.if_not_changed = Some(metadata.clone());
        self
    }
}

/// Converts a value into a [`ComposeInput`].
pub trait IntoComposeInput: Send + Sync + Unpin {
    /// Convert `self` into a composition input.
    fn into_compose_input(self) -> ComposeInput;
}

impl IntoComposeInput for ComposeInput {
    fn into_compose_input(self) -> ComposeInput {
        self
    }
}

impl IntoComposeInput for &str {
    fn into_compose_input(self) -> ComposeInput {
        ComposeInput::new(self)
    }
}

impl IntoComposeInput for String {
    fn into_compose_input(self) -> ComposeInput {
        ComposeInput::new(self)
    }
}

impl IntoComposeInput for Entry {
    fn into_compose_input(self) -> ComposeInput {
        let (path, metadata) = self.into_parts();
        let mut input = ComposeInput::new(path);
        if let Some(version) = metadata.version() {
            input.version = Some(version.to_string());
        }
        input
    }
}
