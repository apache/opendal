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

use std::io::Result;

use time::Duration;

use super::Operation;

/// Args for `presign` operation.
///
/// The path must be normalized.
#[derive(Debug, Clone, Default)]
pub struct OpPresign {
    path: String,
    op: Operation,
    expire: Duration,
}

impl OpPresign {
    /// Create a new `OpPresign`.
    pub fn new(path: &str, op: Operation, expire: Duration) -> Result<Self> {
        Ok(Self {
            path: path.to_string(),
            op,
            expire,
        })
    }

    /// Get path from op.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get operation from op.
    pub fn operation(&self) -> Operation {
        self.op
    }

    /// Get expire from op.
    pub fn expire(&self) -> Duration {
        self.expire
    }
}

/// PresignedRequest is a presigned request return by `presign`.
#[derive(Debug, Clone)]
pub struct PresignedRequest {
    method: http::Method,
    uri: http::Uri,
    headers: http::HeaderMap,
}

impl PresignedRequest {
    /// Create a new PresignedRequest
    pub fn new(method: http::Method, uri: http::Uri, headers: http::HeaderMap) -> Self {
        Self {
            method,
            uri,
            headers,
        }
    }

    /// Return request's method.
    pub fn method(&self) -> &http::Method {
        &self.method
    }

    /// Return request's uri.
    pub fn uri(&self) -> &http::Uri {
        &self.uri
    }

    /// Return request's header.
    pub fn header(&self) -> &http::HeaderMap {
        &self.headers
    }
}
