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

use http::Request;

use crate::*;

/// Reply for `create` operation
#[derive(Debug, Clone, Default)]
pub struct RpCreate {}

/// Reply for `delete` operation
#[derive(Debug, Clone, Default)]
pub struct RpDelete {}

/// Reply for `list` operation.
#[derive(Debug, Clone, Default)]
pub struct RpList {}

/// Reply for `scan` operation.
#[derive(Debug, Clone, Default)]
pub struct RpScan {}

/// Reply for `presign` operation.
#[derive(Debug, Clone)]
pub struct RpPresign {
    req: PresignedRequest,
}

impl RpPresign {
    /// Create a new reply for presign.
    pub fn new(req: PresignedRequest) -> Self {
        RpPresign { req }
    }

    /// Consume reply to build a presigned request.
    pub fn into_presigned_request(self) -> PresignedRequest {
        self.req
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

impl<T: Default> From<PresignedRequest> for Request<T> {
    fn from(v: PresignedRequest) -> Self {
        let mut builder = Request::builder().method(v.method).uri(v.uri);

        let headers = builder.headers_mut().expect("header map must be valid");
        headers.extend(v.headers);

        builder
            .body(T::default())
            .expect("request must build succeed")
    }
}

/// Reply for `read` operation.
#[derive(Debug, Clone)]
pub struct RpRead {
    meta: Metadata,
}

impl RpRead {
    /// Create a new reply.
    pub fn new(content_length: u64) -> Self {
        RpRead {
            meta: Metadata::new(EntryMode::FILE).with_content_length(content_length),
        }
    }

    /// Create reply read with existing metadata.
    pub fn with_metadata(meta: Metadata) -> Self {
        RpRead { meta }
    }

    /// Get a ref of metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.meta
    }

    /// Consume reply to get the meta.
    pub fn into_metadata(self) -> Metadata {
        self.meta
    }
}

/// Reply for `batch` operation.
pub struct RpBatch {
    results: BatchedResults,
}

impl RpBatch {
    /// Create a new RpBatch.
    pub fn new(results: BatchedResults) -> Self {
        Self { results }
    }

    /// Get the results from RpBatch.
    pub fn results(&self) -> &BatchedResults {
        &self.results
    }

    /// Consume RpBatch to get the batched results.
    pub fn into_results(self) -> BatchedResults {
        self.results
    }
}

/// Batch results of `batch` operations.
pub enum BatchedResults {
    /// results of delete batch operation
    Delete(Vec<(String, Result<RpDelete>)>),
}

impl BatchedResults {
    /// Return the length of given results.
    pub fn len(&self) -> usize {
        use BatchedResults::*;
        match self {
            Delete(v) => v.len(),
        }
    }

    /// Return if given results is empty.
    pub fn is_empty(&self) -> bool {
        use BatchedResults::*;
        match self {
            Delete(v) => v.is_empty(),
        }
    }

    /// Return the length of ok results.
    pub fn len_ok(&self) -> usize {
        use BatchedResults::*;
        match self {
            Delete(v) => v.iter().filter(|v| v.1.is_ok()).count(),
        }
    }

    /// Return the length of error results.
    pub fn len_err(&self) -> usize {
        use BatchedResults::*;
        match self {
            Delete(v) => v.iter().filter(|v| v.1.is_err()).count(),
        }
    }
}

/// Reply for `stat` operation.
#[derive(Debug, Clone)]
pub struct RpStat {
    meta: Metadata,
}

impl RpStat {
    /// Create a new reply for stat.
    pub fn new(meta: Metadata) -> Self {
        RpStat { meta }
    }

    /// Operate on inner metadata.
    pub fn map_metadata(mut self, f: impl FnOnce(Metadata) -> Metadata) -> Self {
        self.meta = f(self.meta);
        self
    }

    /// Consume RpStat to get the inner metadata.
    pub fn into_metadata(self) -> Metadata {
        self.meta
    }
}

/// Reply for `write` operation.
#[derive(Debug, Clone, Default)]
pub struct RpWrite {}

impl RpWrite {
    /// Create a new reply for write.
    pub fn new() -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use http::header::CONTENT_LENGTH;
    use http::header::CONTENT_TYPE;
    use http::HeaderMap;
    use http::Method;
    use http::Uri;

    use super::*;
    use crate::raw::*;

    #[test]
    fn test_presigned_request_convert() -> Result<()> {
        let pr = PresignedRequest {
            method: Method::PATCH,
            uri: Uri::from_static("https://opendal.apache.org/path/to/file"),
            headers: {
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_LENGTH, "123".parse()?);
                headers.insert(CONTENT_TYPE, "application/json".parse()?);

                headers
            },
        };

        let req: Request<AsyncBody> = pr.clone().into();
        assert_eq!(Method::PATCH, req.method());
        assert_eq!(
            "https://opendal.apache.org/path/to/file",
            req.uri().to_string()
        );
        assert_eq!("123", req.headers().get(CONTENT_LENGTH).unwrap());
        assert_eq!("application/json", req.headers().get(CONTENT_TYPE).unwrap());

        let req: Request<Body> = pr.into();
        assert_eq!(Method::PATCH, req.method());
        assert_eq!(
            "https://opendal.apache.org/path/to/file",
            req.uri().to_string()
        );
        assert_eq!("123", req.headers().get(CONTENT_LENGTH).unwrap());
        assert_eq!("application/json", req.headers().get(CONTENT_TYPE).unwrap());

        Ok(())
    }
}
