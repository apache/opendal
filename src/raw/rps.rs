// Copyright 2022 Datafuse Labs
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

/// Reply for `create_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct RpCreateMultipart {
    upload_id: String,
}

impl RpCreateMultipart {
    /// Create a new reply for create_multipart.
    pub fn new(upload_id: &str) -> Self {
        Self {
            upload_id: upload_id.to_string(),
        }
    }

    /// Get the upload_id.
    pub fn upload_id(&self) -> &str {
        &self.upload_id
    }
}

/// Reply for `write_multipart` operation.
#[derive(Debug, Clone)]
pub struct RpWriteMultipart {
    part_number: usize,
    etag: String,
}

impl RpWriteMultipart {
    /// Create a new reply for `write_multipart`.
    pub fn new(part_number: usize, etag: &str) -> Self {
        Self {
            part_number,
            etag: etag.to_string(),
        }
    }

    /// Get the part_number from reply.
    pub fn part_number(&self) -> usize {
        self.part_number
    }

    /// Get the etag from reply.
    pub fn etag(&self) -> &str {
        &self.etag
    }

    /// Consume reply to build a object part.
    pub fn into_object_part(self) -> ObjectPart {
        ObjectPart::new(self.part_number, &self.etag)
    }
}

/// Reply for `complete_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct RpCompleteMultipart {}

/// Reply for `abort_multipart` operation.
#[derive(Debug, Clone, Default)]
pub struct RpAbortMultipart {}

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
    meta: ObjectMetadata,
}

impl RpRead {
    /// Create a new reply.
    pub fn new(content_length: u64) -> Self {
        RpRead {
            meta: ObjectMetadata::new(ObjectMode::FILE).with_content_length(content_length),
        }
    }

    /// Create reply read with existing object metadata.
    pub fn with_metadata(meta: ObjectMetadata) -> Self {
        RpRead { meta }
    }

    /// Get a ref of object metadata.
    pub fn metadata(&self) -> &ObjectMetadata {
        &self.meta
    }

    /// Consume reply to get the object meta.
    pub fn into_metadata(self) -> ObjectMetadata {
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

/// Batch results of `bacth` operations.
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
    meta: ObjectMetadata,
}

impl RpStat {
    /// Create a new reply for stat.
    pub fn new(meta: ObjectMetadata) -> Self {
        RpStat { meta }
    }

    /// Operate on inner metadata.
    pub fn map_metadata(mut self, f: impl FnOnce(ObjectMetadata) -> ObjectMetadata) -> Self {
        self.meta = f(self.meta);
        self
    }

    /// Consume RpStat to get the inner metadata.
    pub fn into_metadata(self) -> ObjectMetadata {
        self.meta
    }
}

/// Reply for `write` operation.
#[derive(Debug, Clone, Default)]
pub struct RpWrite {
    written: u64,
}

impl RpWrite {
    /// Create a new reply for write.
    pub fn new(written: u64) -> Self {
        Self { written }
    }

    /// Get the written size (in bytes) of write operation.
    pub fn written(&self) -> u64 {
        self.written
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
            uri: Uri::from_static("https://opendal.databend.rs/path/to/file"),
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
            "https://opendal.databend.rs/path/to/file",
            req.uri().to_string()
        );
        assert_eq!("123", req.headers().get(CONTENT_LENGTH).unwrap());
        assert_eq!("application/json", req.headers().get(CONTENT_TYPE).unwrap());

        let req: Request<Body> = pr.into();
        assert_eq!(Method::PATCH, req.method());
        assert_eq!(
            "https://opendal.databend.rs/path/to/file",
            req.uri().to_string()
        );
        assert_eq!("123", req.headers().get(CONTENT_LENGTH).unwrap());
        assert_eq!("application/json", req.headers().get(CONTENT_TYPE).unwrap());

        Ok(())
    }
}
