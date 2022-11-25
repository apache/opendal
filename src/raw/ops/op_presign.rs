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

use http::Request;

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
