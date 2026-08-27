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

use super::backend::*;
use super::core::parse_error;
use http::Response;
use http::StatusCode;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

const MISSING_IF_MATCH_MESSAGE: &str = "resource did not exist";

/// Reader returned by this backend.
pub struct WebdavReader {
    backend: WebdavBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl WebdavReader {
    pub(super) fn new(
        backend: WebdavBackend,
        ctx: OperationContext,
        path: &str,
        args: OpRead,
    ) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for WebdavReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let resp = backend
            .core
            .webdav_get(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();

        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_metadata(path, resp.headers())?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_read_error(Response::from_parts(part, buf), &args));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

fn parse_read_error(resp: Response<Buffer>, args: &OpRead) -> Error {
    if resp.status() == StatusCode::PRECONDITION_FAILED
        && args.if_match().is_some()
        && String::from_utf8_lossy(&resp.body().to_bytes()).contains(MISSING_IF_MATCH_MESSAGE)
    {
        let (parts, body) = resp.into_parts();
        let err = Error::new(
            ErrorKind::NotFound,
            String::from_utf8_lossy(&body.to_bytes()).into_owned(),
        );
        return with_error_response_context(err, parts);
    }

    parse_error(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_read_error_maps_missing_if_match_to_not_found() {
        let resp = Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .body(Buffer::from(
                "An If-Match header was specified and the resource did not exist",
            ))
            .unwrap();

        let err = parse_read_error(resp, &OpRead::new().with_if_match("etag"));
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn test_parse_read_error_preserves_etag_mismatch() {
        let resp = Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .body(Buffer::from("The ETag did not match"))
            .unwrap();

        let err = parse_read_error(resp, &OpRead::new().with_if_match("etag"));
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
    }
}
