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
use super::core::{ErrorContext, format_etag, parse_error};
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct LakefsReader {
    backend: LakefsBackend,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl LakefsReader {
    pub(super) fn new(
        backend: LakefsBackend,
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

impl oio::StreamRead for LakefsReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let path = self.path.as_str();
        let args = &self.args;
        let resp = self
            .backend
            .core
            .get_object_content(&self.ctx, path, range, args)
            .await?;
        let status = resp.status();
        let if_match = args.if_match();
        let if_none_match = args.if_none_match();

        if status == StatusCode::NOT_MODIFIED && if_none_match.is_some() {
            let (parts, _) = resp.into_parts();
            let err = Error::new(
                ErrorKind::ConditionNotMatch,
                "lakeFS object matched the If-None-Match ETag",
            )
            .with_context("service_operation", "GetObject");
            return Err(with_error_response_context(err, parts));
        }

        let serves_content = matches!(status, StatusCode::OK | StatusCode::PARTIAL_CONTENT);
        if (serves_content || status == StatusCode::RANGE_NOT_SATISFIABLE)
            && (if_match.is_some() || if_none_match.is_some())
        {
            // The lakeFS GET API ignores If-Match, and older servers also
            // ignore If-None-Match. Compare against the same response that
            // serves the content or rejects the range.
            if let Some(etag) = parse_etag(resp.headers())? {
                if if_match.is_some_and(|expected| etag != format_etag(expected))
                    || if_none_match.is_some_and(|excluded| etag == format_etag(excluded))
                {
                    return Err(Error::new(
                        ErrorKind::ConditionNotMatch,
                        "lakeFS object did not satisfy the ETag condition",
                    ));
                }
            } else if serves_content {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "lakeFS response is missing an ETag",
                ));
            }
        }

        if serves_content {
            let meta = parse_into_metadata(path, resp.headers())?;
            return Ok((RpRead::new(meta), Box::new(resp.into_body())));
        }

        let (parts, mut body) = resp.into_parts();
        let buffer = body.to_buffer().await?;
        Err(parse_error(
            ErrorContext::new(ServiceOperation("GetObject")),
            Response::from_parts(parts, buffer),
        ))
    }
}
