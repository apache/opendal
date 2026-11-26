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

use crate::backend::*;
use crate::core::ErrorContext;
use crate::core::constants::X_AMZ_CHECKSUM_TYPE;
use crate::core::constants::X_AMZ_META_PREFIX;
use crate::core::constants::X_AMZ_VERSION_ID;
use crate::core::parse_error;
use crate::core::parse_s3_checksum;
use http::HeaderMap;
use http::Response;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct S3Reader {
    backend: S3Backend,
    // StreamRead::open can run after Service::read returns, so keep the
    // per-operation context with the reader.
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl S3Reader {
    pub(super) fn new(backend: S3Backend, ctx: OperationContext, path: &str, args: OpRead) -> Self {
        Self {
            backend,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::StreamRead for S3Reader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let args = self.args.clone();
        let error_ctx = ErrorContext::new(ServiceOperation("GetObject"))
            .with_caller_condition(args.is_conditional());
        let resp = backend
            .core
            .s3_get_object(&self.ctx, path, range, &args)
            .await?;

        let status = resp.status();
        let checksum = args.checksum_for_range(range)
            && backend.core.capability.read_with_checksum
            && (status == StatusCode::OK
                || resp.headers().contains_key(http::header::CONTENT_RANGE));
        let (rp, stream) = match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => (
                RpRead::new(parse_into_s3_metadata(path, resp.headers(), checksum)?),
                resp.into_body(),
            ),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                return Err(parse_error(error_ctx, Response::from_parts(part, buf)));
            }
        };

        Ok((rp, Box::new(stream) as Box<dyn oio::ReadStreamDyn>))
    }
}

pub(super) fn parse_into_s3_metadata(
    path: &str,
    headers: &HeaderMap,
    checksum_requested: bool,
) -> Result<Metadata> {
    let mut meta = parse_into_metadata(path, headers)?.into_builder();

    let user_meta = parse_prefixed_headers(headers, X_AMZ_META_PREFIX);
    if !user_meta.is_empty() {
        meta.user_metadata(user_meta);
    }

    if let Some(v) = parse_header_to_str(headers, X_AMZ_VERSION_ID)? {
        meta.version(v);
    }
    if checksum_requested && let Some(checksum) = parse_s3_read_checksum(headers) {
        // A 206 response can describe the entire object when the requested
        // range spans all of its bytes.
        let whole_object = parse_content_range(headers)?.is_none_or(|range| {
            range
                .size()
                .is_some_and(|size| range.range() == Some(0..size))
        });
        if whole_object {
            meta.checksum(checksum);
        }
    }
    Ok(meta.build())
}

fn parse_s3_read_checksum(headers: &HeaderMap) -> Option<Checksum> {
    // COMPOSITE and unknown types do not describe a full-object checksum.
    if headers
        .get(X_AMZ_CHECKSUM_TYPE)
        .is_some_and(|value| value != "FULL_OBJECT")
    {
        return None;
    }

    // A response without a type can still carry an unsuffixed single-part
    // checksum. The base64 decoder rejects multipart values ending in -N.
    [
        ("x-amz-checksum-crc64nvme", ChecksumAlgo::Crc64Nvme),
        ("x-amz-checksum-crc32c", ChecksumAlgo::Crc32c),
        ("x-amz-checksum-sha256", ChecksumAlgo::Sha256),
    ]
    .into_iter()
    .find_map(|(header, algo)| parse_s3_checksum(algo, headers.get(header)?.to_str().ok()?))
}

#[cfg(test)]
mod tests {
    use http::HeaderValue;

    use super::*;

    #[test]
    fn test_s3_checksum_ignores_composite_and_malformed_values() {
        let mut headers = HeaderMap::new();
        let expected = Checksum::new(ChecksumAlgo::Crc32c, [0xe3, 0x06, 0x92, 0x83]);
        for (kind, value, full_object) in [
            (None, "4waSgw==", true),
            (Some("FULL_OBJECT"), "4waSgw==", true),
            (Some("COMPOSITE"), "4waSgw==", false),
            (Some("COMPOSITE"), "eOQlKw==-2", false),
            (Some("FUTURE_TYPE"), "4waSgw==", false),
            (Some("FULL_OBJECT"), "eOQlKw==-2", false),
            (None, "eOQlKw==-2", false),
            (None, "a", false),
            (None, "YWJj", false),
        ] {
            headers.clear();
            if let Some(kind) = kind {
                headers.insert(X_AMZ_CHECKSUM_TYPE, HeaderValue::from_static(kind));
            }
            headers.insert("x-amz-checksum-crc32c", HeaderValue::from_static(value));
            assert_eq!(
                parse_s3_read_checksum(&headers),
                full_object.then(|| expected.clone())
            );
        }

        headers.clear();
        headers.insert("x-amz-checksum-crc32", HeaderValue::from_static("4waSgw=="));
        assert_eq!(parse_s3_read_checksum(&headers), None);
    }
}
