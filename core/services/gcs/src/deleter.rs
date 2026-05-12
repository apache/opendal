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

use std::sync::Arc;

use http::StatusCode;

use super::core::*;
use super::error::parse_error;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

pub struct GcsDeleter {
    core: Arc<GcsCore>,
}

impl GcsDeleter {
    pub fn new(core: Arc<GcsCore>) -> Self {
        Self { core }
    }
}

impl oio::BatchDelete for GcsDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.gcs_delete_object(&path).await?;

        // deleting not existing objects is ok
        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(parse_error(resp))
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let paths: Vec<String> = batch.into_iter().map(|(p, _)| p).collect();
        let resp = self.core.gcs_delete_objects(paths.clone()).await?;

        parse_batch_delete_response(paths, resp)
    }
}

fn parse_batch_delete_response(
    paths: Vec<String>,
    resp: http::Response<Buffer>,
) -> Result<BatchDeleteResult> {
    let status = resp.status();

    // If the overall request isn't formatted correctly and Cloud Storage is unable to parse it into sub-requests, you receive a 400 error.
    // Otherwise, Cloud Storage returns a 200 status code, even if some or all of the sub-requests fail.
    if status != StatusCode::OK {
        return Err(parse_error(resp));
    }

    let boundary = parse_multipart_boundary(resp.headers())?.ok_or_else(|| {
        Error::new(
            ErrorKind::Unexpected,
            "gcs batch delete response content type is empty",
        )
    })?;
    let multipart: Multipart<MixedPart> = Multipart::new()
        .with_boundary(boundary)
        .parse(resp.into_body().to_bytes())?;
    let parts = multipart.into_parts();

    if paths.len() != parts.len() {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "invalid batch response, paths and response parts don't match",
        )
        .with_context("expected", paths.len())
        .with_context("actual", parts.len()));
    }

    let mut batched_result = BatchDeleteResult::default();

    for (i, part) in parts.into_iter().enumerate() {
        let resp = part.into_response();
        // TODO: maybe we can take it directly?
        let path = paths[i].clone();

        // deleting not existing objects is ok
        if resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND {
            batched_result.succeeded.push((path, OpDelete::default()));
        } else {
            batched_result
                .failed
                .push((path, OpDelete::default(), parse_error(resp)));
        }
    }

    // If no object is deleted, return directly.
    if batched_result.succeeded.is_empty() {
        let err = batched_result.failed.remove(0).2;
        return Err(err);
    }

    Ok(batched_result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const BOUNDARY: &str = "batch_test_boundary";

    fn batch_delete_response(part_count: usize) -> http::Response<Buffer> {
        let mut body = String::new();
        for _ in 0..part_count {
            body.push_str(&format!(
                "--{BOUNDARY}\r\n\
                 Content-Type: application/http\r\n\
                 \r\n\
                 HTTP/1.1 204 No Content\r\n\
                 \r\n"
            ));
        }
        body.push_str(&format!("--{BOUNDARY}--"));

        http::Response::builder()
            .status(StatusCode::OK)
            .header(
                http::header::CONTENT_TYPE,
                format!("multipart/mixed; boundary={BOUNDARY}"),
            )
            .body(Buffer::from(body))
            .expect("test response must be valid")
    }

    #[test]
    fn test_parse_batch_delete_response_rejects_more_parts_than_paths() {
        let err =
            match parse_batch_delete_response(vec!["path".to_string()], batch_delete_response(2)) {
                Ok(_) => panic!("parts count mismatch must fail"),
                Err(err) => err,
            };

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("paths and response parts"));
    }

    #[test]
    fn test_parse_batch_delete_response_rejects_fewer_parts_than_paths() {
        let err = match parse_batch_delete_response(
            vec!["path-0".to_string(), "path-1".to_string()],
            batch_delete_response(1),
        ) {
            Ok(_) => panic!("parts count mismatch must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.to_string().contains("paths and response parts"));
    }
}
