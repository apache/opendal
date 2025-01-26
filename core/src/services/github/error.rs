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

use bytes::Buf;
use http::Response;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

#[derive(Default, Debug, Deserialize)]
#[allow(dead_code)]
struct GithubError {
    error: GithubSubError,
}

#[derive(Default, Debug, Deserialize)]
#[allow(dead_code)]
struct GithubSubError {
    message: String,
    documentation_url: String,
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (kind, retryable) = match parts.status.as_u16() {
        401 | 403 => (ErrorKind::PermissionDenied, false),
        404 => (ErrorKind::NotFound, false),
        304 | 412 => (ErrorKind::ConditionNotMatch, false),
        // https://github.com/apache/opendal/issues/4146
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/423
        // We should retry it when we get 423 error.
        423 => (ErrorKind::RateLimited, true),
        // Service like Upyun could return 499 error with a message like:
        // Client Disconnect, we should retry it.
        499 => (ErrorKind::Unexpected, true),
        500 | 502 | 503 | 504 => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, _github_content_err) =
        serde_json::from_reader::<_, GithubError>(bs.clone().reader())
            .map(|github_content_err| (format!("{github_content_err:?}"), Some(github_content_err)))
            .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

#[cfg(test)]
mod test {
    use http::StatusCode;

    use super::*;

    #[tokio::test]
    async fn test_parse_error() {
        let err_res = vec![(
            r#"{
                "message": "Not Found",
                "documentation_url": "https://docs.github.com/rest/repos/contents#get-repository-content"
            }"#,
            ErrorKind::NotFound,
            StatusCode::NOT_FOUND,
        )];

        for res in err_res {
            let bs = bytes::Bytes::from(res.0);
            let body = Buffer::from(bs);
            let resp = Response::builder().status(res.2).body(body).unwrap();

            let err = parse_error(resp);

            assert_eq!(err.kind(), res.1);
        }
    }
}
