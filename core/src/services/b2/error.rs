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

/// the error response of b2
#[derive(Default, Debug, Deserialize)]
#[allow(dead_code)]
struct B2Error {
    status: u32,
    code: String,
    message: String,
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (mut kind, mut retryable) = match parts.status.as_u16() {
        403 => (ErrorKind::PermissionDenied, false),
        404 => (ErrorKind::NotFound, false),
        304 | 412 => (ErrorKind::ConditionNotMatch, false),
        // Service b2 could return 403, show the authorization error
        401 => (ErrorKind::PermissionDenied, true),
        429 => (ErrorKind::RateLimited, true),
        500 | 502 | 503 | 504 => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, b2_err) = serde_json::from_reader::<_, B2Error>(bs.clone().reader())
        .map(|b2_err| (format!("{b2_err:?}"), Some(b2_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(b2_err) = b2_err {
        (kind, retryable) = parse_b2_error_code(b2_err.code.as_str()).unwrap_or((kind, retryable));
    };

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

/// Returns the `Error kind` of this code and whether the error is retryable.
pub(crate) fn parse_b2_error_code(code: &str) -> Option<(ErrorKind, bool)> {
    match code {
        "already_hidden" => Some((ErrorKind::AlreadyExists, false)),
        "no_such_file" => Some((ErrorKind::NotFound, false)),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use http::StatusCode;

    use super::*;

    #[test]
    fn test_parse_b2_error_code() {
        let code = "already_hidden";
        assert_eq!(
            parse_b2_error_code(code),
            Some((crate::ErrorKind::AlreadyExists, false))
        );

        let code = "no_such_file";
        assert_eq!(
            parse_b2_error_code(code),
            Some((crate::ErrorKind::NotFound, false))
        );

        let code = "not_found";
        assert_eq!(parse_b2_error_code(code), None);
    }

    #[tokio::test]
    async fn test_parse_error() {
        let err_res = vec![
            (
                r#"{"status": 403, "code": "access_denied", "message":"The provided customer-managed encryption key is wrong."}"#,
                ErrorKind::PermissionDenied,
                StatusCode::FORBIDDEN,
            ),
            (
                r#"{"status": 404, "code": "not_found", "message":"File is not in B2 Cloud Storage."}"#,
                ErrorKind::NotFound,
                StatusCode::NOT_FOUND,
            ),
            (
                r#"{"status": 401, "code": "bad_auth_token", "message":"The auth token used is not valid. Call b2_authorize_account again to either get a new one, or an error message describing the problem."}"#,
                ErrorKind::PermissionDenied,
                StatusCode::UNAUTHORIZED,
            ),
        ];

        for res in err_res {
            let bs = bytes::Bytes::from(res.0);
            let body = Buffer::from(bs);
            let resp = Response::builder().status(res.2).body(body).unwrap();

            let err = parse_error(resp);

            assert_eq!(err.kind(), res.1);
        }
    }
}
