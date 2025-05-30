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

/// the error response of seafile
#[derive(Default, Debug, Deserialize)]
#[allow(dead_code)]
struct SeafileError {
    error_msg: String,
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (kind, _retryable) = match parts.status.as_u16() {
        403 => (ErrorKind::PermissionDenied, false),
        404 => (ErrorKind::NotFound, false),
        520 => (ErrorKind::Unexpected, false),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, _seafile_err) = serde_json::from_reader::<_, SeafileError>(bs.clone().reader())
        .map(|seafile_err| (format!("{seafile_err:?}"), Some(seafile_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    err
}

#[cfg(test)]
mod test {
    use http::StatusCode;

    use super::*;

    #[tokio::test]
    async fn test_parse_error() {
        let err_res = vec![
            (
                r#"{"error_msg": "Permission denied"}"#,
                ErrorKind::PermissionDenied,
                StatusCode::FORBIDDEN,
            ),
            (
                r#"{"error_msg": "Folder /e982e75a-fead-487c-9f41-63094d9bf0de/a9d867b9-778d-4612-b674-47e674c14c28/ not found."}"#,
                ErrorKind::NotFound,
                StatusCode::NOT_FOUND,
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
