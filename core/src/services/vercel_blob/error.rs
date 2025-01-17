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
use quick_xml::de;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

/// VercelBlobError is the error returned by VercelBlob service.
#[derive(Default, Debug, Deserialize)]
#[serde(default)]
struct VercelBlobError {
    error: VercelBlobErrorDetail,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default)]
struct VercelBlobErrorDetail {
    code: String,
    message: Option<String>,
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (kind, retryable) = match parts.status.as_u16() {
        403 => (ErrorKind::PermissionDenied, false),
        404 => (ErrorKind::NotFound, false),
        500 | 502 | 503 | 504 => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, _vercel_blob_err) = de::from_reader::<_, VercelBlobError>(bs.clone().reader())
        .map(|vercel_blob_err| (format!("{vercel_blob_err:?}"), Some(vercel_blob_err)))
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
                    "error": {
                        "code": "forbidden",
                        "message": "Invalid token"
                    }
                }"#,
            ErrorKind::PermissionDenied,
            StatusCode::FORBIDDEN,
        )];

        for res in err_res {
            let body = Buffer::from(res.0.as_bytes().to_vec());
            let resp = Response::builder().status(res.2).body(body).unwrap();

            let err = parse_error(resp);

            assert_eq!(err.kind(), res.1);
        }

        let bs = bytes::Bytes::from(
            r#"{
                "error": {
                    "code": "forbidden",
                    "message": "Invalid token"
                }
            }"#,
        );

        let out: VercelBlobError = serde_json::from_reader(bs.reader()).expect("must success");
        println!("{out:?}");

        assert_eq!(out.error.code, "forbidden");
        assert_eq!(out.error.message, Some("Invalid token".to_string()));
    }
}
