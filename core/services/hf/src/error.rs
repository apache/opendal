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

use std::fmt::Debug;

use http::Response;
use http::StatusCode;
use serde::Deserialize;

use opendal_core::raw::*;
use opendal_core::*;

#[derive(Default, Deserialize)]
struct HfError {
    error: String,
}

impl Debug for HfError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfError")
            .field("message", &self.error.replace('\n', " "))
            .finish()
    }
}

pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let message = match serde_json::from_slice::<HfError>(&bs) {
        Ok(hf_error) => hf_error.error,
        Err(_) => String::from_utf8_lossy(&bs).into_owned(),
    };

    let branch_updated_conflict = parts.status == StatusCode::PRECONDITION_FAILED
        && message
            .to_ascii_lowercase()
            .contains("branch was updated since you opened this page");

    let (kind, retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::PRECONDITION_FAILED => (ErrorKind::ConditionNotMatch, branch_updated_conflict),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

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

    #[test]
    fn test_parse_error() -> Result<()> {
        let resp = r#"
            {
                "error": "Invalid username or password."
            }
            "#;
        let decoded_response = serde_json::from_slice::<HfError>(resp.as_bytes())
            .map_err(new_json_deserialize_error)?;

        assert_eq!(decoded_response.error, "Invalid username or password.");

        Ok(())
    }

    #[test]
    fn test_parse_error_branch_update_conflict_is_temporary() {
        let body = Buffer::from(bytes::Bytes::from(
            r#"{"error":"The branch was updated since you opened this page. Please refresh and try again."}"#,
        ));
        let resp = Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .body(body)
            .unwrap();

        let err = parse_error(resp);

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(err.is_temporary());
    }

    #[test]
    fn test_parse_error_other_precondition_failed_is_not_temporary() {
        let body = Buffer::from(bytes::Bytes::from(r#"{"error":"etag mismatch"}"#));
        let resp = Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .body(body)
            .unwrap();

        let err = parse_error(resp);

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(!err.is_temporary());
    }
}
