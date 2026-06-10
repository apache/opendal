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

use http::StatusCode;

use opendal_core::raw::*;
use opendal_core::*;

pub(super) fn parse_error(parts: http::response::Parts) -> Error {
    // HF sets x-error-message on every error response with a short human-readable
    // description. Using the header avoids reading the response body, which can be
    // a large HTML error page (e.g. 52 KB on 404s from the /resolve/ endpoint).
    let message = parts
        .headers
        .get("x-error-message")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown error")
        .to_string();

    // HF git-style commit APIs reject stale branch snapshots with 412.
    // Treat this specific conflict as temporary so RetryLayer can replay
    // the whole write/delete close sequence on a fresh branch head.
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
    use http::Response;
    use http::StatusCode;

    use super::*;

    #[test]
    fn test_parse_error_branch_update_conflict_is_temporary() {
        let (parts, _) = Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .header(
                "x-error-message",
                "The branch was updated since you opened this page. Please refresh and try again.",
            )
            .body(())
            .unwrap()
            .into_parts();

        let err = parse_error(parts);

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(err.is_temporary());
    }

    #[test]
    fn test_parse_error_other_precondition_failed_is_not_temporary() {
        let (parts, _) = Response::builder()
            .status(StatusCode::PRECONDITION_FAILED)
            .header("x-error-message", "etag mismatch")
            .body(())
            .unwrap()
            .into_parts();

        let err = parse_error(parts);

        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
        assert!(!err.is_temporary());
    }
}
