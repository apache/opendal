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
use http::{Response, StatusCode};
use quick_xml::de;
use serde::Deserialize;

use opendal_core::raw::*;
use opendal_core::*;

/// TosError is the error returned by TOS service.
#[derive(Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "PascalCase")]
pub(crate) struct TosError {
    pub code: String,
    pub message: String,
    pub resource: String,
    pub request_id: String,
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (mut kind, mut retryable) = match parts.status {
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::NOT_MODIFIED | StatusCode::PRECONDITION_FAILED => {
            (ErrorKind::ConditionNotMatch, false)
        }
        StatusCode::CONFLICT => (ErrorKind::ConditionNotMatch, true),
        StatusCode::TOO_MANY_REQUESTS => (ErrorKind::RateLimited, true),
        code if code.is_server_error() => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let body_content = bs.chunk();
    let (message, tos_err) = de::from_reader::<_, TosError>(body_content.reader())
        .map(|tos_err| (format!("{tos_err:?}"), Some(tos_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(tos_err) = tos_err {
        (kind, retryable) =
            parse_tos_error_code(tos_err.code.as_str()).unwrap_or((kind, retryable));
    }

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

/// Returns the `Error kind` of this code and whether the error is retryable.
/// TOS error codes: https://www.volcengine.com/docs/6349/74874
pub fn parse_tos_error_code(code: &str) -> Option<(ErrorKind, bool)> {
    match code {
        "NoSuchBucket" => Some((ErrorKind::ConfigInvalid, false)),
        "NoSuchKey" => Some((ErrorKind::NotFound, false)),
        "RequestTimeout" => Some((ErrorKind::Unexpected, true)),
        "InternalError" => Some((ErrorKind::Unexpected, true)),
        "OperationAborted" => Some((ErrorKind::Unexpected, true)),
        "SlowDown" => Some((ErrorKind::RateLimited, true)),
        "ServiceUnavailable" => Some((ErrorKind::Unexpected, true)),
        "TooManyRequests" => Some((ErrorKind::RateLimited, true)),
        "ExceedAccountQPSLimit" | "ExceedAccountRateLimit" => Some((ErrorKind::RateLimited, true)),
        "ExceedBucketQPSLimit" | "ExceedBucketRateLimit" => Some((ErrorKind::RateLimited, true)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error() {
        let bs = bytes::Bytes::from(
            r#"
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The resource you requested does not exist</Message>
  <Resource>/mybucket/myfoto.jpg</Resource>
  <RequestId>4442587FB7D0A2F9</RequestId>
</Error>
"#,
        );

        let out: TosError = de::from_reader(bs.reader()).expect("must success");
        println!("{out:?}");

        assert_eq!(out.code, "NoSuchKey");
        assert_eq!(out.message, "The resource you requested does not exist");
        assert_eq!(out.resource, "/mybucket/myfoto.jpg");
        assert_eq!(out.request_id, "4442587FB7D0A2F9");
    }

    #[test]
    fn test_parse_error_from_unrelated_input() {
        let bs = bytes::Bytes::from(
            r#"
<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult xmlns="http://tos.apache.org/doc/2024-01-01/">
  <Bucket>example-bucket</Bucket>
  <Key>example-object</Key>
  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
</CompleteMultipartUploadResult>
"#,
        );

        let out: TosError = de::from_reader(bs.reader()).expect("must success");
        assert_eq!(out, TosError::default());
    }
}
