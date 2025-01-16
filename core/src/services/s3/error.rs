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
use http::response::Parts;
use http::Response;
use quick_xml::de;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

/// S3Error is the error returned by s3 service.
#[derive(Default, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, rename_all = "PascalCase")]
pub(crate) struct S3Error {
    pub code: String,
    pub message: String,
    pub resource: String,
    pub request_id: String,
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (mut kind, mut retryable) = match parts.status.as_u16() {
        403 => (ErrorKind::PermissionDenied, false),
        404 => (ErrorKind::NotFound, false),
        304 | 412 => (ErrorKind::ConditionNotMatch, false),
        // Service like R2 could return 499 error with a message like:
        // Client Disconnect, we should retry it.
        499 => (ErrorKind::Unexpected, true),
        500 | 502 | 503 | 504 => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let body_content = bs.chunk();
    let (message, s3_err) = de::from_reader::<_, S3Error>(body_content.reader())
        .map(|s3_err| (format!("{s3_err:?}"), Some(s3_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(s3_err) = s3_err {
        (kind, retryable) = parse_s3_error_code(s3_err.code.as_str()).unwrap_or((kind, retryable));
    }

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

/// Util function to build [`Error`] from a [`S3Error`] object.
pub(crate) fn from_s3_error(s3_error: S3Error, parts: Parts) -> Error {
    let (kind, retryable) =
        parse_s3_error_code(s3_error.code.as_str()).unwrap_or((ErrorKind::Unexpected, false));
    let mut err = Error::new(kind, format!("{s3_error:?}"));

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}

/// Returns the `Error kind` of this code and whether the error is retryable.
/// All possible error code: <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList>
pub fn parse_s3_error_code(code: &str) -> Option<(ErrorKind, bool)> {
    match code {
        // > The specified bucket does not exist.
        //
        // Although the status code is 404, NoSuchBucket is
        // a config invalid error, and it's not retryable from OpenDAL.
        "NoSuchBucket" => Some((ErrorKind::ConfigInvalid, false)),
        // > Your socket connection to the server was not read from
        // > or written to within the timeout period."
        //
        // It's Ok for us to retry it again.
        "RequestTimeout" => Some((ErrorKind::Unexpected, true)),
        // > An internal error occurred. Try again.
        "InternalError" => Some((ErrorKind::Unexpected, true)),
        // > A conflicting conditional operation is currently in progress
        // > against this resource. Try again.
        "OperationAborted" => Some((ErrorKind::Unexpected, true)),
        // > Please reduce your request rate.
        //
        // It's Ok to retry since later on the request rate may get reduced.
        "SlowDown" => Some((ErrorKind::RateLimited, true)),
        // > Service is unable to handle request.
        //
        // ServiceUnavailable is considered a retryable error because it typically
        // indicates a temporary issue with the service or server, such as high load,
        // maintenance, or an internal problem.
        "ServiceUnavailable" => Some((ErrorKind::Unexpected, true)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Error response example is from https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
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

        let out: S3Error = de::from_reader(bs.reader()).expect("must success");
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
<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Location>http://Example-Bucket.s3.ap-southeast-1.amazonaws.com/Example-Object</Location>
  <Bucket>Example-Bucket</Bucket>
  <Key>Example-Object</Key>
  <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
</CompleteMultipartUploadResult>
"#,
        );

        let out: S3Error = de::from_reader(bs.reader()).expect("must success");
        assert_eq!(out, S3Error::default());
    }
}
