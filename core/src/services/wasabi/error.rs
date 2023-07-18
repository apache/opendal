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
use http::StatusCode;
use quick_xml::de;
use serde::Deserialize;

use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

/// WasabiError is the error returned by wasabi service.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct WasabiError {
    code: String,
    message: String,
    resource: String,
    request_id: String,
}

/// Parse error response into Error.
pub async fn parse_error(resp: Response<IncomingAsyncBody>) -> Result<Error> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    let (mut kind, mut retryable) = match parts.status {
        StatusCode::NOT_FOUND => (ErrorKind::NotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::PermissionDenied, false),
        StatusCode::PRECONDITION_FAILED | StatusCode::NOT_MODIFIED => {
            (ErrorKind::ConditionNotMatch, false)
        }
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let (message, wasabi_err) = de::from_reader::<_, WasabiError>(bs.clone().reader())
        .map(|err| (format!("{err:?}"), Some(err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(wasabi_err) = wasabi_err {
        (kind, retryable) = parse_wasabi_error_code(&wasabi_err.code).unwrap_or((kind, retryable));
    }

    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    Ok(err)
}

/// Returns the `Error kind` of this code and whether the error is retryable.
/// All possible error code: <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList>
pub fn parse_wasabi_error_code(code: &str) -> Option<(ErrorKind, bool)> {
    match code {
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

        let out: WasabiError = de::from_reader(bs.reader()).expect("must success");
        println!("{out:?}");

        assert_eq!(out.code, "NoSuchKey");
        assert_eq!(out.message, "The resource you requested does not exist");
        assert_eq!(out.resource, "/mybucket/myfoto.jpg");
        assert_eq!(out.request_id, "4442587FB7D0A2F9");
    }
}
