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

/// the error response of alluxio
#[derive(Default, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct AlluxioError {
    status_code: String,
    message: String,
}

pub async fn parse_error(resp: Response<oio::Buffer>) -> Result<Error> {
    let (parts, mut body) = resp.into_parts();
    let bs = body.copy_to_bytes(body.remaining());

    let mut kind = match parts.status.as_u16() {
        500 => ErrorKind::Unexpected,
        _ => ErrorKind::Unexpected,
    };

    let (message, alluxio_err) = serde_json::from_reader::<_, AlluxioError>(bs.clone().reader())
        .map(|alluxio_err| (format!("{alluxio_err:?}"), Some(alluxio_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(alluxio_err) = alluxio_err {
        kind = match alluxio_err.status_code.as_str() {
            "ALREADY_EXISTS" => ErrorKind::AlreadyExists,
            "NOT_FOUND" => ErrorKind::NotFound,
            "INVALID_ARGUMENT" => ErrorKind::InvalidInput,
            _ => ErrorKind::Unexpected,
        }
    }

    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    Ok(err)
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use http::StatusCode;

    use super::*;

    /// Error response example is from https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
    #[tokio::test]
    async fn test_parse_error() {
        let err_res = vec![
            (
                r#"{"statusCode":"ALREADY_EXISTS","message":"The resource you requested already exist"}"#,
                ErrorKind::AlreadyExists,
            ),
            (
                r#"{"statusCode":"NOT_FOUND","message":"The resource you requested does not exist"}"#,
                ErrorKind::NotFound,
            ),
            (
                r#"{"statusCode":"INVALID_ARGUMENT","message":"The argument you provided is invalid"}"#,
                ErrorKind::InvalidInput,
            ),
            (
                r#"{"statusCode":"INTERNAL_SERVER_ERROR","message":"Internal server error"}"#,
                ErrorKind::Unexpected,
            ),
        ];

        for res in err_res {
            let bs = bytes::Bytes::from(res.0);
            let body = oio::Buffer::new(
                Box::new(oio::into_stream(stream::iter(vec![Ok(bs.clone())]))),
                None,
            );
            let resp = Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(body)
                .unwrap();

            let err = parse_error(resp).await;

            assert!(err.is_ok());
            assert_eq!(err.unwrap().kind(), res.1);
        }
    }
}
