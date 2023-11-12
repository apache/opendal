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

pub async fn parse_error(resp: Response<IncomingAsyncBody>) -> Result<Error> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    let mut kind = match parts.status.as_u16() {
        500 => ErrorKind::Unexpected,
        _ => ErrorKind::Unexpected,
    };

    let (message, alluxio_err) = serde_json::from_reader::<_, AlluxioError>(bs.clone().reader())
        .map(|alluxio_err| (format!("{alluxio_err:?}"), Some(alluxio_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(alluxio_err) = alluxio_err {
        kind = match alluxio_err.status_code.as_str() {
            "AlreadyExists" => ErrorKind::AlreadyExists,
            "NotFound" => ErrorKind::NotFound,
            "InvalidArgument" => ErrorKind::InvalidInput,
            _ => ErrorKind::Unexpected,
        }
    }

    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    Ok(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Error response example is from https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
    #[test]
    fn test_parse_error() {
        let bs = bytes::Bytes::from(
            r#"
            {
                "statusCode": "AlreadyExists",
                "message": "The resource you requested already exist"
            }
"#,
        );

        let out: AlluxioError = serde_json::from_reader(bs.reader()).expect("must success");
        println!("{out:?}");

        assert_eq!(out.status_code, "AlreadyExists");
        assert_eq!(out.message, "The resource you requested already exist");
    }
}
