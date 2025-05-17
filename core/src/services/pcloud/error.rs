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
use std::fmt::Formatter;

use http::Response;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

/// PcloudError is the error returned by Pcloud service.
#[derive(Default, Deserialize)]
pub(super) struct PcloudError {
    pub result: u32,
    pub error: Option<String>,
}

impl Debug for PcloudError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PcloudError")
            .field("result", &self.result)
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();
    let message = String::from_utf8_lossy(&bs).into_owned();

    let mut err = Error::new(ErrorKind::Unexpected, message);

    err = with_error_response_context(err, parts);

    err
}

#[cfg(test)]
mod test {
    use http::StatusCode;

    use super::*;

    #[tokio::test]
    async fn test_parse_error() {
        let err_res = vec![(
            r#"<html>

                <head>
                    <title>Invalid link</title>
                </head>

                <body>This link was generated for another IP address. Try previous step again.</body>

                </html> "#,
            ErrorKind::Unexpected,
            StatusCode::GONE,
        )];

        for res in err_res {
            let bs = bytes::Bytes::from(res.0);
            let body = Buffer::from(bs);
            let resp = Response::builder().status(res.2).body(body).unwrap();

            let err = parse_error(resp);

            assert_eq!(err.kind(), res.1);
        }
    }
}
