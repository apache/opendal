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

use http::Response;
use http::StatusCode;
use serde::Deserialize;
use serde_json::de;

use crate::raw::*;
use crate::*;

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsError {
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "Code")]
    code: usize,
    #[serde(rename = "Type")]
    ty: String,
}

/// Parse error response into io::Error.
///
/// > Status code 500 means that the function does exist, but IPFS was not
/// > able to fulfil the request because of an error.
/// > To know that reason, you have to look at the error message that is
/// > usually returned with the body of the response
/// > (if no error, check the daemon logs).
///
/// ref: https://docs.ipfs.tech/reference/kubo/rpc/#http-status-codes
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let ipfs_error = de::from_slice::<IpfsError>(&bs).ok();

    let (kind, retryable) = match parts.status {
        StatusCode::INTERNAL_SERVER_ERROR => {
            if let Some(ie) = &ipfs_error {
                match ie.message.as_str() {
                    "file does not exist" => (ErrorKind::NotFound, false),
                    _ => (ErrorKind::Unexpected, false),
                }
            } else {
                (ErrorKind::Unexpected, false)
            }
        }
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => {
            (ErrorKind::Unexpected, true)
        }
        _ => (ErrorKind::Unexpected, false),
    };

    let message = match ipfs_error {
        Some(ipfs_error) => format!("{ipfs_error:?}"),
        None => String::from_utf8_lossy(&bs).into_owned(),
    };

    let mut err = Error::new(kind, message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}
