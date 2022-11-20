// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use http::StatusCode;
use serde::Deserialize;

use crate::http_util::ErrorResponse;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

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
pub fn parse_error(er: ErrorResponse) -> Error {
    let (kind, retryable) = match er.status_code() {
        StatusCode::INTERNAL_SERVER_ERROR => {
            let ie: Result<IpfsError> =
                serde_json::from_slice(er.body()).map_err(parse_json_deserialize_error);
            match ie {
                Ok(ie) => match ie.message.as_str() {
                    "file does not exist" => (ErrorKind::ObjectNotFound, false),
                    _ => (ErrorKind::Unexpected, false),
                },
                Err(e) => return e,
            }
        }
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT => {
            (ErrorKind::Unexpected, true)
        }
        _ => (ErrorKind::Unexpected, false),
    };

    let mut err = Error::new(kind, &er.to_string());

    if retryable {
        err = err.set_temporary();
    }

    err
}

pub fn parse_json_deserialize_error(e: serde_json::Error) -> Error {
    Error::new(ErrorKind::Unexpected, "deserialize json").with_source(e)
}
