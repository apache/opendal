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

use crate::http_util::ErrorResponse;
use crate::Error;
use crate::ErrorKind;

pub fn parse_error(er: ErrorResponse) -> Error {
    let (kind, retryable) = match er.status_code() {
        StatusCode::NOT_FOUND => (ErrorKind::ObjectNotFound, false),
        StatusCode::FORBIDDEN => (ErrorKind::ObjectPermissionDenied, false),
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => (ErrorKind::Unexpected, true),
        _ => (ErrorKind::Unexpected, false),
    };

    let mut err = Error::new(kind, &er.to_string());

    if retryable {
        err.set_temporary();
    }

    err
}

pub fn parse_xml_deserialize_error(e: quick_xml::DeError) -> Error {
    Error::new(ErrorKind::Unexpected, "deserialize xml").with_source(e)
}
