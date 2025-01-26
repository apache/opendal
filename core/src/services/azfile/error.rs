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

use bytes::Buf;
use http::Response;
use http::StatusCode;
use quick_xml::de;
use serde::Deserialize;

use crate::raw::*;
use crate::*;

/// AzfileError is the error returned by azure file service.
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct AzfileError {
    code: String,
    message: String,
    query_parameter_name: String,
    query_parameter_value: String,
    reason: String,
}

impl Debug for AzfileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("AzfileError");
        de.field("code", &self.code);
        // replace `\n` to ` ` for better reading.
        de.field("message", &self.message.replace('\n', " "));

        if !self.query_parameter_name.is_empty() {
            de.field("query_parameter_name", &self.query_parameter_name);
        }
        if !self.query_parameter_value.is_empty() {
            de.field("query_parameter_value", &self.query_parameter_value);
        }
        if !self.reason.is_empty() {
            de.field("reason", &self.reason);
        }

        de.finish()
    }
}

/// Parse error response into Error.
pub(super) fn parse_error(resp: Response<Buffer>) -> Error {
    let (parts, body) = resp.into_parts();
    let bs = body.to_bytes();

    let (kind, retryable) = match parts.status {
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

    let mut message = match de::from_reader::<_, AzfileError>(bs.clone().reader()) {
        Ok(azfile_err) => format!("{azfile_err:?}"),
        Err(_) => String::from_utf8_lossy(&bs).into_owned(),
    };

    // If there is no body here, fill with error code.
    if message.is_empty() {
        if let Some(v) = parts.headers.get("x-ms-error-code") {
            if let Ok(code) = v.to_str() {
                message = format!(
                    "{:?}",
                    AzfileError {
                        code: code.to_string(),
                        ..Default::default()
                    }
                )
            }
        }
    }

    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    if retryable {
        err = err.set_temporary();
    }

    err
}
