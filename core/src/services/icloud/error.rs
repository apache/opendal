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

use crate::raw::{with_error_response_context, IncomingAsyncBody};
use crate::types::ErrorKind;
use crate::Result;
use crate::*;
use bytes::Buf;
use http::Response;
use serde::Deserialize;

#[derive(Default, Debug, Deserialize)]
struct iCloudError {
    status_code: String,
    message: String,
}

pub async fn parse_error(resp: Response<IncomingAsyncBody>) -> Result<Error> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    let mut kind = match parts.status.as_u16() {
        //status:421 Misdirected Request
        421 | 450 | 500 => ErrorKind::NotFound,
        401 => ErrorKind::Unexpected,
        _ => ErrorKind::Unexpected,
    };

    let (message, iCloud_err) = serde_json::from_reader::<_, iCloudError>(bs.clone().reader())
        .map(|iCloud_err| (format!("{iCloud_err:?}"), Some(iCloud_err)))
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    if let Some(iCloud_err) = iCloud_err {
        kind = match iCloud_err.status_code.as_str() {
            "NOT_FOUND" => ErrorKind::NotFound,
            "PERMISSION_DENIED" => ErrorKind::PermissionDenied,
            _ => ErrorKind::Unexpected,
        }
    }

    let mut err = Error::new(kind, &message);

    err = with_error_response_context(err, parts);

    Ok(err)
}
