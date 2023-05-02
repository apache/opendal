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
use serde_json::from_slice;

use crate::raw::*;
use crate::Error;
use crate::ErrorKind;
use crate::Result;

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
/// The error returned by Supabase
pub struct SupabaseError {
    status_code: String,
    error: String,
    message: String,
}

/// Parse the supabase error type to the OpenDAL error type
pub async fn parse_error(resp: Response<IncomingAsyncBody>) -> Result<Error> {
    let (parts, body) = resp.into_parts();
    let bs = body.bytes().await?;

    let (mut kind, mut retryable) = (ErrorKind::Unexpected, false);
    let (message, _) = from_slice::<SupabaseError>(&bs)
        .map(|sb_err| {
            (kind, retryable) = parse_supabase_error(&sb_err);
            (format!("{sb_err:?}"), Some(sb_err))
        })
        .unwrap_or_else(|_| (String::from_utf8_lossy(&bs).into_owned(), None));

    let mut err = Error::new(kind, &message).with_context("response", format!("{parts:?}"));

    if retryable {
        err = err.set_temporary();
    }

    Ok(err)
}

// Return the error kind and whether it is retryable
fn parse_supabase_error(err: &SupabaseError) -> (ErrorKind, bool) {
    let code = err.status_code.parse::<u16>().unwrap();
    if code == StatusCode::CONFLICT.as_u16() && err.error == "Duplicate" {
        (ErrorKind::AlreadyExists, false)
    } else if code == StatusCode::NOT_FOUND.as_u16() {
        (ErrorKind::NotFound, false)
    } else if code == StatusCode::FORBIDDEN.as_u16() {
        (ErrorKind::PermissionDenied, false)
    } else if code == StatusCode::PRECONDITION_FAILED.as_u16() {
        (ErrorKind::ConditionNotMatch, false)
    } else {
        (ErrorKind::Unexpected, false)
    }
}
