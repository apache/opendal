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

use crate::*;

#[derive(Default, Debug, Deserialize)]
struct AliyunDriveError {
    code: String,
    message: String,
}

pub(super) fn parse_error(res: Response<Buffer>) -> Error {
    let (parts, body) = res.into_parts();
    let bs = body.to_bytes();
    let (code, message) = serde_json::from_reader::<_, AliyunDriveError>(bs.clone().reader())
        .map(|err| (Some(err.code), err.message))
        .unwrap_or((None, String::from_utf8_lossy(&bs).into_owned()));
    let (kind, retryable) = match parts.status.as_u16() {
        403 => (ErrorKind::PermissionDenied, false),
        400 => match code {
            Some(code) if code == "NotFound.File" => (ErrorKind::NotFound, false),
            Some(code) if code == "AlreadyExist.File" => (ErrorKind::AlreadyExists, false),
            Some(code) if code == "PreHashMatched" => (ErrorKind::IsSameFile, false),
            _ => (ErrorKind::Unexpected, false),
        },
        409 => (ErrorKind::AlreadyExists, false),
        429 => match code {
            Some(code) if code == "TooManyRequests" => (ErrorKind::RateLimited, true),
            _ => (ErrorKind::Unexpected, false),
        },
        _ => (ErrorKind::Unexpected, false),
    };
    let mut err = Error::new(kind, message);
    if retryable {
        err = err.set_temporary();
    }
    err
}
