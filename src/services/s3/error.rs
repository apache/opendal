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

use aws_sdk_s3::error::GetObjectError;
use aws_sdk_s3::error::GetObjectErrorKind;
use aws_sdk_s3::error::HeadObjectError;
use aws_sdk_s3::error::HeadObjectErrorKind;
use aws_smithy_http::result::SdkError;

use crate::error::Error;
use crate::error::Kind;

pub fn parse_get_object_error(
    err: SdkError<GetObjectError>,
    op: &'static str,
    path: &str,
) -> Error {
    if let SdkError::ServiceError { err, .. } = err {
        match err.kind {
            GetObjectErrorKind::NoSuchKey(_) => Error::Object {
                kind: Kind::ObjectNotExist,
                op,
                path: path.to_string(),
                source: anyhow::Error::from(err),
            },
            _ => Error::Object {
                kind: Kind::Unexpected,
                op,
                path: path.to_string(),
                source: anyhow::Error::from(err),
            },
        }
    } else {
        Error::Object {
            kind: Kind::Unexpected,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        }
    }
}

pub fn parse_head_object_error(
    err: SdkError<HeadObjectError>,
    op: &'static str,
    path: &str,
) -> Error {
    if let SdkError::ServiceError { err, .. } = err {
        match err.kind {
            HeadObjectErrorKind::NotFound(_) => Error::Object {
                kind: Kind::ObjectNotExist,
                op,
                path: path.to_string(),
                source: anyhow::Error::from(err),
            },
            _ => Error::Object {
                kind: Kind::Unexpected,
                op,
                path: path.to_string(),
                source: anyhow::Error::from(err),
            },
        }
    } else {
        Error::Object {
            kind: Kind::Unexpected,
            op,
            path: path.to_string(),
            source: anyhow::Error::from(err),
        }
    }
}

// parse_unexpect_error is used to parse SdkError into unexpected.
pub fn parse_unexpect_error<E: 'static + Send + Sync + std::error::Error>(
    err: SdkError<E>,
    op: &'static str,
    path: &str,
) -> Error {
    Error::Object {
        kind: Kind::Unexpected,
        op,
        path: path.to_string(),
        source: anyhow::Error::from(err),
    }
}
