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
use azure_core::HttpError;
use crate::error::Kind;
use crate::error::Error;
use std::error::Error as StdError;


pub fn parse_get_object_error(
    err: Box<dyn StdError + Send + Sync>,
    op: &'static str,
    path: &str,
) -> Error {
    if let Some(err) = err.downcast_ref::<HttpError>() {
        if matches!(
            err,
            HttpError::StatusCode {
                status: StatusCode::NOT_FOUND,
                ..
            }
        ) {
            return Error::Object {
                kind: Kind::ObjectNotExist,
                op,
                path: path.to_string(),
                source: anyhow::Error::from(err),
            }
        }
        
    }
    return Error::Object {
        kind: Kind::Unexpected,
        op,
        path: path.to_string(),
        source: todo!(),
    }    

}

pub fn parse_head_object_error(
    err: Box<dyn StdError + Send + Sync>,
    op: &'static str,
    path: &str,
) -> Error {
    if let Some(err) = err.downcast_ref::<HttpError>() {
        if matches!(
            err,
            HttpError::StatusCode {
                status: StatusCode::NOT_FOUND,
                ..
            }
        ) {
            return Error::Object {
                kind: Kind::ObjectNotExist,
                op,
                path: path.to_string(),
                source: anyhow::Error::from(err),
            }
        }
    }
    Error::Object {
        kind: Kind::Unexpected,
        op,
        path: path.to_string(),
        source: todo!(),
    }   
}

// parse_unexpect_error is used to parse SdkError into unexpected.
pub fn parse_unexpect_error<E: 'static + Send + Sync + std::error::Error>(
    err: Box<dyn StdError + Send + Sync>,
    op: &'static str,
    path: &str,
) -> Error {
    Error::Object {
        kind: Kind::Unexpected,
        op,
        path: path.to_string(),
        source: todo!(),
    }
}
