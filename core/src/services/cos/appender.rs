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

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::header::CONTENT_LENGTH;
use http::StatusCode;

use super::core::*;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub const X_COS_OBJECT_TYPE: &str = "x-cos-object-type";
pub const X_COS_NEXT_APPEND_POSITION: &str = "x-cos-next-append-position";

pub struct CosAppender {
    core: Arc<CosCore>,

    op: OpAppend,
    path: String,

    position: Option<u64>,
}

impl CosAppender {
    pub fn new(core: Arc<CosCore>, path: &str, op: OpAppend) -> Self {
        Self {
            core,
            op,
            path: path.to_string(),
            position: None,
        }
    }
}

#[async_trait]
impl oio::Append for CosAppender {
    async fn append(&mut self, bs: Bytes) -> Result<()> {
        // If the position is not set, we need to get the current position.
        if self.position.is_none() {
            let resp = self.core.cos_head_object(&self.path, None, None).await?;

            let status = resp.status();
            match status {
                StatusCode::OK => {
                    let object_type = resp
                        .headers()
                        .get(X_COS_OBJECT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::Unexpected,
                                "missing x-cos-object-type, the object may not be appendable",
                            )
                        })?;

                    if object_type != "appendable" {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "object_type mismatch. the object may not be appendable",
                        ));
                    }

                    let position = resp
                        .headers()
                        .get(CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .ok_or_else(|| {
                            Error::new(
                                ErrorKind::Unexpected,
                                "missing content-length, the object may not be appendable",
                            )
                        })?;
                    self.position = Some(position);
                }
                StatusCode::NOT_FOUND => {
                    self.position = Some(0);
                }
                _ => {
                    return Err(parse_error(resp).await?);
                }
            }
        }

        let mut req = self.core.cos_append_object_request(
            &self.path,
            self.position.expect("position is not set"),
            bs.len(),
            &self.op,
            AsyncBody::Bytes(bs),
        )?;

        self.core.sign(&mut req).await?;

        let resp = self.core.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let position = resp
                    .headers()
                    .get(X_COS_NEXT_APPEND_POSITION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "append ok but missing x-cos-next-append-position, the object may not be appendable",
                        )
                    })?;
                self.position = Some(position);
                Ok(())
            }
            StatusCode::CONFLICT => {
                // The object is not appendable or the position is not match with the object's length.
                // If the position is not match, we could get the current position and retry.
                let position = resp
                    .headers()
                    .get(X_COS_NEXT_APPEND_POSITION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "append conflict. missing x-cos-next-append-position, the object may not be appendable",
                        )
                    })?;
                self.position = Some(position);

                // Then return the error to the caller, so the caller could retry.
                Err(Error::new(
                    ErrorKind::ConditionNotMatch,
                    "the position is not match with the object's length. position has been updated.",
                ))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
