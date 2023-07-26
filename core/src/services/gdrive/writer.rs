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
use http::StatusCode;

use super::core::GdriveCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct GdriveWriter {
    core: Arc<GdriveCore>,

    op: OpWrite,
    path: String,

    target: Option<String>,
    position: u64,
    size: Option<u64>,
}

impl GdriveWriter {
    pub fn new(core: Arc<GdriveCore>, op: OpWrite, path: String) -> Self {
        GdriveWriter {
            core,
            op,
            path,
            position: 0,
            size: None,
            target: None,
        }
    }

    pub async fn initial_upload(&mut self) -> Result<()> {
        let resp = self
            .core
            .gdrive_upload_initial_request(&self.path, None)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let headers = resp.headers();

                match headers.get("location") {
                    Some(location) => {
                        self.target = Some(location.to_str().map_err(|_| {
                            Error::new(
                                ErrorKind::Unexpected,
                                "initial upload failed: location header parse failed",
                            )
                        })?.to_string());
                        Ok(())
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::Unexpected,
                            "initial upload failed: location header not found",
                        ))
                    }
                }
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    pub async fn write_part(&mut self, size: u64, part: AsyncBody) -> Result<()> {
        if let Some(target) = &self.target {
            let resp = self
                .core
                .gdrive_upload_part_request(target, size, self.position, self.size, part)
                .await?;

            println!("size: {}", size);

            let status = resp.status();

            match status {
                StatusCode::PERMANENT_REDIRECT => {
                    self.position = self.position + size;

                    resp.into_body().consume().await?;
                    Ok(())
                }
                _ => Err(parse_error(resp).await?),
            }
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                "write part failed: upload location not found",
            ))
        }
    }

    pub async fn finish_upload(&self) -> Result<()> {
        if let Some(target) = &self.target {
            println!("final position: {}", self.position + 1);
            println!("final size: {}", self.size.unwrap_or(self.position + 1));

            let resp = self
                .core
                .gdrive_finish_upload_request(
                    target,
                    if let Some(size) = self.size {
                        if (self.position + 1) < size {
                            return Err(Error::new(
                                ErrorKind::Unexpected,
                                "finish upload failed: upload size mismatch",
                            ));
                        }
                        size
                    } else {
                        self.position + 1
                    },
                )
                .await?;

            let status = resp.status();

            match status {
                StatusCode::OK | StatusCode::CREATED => {
                    resp.into_body().consume().await?;
                    Ok(())
                }
                _ => Err(parse_error(resp).await?),
            }
        } else {
            Ok(())
        }
    }

    pub async fn abort_upload(&self) -> Result<()> {
        if let Some(target) = &self.target {
            let resp = self.core.gdrive_cancel_upload_request(target).await?;

            let status = resp.status();

            match status {
                StatusCode::OK => {
                    resp.into_body().consume().await?;
                    Ok(())
                }
                _ => Err(parse_error(resp).await?),
            }
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl oio::Write for GdriveWriter {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.target.is_none() {
            self.initial_upload().await?;
        }

        self.write_part(bs.len() as u64, AsyncBody::Bytes(bs)).await
    }

    async fn sink(&mut self, size: u64, s: oio::Streamer) -> Result<()> {
        if self.target.is_none() {
            self.initial_upload().await?;
        }

        self.write_part(size, AsyncBody::Stream(s)).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.abort_upload().await
    }

    async fn close(&mut self) -> Result<()> {
        self.finish_upload().await
    }
}
