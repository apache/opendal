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

use super::core::*;
use super::error::parse_error;
use crate::raw::oio::BatchDeleteResult;
use crate::raw::*;
use crate::*;
use http::StatusCode;
use std::sync::Arc;

pub struct AzblobDeleter {
    core: Arc<AzblobCore>,
}

impl AzblobDeleter {
    pub fn new(core: Arc<AzblobCore>) -> Self {
        Self { core }
    }
}

impl oio::BatchDelete for AzblobDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let resp = self.core.azblob_delete_blob(&path).await?;

        let status = resp.status();

        match status {
            StatusCode::ACCEPTED | StatusCode::NOT_FOUND => Ok(()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        // TODO: Add remove version support.
        let paths = batch.into_iter().map(|(p, _)| p).collect::<Vec<_>>();

        // construct and complete batch request
        let resp = self.core.azblob_batch_delete(&paths).await?;

        // check response status
        if resp.status() != StatusCode::ACCEPTED {
            return Err(parse_error(resp));
        }

        // get boundary from response header
        let boundary = parse_multipart_boundary(resp.headers())?
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "invalid response: no boundary provided in header",
                )
            })?
            .to_string();

        let bs = resp.into_body().to_bytes();
        let multipart: Multipart<MixedPart> =
            Multipart::new().with_boundary(&boundary).parse(bs)?;
        let parts = multipart.into_parts();

        if paths.len() != parts.len() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "invalid batch response, paths and response parts don't match",
            ));
        }

        let mut batched_result = BatchDeleteResult::default();

        for (i, part) in parts.into_iter().enumerate() {
            let resp = part.into_response();
            let path = paths[i].clone();

            // deleting not existing objects is ok
            if resp.status() == StatusCode::ACCEPTED || resp.status() == StatusCode::NOT_FOUND {
                batched_result.succeeded.push((path, OpDelete::default()));
            } else {
                batched_result
                    .failed
                    .push((path, OpDelete::default(), parse_error(resp)));
            }
        }

        // If no object is deleted, return directly.
        if batched_result.succeeded.is_empty() {
            let err = batched_result.failed.remove(0).2;
            return Err(err);
        }

        Ok(batched_result)
    }
}
