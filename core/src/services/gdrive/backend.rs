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
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use super::core::GdriveCore;
use super::error::parse_error;
use super::writer::GdriveWriter;
use crate::raw::*;
use crate::types::Result;
use crate::Capability;
use crate::Error;
use crate::ErrorKind;

#[derive(Clone, Debug)]
pub struct GdriveBackend {
    core: Arc<GdriveCore>,
}

impl GdriveBackend {
    pub(crate) fn new(root: String, access_token: String, http_client: HttpClient) -> Self {
        GdriveBackend {
            core: Arc::new(GdriveCore {
                root,
                access_token,
                client: http_client,
                path_cache: Arc::default(),
            }),
        }
    }
}

#[async_trait]
impl Accessor for GdriveBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = GdriveWriter;
    type BlockingWriter = ();
    type Appender = ();
    type Pager = ();
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut ma = AccessorInfo::default();
        ma.set_scheme(crate::Scheme::Gdrive)
            .set_root(&self.core.root)
            .set_capability(Capability {
                read: true,
                write: true,
                delete: true,
                ..Default::default()
            });

        ma
    }

    async fn read(&self, path: &str, _args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.gdrive_get(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            GdriveWriter::new(self.core.clone(), args, String::from(path)),
        ))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.core.gdrive_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::NO_CONTENT => Ok(RpDelete::default()),
            _ => Err(parse_error(resp).await?),
        }
    }
}
