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
use std::fmt::Formatter;
use std::sync::Arc;

use http::Response;
use http::StatusCode;

use super::core::OneDriveCore;
use super::delete::OneDriveDeleter;
use super::error::parse_error;
use super::lister::OneDriveLister;
use super::writer::OneDriveWriter;
use crate::raw::*;
use crate::*;

#[derive(Clone)]
pub struct OnedriveBackend {
    pub core: Arc<OneDriveCore>,
}

impl Debug for OnedriveBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OnedriveBackend")
            .field("core", &self.core)
            .finish()
    }
}

impl Access for OnedriveBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<OneDriveWriter>;
    type Lister = oio::PageLister<OneDriveLister>;
    type Deleter = oio::OneShotDeleter<OneDriveDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _args: OpCreateDir) -> Result<RpCreateDir> {
        if path == "/" {
            // skip, the root path exists in the personal OneDrive.
            return Ok(RpCreateDir::default());
        }

        let response = self.core.onedrive_create_dir(path).await?;

        let status = response.status();
        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(response)),
        }
    }

    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        let path = build_rooted_abs_path(&self.core.root, path);
        let meta = self.core.onedrive_stat(path.as_str()).await?;

        Ok(RpStat::new(meta))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let response = self.core.onedrive_get_content(path, args.range()).await?;

        let status = response.status();
        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), response.into_body()))
            }
            _ => {
                let (part, mut body) = response.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let path = build_rooted_abs_path(&self.core.root, path);

        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(OneDriveWriter::new(self.core.clone(), args, path)),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(OneDriveDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = OneDriveLister::new(path.to_string(), self.core.clone());
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
