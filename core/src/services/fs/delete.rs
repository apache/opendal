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
use crate::raw::*;
use crate::*;
use std::sync::Arc;

pub struct FsDeleter {
    core: Arc<FsCore>,
}

impl FsDeleter {
    pub fn new(core: Arc<FsCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for FsDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        let meta = tokio::fs::metadata(&p).await;

        match meta {
            Ok(meta) => {
                if meta.is_dir() {
                    tokio::fs::remove_dir(&p).await.map_err(new_std_io_error)?;
                } else {
                    tokio::fs::remove_file(&p).await.map_err(new_std_io_error)?;
                }

                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(new_std_io_error(err)),
        }
    }
}

impl oio::BlockingOneShotDelete for FsDeleter {
    fn blocking_delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let p = self.core.root.join(path.trim_end_matches('/'));

        let meta = std::fs::metadata(&p);

        match meta {
            Ok(meta) => {
                if meta.is_dir() {
                    std::fs::remove_dir(&p).map_err(new_std_io_error)?;
                } else {
                    std::fs::remove_file(&p).map_err(new_std_io_error)?;
                }

                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(new_std_io_error(err)),
        }
    }
}
