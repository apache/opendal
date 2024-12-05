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

use super::core::{AliyunDriveCore, AliyunDriveFile};
use crate::raw::*;
use crate::*;
use bytes::Buf;
use std::sync::Arc;

pub struct AliyunDriveDeleter {
    core: Arc<AliyunDriveCore>,
}

impl AliyunDriveDeleter {
    pub fn new(core: Arc<AliyunDriveCore>) -> Self {
        AliyunDriveDeleter { core }
    }
}

impl oio::OneShotDelete for AliyunDriveDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let res = match self.core.get_by_path(&path).await {
            Ok(output) => Some(output),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };
        if let Some(res) = res {
            let file: AliyunDriveFile =
                serde_json::from_reader(res.reader()).map_err(new_json_serialize_error)?;
            self.core.delete_path(&file.file_id).await?;
        }
        Ok(())
    }
}
