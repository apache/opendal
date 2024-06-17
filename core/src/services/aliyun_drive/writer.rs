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

use super::core::{AliyunDriveCore, UploadUrlResponse};
use crate::{
    raw::*,
    services::aliyun_drive::core::{CheckNameMode, CreateResponse, CreateType},
    *,
};
use bytes::Buf;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type AliyunDriveWriters = oio::MultipartWriter<AliyunDriveWriter>;

pub struct AliyunDriveWriter {
    core: Arc<AliyunDriveCore>,

    _op: OpWrite,
    parent_file_id: String,
    name: String,

    file_id: Arc<RwLock<Option<String>>>,
}

impl AliyunDriveWriter {
    pub fn new(core: Arc<AliyunDriveCore>, parent_file_id: &str, name: &str, op: OpWrite) -> Self {
        AliyunDriveWriter {
            core,
            _op: op,
            parent_file_id: parent_file_id.to_string(),
            name: name.to_string(),
            file_id: Arc::new(RwLock::new(None)),
        }
    }

    async fn write_file_id(&self, id: String) {
        let mut file_id = self.file_id.write().await;

        *file_id = Some(id);
    }

    async fn read_file_id(&self) -> Result<String> {
        let file_id = self.file_id.read().await;
        let Some(ref file_id) = *file_id else {
            return Err(Error::new(ErrorKind::Unexpected, "cannot find file_id"));
        };

        Ok(file_id.clone())
    }

    async fn write(
        &self,
        body: Option<Buffer>,
        upload_url: Option<&str>,
    ) -> Result<Option<String>> {
        if let Some(upload_url) = upload_url {
            let Some(body) = body else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "cannot upload without body",
                ));
            };
            if let Err(err) = self.core.upload(upload_url, body).await {
                if err.kind() != ErrorKind::AlreadyExists {
                    return Err(err);
                }
            };
            return Ok(None);
        }

        let res = self
            .core
            .create(
                Some(&self.parent_file_id),
                &self.name,
                CreateType::File,
                CheckNameMode::Refuse,
            )
            .await;

        let res = match res {
            Err(err) if err.kind() == ErrorKind::IsSameFile => {
                return Ok(None);
            }
            Err(err) => {
                return Err(err);
            }
            Ok(res) => res,
        };

        let output: CreateResponse =
            serde_json::from_reader(res.reader()).map_err(new_json_deserialize_error)?;
        self.write_file_id(output.file_id).await;
        if output.exist.is_some_and(|x| x) {
            return Err(Error::new(ErrorKind::AlreadyExists, "file exists"));
        }

        if output.upload_id.is_some() {
            if let Some(body) = body {
                let Some(part_info_list) = output.part_info_list else {
                    return Err(Error::new(ErrorKind::Unexpected, "cannot find upload_url"));
                };
                if part_info_list.is_empty() {
                    return Err(Error::new(ErrorKind::Unexpected, "cannot find upload_url"));
                }
                if let Err(err) = self.core.upload(&part_info_list[0].upload_url, body).await {
                    if err.kind() != ErrorKind::AlreadyExists {
                        return Err(err);
                    }
                }
            }
        }

        Ok(output.upload_id)
    }

    async fn complete(&self, upload_id: &str) -> Result<Buffer> {
        let file_id = self.read_file_id().await?;

        self.core.complete(&file_id, upload_id).await
    }

    async fn delete(&self) -> Result<()> {
        let file_id = self.read_file_id().await?;

        self.core.delete_path(&file_id).await
    }
}

impl oio::MultipartWrite for AliyunDriveWriter {
    async fn write_once(&self, size: u64, body: crate::Buffer) -> Result<()> {
        let upload_id = self.initiate_part().await?;
        self.write_part(&upload_id, 0, size, body).await?;

        self.complete(&upload_id).await?;
        Ok(())
    }

    async fn initiate_part(&self) -> Result<String> {
        let Some(upload_id) = self.write(None, None).await? else {
            return Err(Error::new(ErrorKind::Unsupported, "cannot find upload_id"));
        };

        Ok(upload_id)
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        _size: u64,
        body: crate::Buffer,
    ) -> Result<oio::MultipartPart> {
        let file_id = self.read_file_id().await?;
        let res = self
            .core
            .get_upload_url(&file_id, upload_id, Some(part_number + 1))
            .await?;
        let output: UploadUrlResponse =
            serde_json::from_reader(res.reader()).map_err(new_json_deserialize_error)?;

        let Some(part_info_list) = output.part_info_list else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "cannot find part_info_list",
            ));
        };
        if part_info_list.is_empty() {
            return Err(Error::new(ErrorKind::Unexpected, "cannot find upload_url"));
        }
        self.write(Some(body), Some(&part_info_list[0].upload_url))
            .await?;

        Ok(oio::MultipartPart {
            part_number,
            etag: part_info_list[0].etag.clone().unwrap_or("".to_string()),
            checksum: None,
        })
    }

    async fn complete_part(&self, upload_id: &str, _parts: &[oio::MultipartPart]) -> Result<()> {
        self.complete(upload_id).await?;

        Ok(())
    }

    async fn abort_part(&self, _upload_id: &str) -> Result<()> {
        self.delete().await
    }
}
