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

use base64::engine::general_purpose;
use base64::Engine;
use bytes::Buf;
use md5::Digest;
use md5::Md5;
use sha1::Sha1;
use tokio::sync::RwLock;

use super::core::AliyunDriveCore;
use super::core::RapidUpload;
use super::core::UploadUrlResponse;
use crate::raw::*;
use crate::services::aliyun_drive::core::CheckNameMode;
use crate::services::aliyun_drive::core::CreateResponse;
use crate::services::aliyun_drive::core::CreateType;
use crate::*;

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

    async fn get_rapid_upload(
        &self,
        size: Option<u64>,
        body: Option<crate::Buffer>,
        pre_hash: bool,
    ) -> Result<Option<RapidUpload>> {
        let Some(size) = size else {
            return Ok(None);
        };
        let Some(body) = body else {
            return Ok(None);
        };
        if pre_hash && size > 1024 * 100 {
            return Ok(Some(RapidUpload {
                pre_hash: Some(format!(
                    "{:x}",
                    Sha1::new_with_prefix(body.slice(0..1024).to_vec()).finalize()
                )),
                content_hash: None,
                proof_code: None,
            }));
        }
        let (token, _) = self.core.get_token_and_drive().await?;
        let Ok(index) = u64::from_str_radix(
            &format!("{:x}", Md5::new_with_prefix(token.unwrap()).finalize())[0..16],
            16,
        ) else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "cannot parse hexadecimal",
            ));
        };
        let size = size as usize;
        let index = index as usize % size;
        let (range_start, range_end) = if index + 8 > size {
            (index, size)
        } else {
            (index, index + 8)
        };

        Ok(Some(RapidUpload {
            pre_hash: None,
            content_hash: Some(format!(
                "{:x}",
                Sha1::new_with_prefix(body.to_vec()).finalize()
            )),
            proof_code: Some(
                general_purpose::STANDARD.encode(body.to_bytes().slice(range_start..range_end)),
            ),
        }))
    }

    async fn write(
        &self,
        size: Option<u64>,
        body: Option<crate::Buffer>,
        pre_hash: bool,
        upload_url: Option<&str>,
    ) -> Result<(bool, Option<String>)> {
        if let Some(upload_url) = upload_url {
            let Some(body) = body else {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "cannot upload without body",
                ));
            };
            self.core.upload(upload_url, body).await?;
            return Ok((false, None));
        }

        let res = self
            .core
            .create_with_rapid_upload(
                Some(&self.parent_file_id),
                &self.name,
                CreateType::File,
                CheckNameMode::Refuse,
                size,
                self.get_rapid_upload(size, body.clone(), pre_hash).await?,
            )
            .await;

        let res = match res {
            Err(err) if err.kind() == ErrorKind::IsSameFile => {
                return Ok((true, None));
            }
            Err(err) => {
                return Err(err);
            }
            Ok(res) => res,
        };

        let output: CreateResponse =
            serde_json::from_reader(res.reader()).map_err(new_json_deserialize_error)?;
        self.write_file_id(output.file_id).await;

        if output.upload_id.is_some() && output.rapid_upload.is_some_and(|x| !x) {
            if let Some(body) = body {
                let Some(part_info_list) = output.part_info_list else {
                    return Err(Error::new(ErrorKind::Unexpected, "cannot find upload_url"));
                };
                if part_info_list.is_empty() {
                    return Err(Error::new(ErrorKind::Unexpected, "cannot find upload_url"));
                }
                self.core
                    .upload(&part_info_list[0].upload_url, body)
                    .await?;
            }
        }

        Ok((false, output.upload_id))
    }
}

impl oio::MultipartWrite for AliyunDriveWriter {
    async fn write_once(&self, size: u64, body: crate::Buffer) -> Result<()> {
        let upload_id = if self.core.rapid_upload {
            let (rapid, mut upload_id) = self
                .write(Some(size), Some(body.clone()), true, None)
                .await?;

            let size = if rapid { Some(size) } else { None };
            let (_, new_upload_id) = self.write(size, Some(body), false, None).await?;

            if new_upload_id.is_some() {
                upload_id = new_upload_id;
            }

            let Some(upload_id) = upload_id else {
                return Err(Error::new(ErrorKind::Unexpected, "cannot find upload_id"));
            };
            upload_id
        } else {
            let upload_id = self.initiate_part().await?;
            self.write_part(&upload_id, 0, size, body).await?;
            upload_id
        };
        let file_id = self.read_file_id().await?;

        self.core.complete(&file_id, &upload_id).await?;

        Ok(())
    }

    async fn initiate_part(&self) -> Result<String> {
        let (_, upload_id) = self.write(None, None, false, None).await?;

        let Some(upload_id) = upload_id else {
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
        self.write(None, Some(body), false, Some(&part_info_list[0].upload_url))
            .await?;

        Ok(oio::MultipartPart {
            part_number,
            etag: part_info_list[0].etag.clone().unwrap_or("".to_string()),
            checksum: None,
        })
    }

    async fn complete_part(&self, upload_id: &str, _parts: &[oio::MultipartPart]) -> Result<()> {
        let file_id = self.read_file_id().await?;
        self.core.complete(&file_id, upload_id).await?;

        Ok(())
    }

    async fn abort_part(&self, _upload_id: &str) -> Result<()> {
        let file_id = self.read_file_id().await?;

        self.core.delete_path(&file_id).await
    }
}
