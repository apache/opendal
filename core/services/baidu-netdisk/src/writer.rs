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

use crate::core::{BaiduNetdiskCore, FILE_API, FileMetadata};
use http::{Request, header};
use md5::{Digest, Md5};
use opendal_core::{raw::*, *};
use serde::Deserialize;
use std::sync::Arc;

const BLOCK_SIZE: usize = 4 * 1024 * 1024;
pub struct BaiduNetdiskWriter {
    pub core: Arc<BaiduNetdiskCore>,
    pub ctx: OperationContext,
    pub path: String,
}

#[derive(Deserialize)]
struct Precreate {
    uploadid: String,
    block_list: Vec<usize>,
}
#[derive(Deserialize)]
struct LocateUpload {
    servers: Vec<UploadServer>,
}
#[derive(Deserialize)]
struct UploadServer {
    server: String,
}
#[derive(Deserialize)]
struct UploadPart {
    md5: String,
}

impl oio::OneShotWrite for BaiduNetdiskWriter {
    async fn write_once(&self, data: Buffer) -> Result<Metadata> {
        if data.len() as u64 > 4 * 1024 * 1024 * 1024 {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "Baidu Netdisk writes are limited to 4 GiB",
            ));
        }
        match self.core.stat(&self.ctx, &self.path).await {
            Ok(file) if file.isdir == 1 => {
                return Err(Error::new(
                    ErrorKind::IsADirectory,
                    "write destination is a directory",
                ));
            }
            Err(e) if e.kind() != ErrorKind::NotFound => return Err(e),
            _ => {}
        }
        self.core
            .create_dir(&self.ctx, get_parent(&self.path))
            .await?;
        let path = self.core.absolute_path(&self.path);
        let blocks: Vec<Buffer> = if data.is_empty() {
            vec![Buffer::new()]
        } else {
            (0..data.len())
                .step_by(BLOCK_SIZE)
                .map(|start| data.slice(start..(start + BLOCK_SIZE).min(data.len())))
                .collect()
        };
        let hashes: Vec<String> = blocks
            .iter()
            .map(|block| {
                let mut md5 = Md5::new();
                for chunk in block.clone() {
                    md5.update(chunk);
                }
                hex::encode(md5.finalize())
            })
            .collect();
        let block_list = serde_json::to_string(&hashes).map_err(new_json_serialize_error)?;
        let size = data.len().to_string();
        let form = form_urlencoded::Serializer::new(String::new())
            .append_pair("path", &path)
            .append_pair("size", &size)
            .append_pair("isdir", "0")
            .append_pair("autoinit", "1")
            .append_pair("rtype", "3")
            .append_pair("block_list", &block_list)
            .finish();
        let req = Request::post(format!("{FILE_API}?method=precreate"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(form))
            .map_err(new_request_build_error)?;
        let response = self.core.send(&self.ctx, req).await?;
        let mut pre: Precreate =
            serde_json::from_value(response).map_err(new_json_deserialize_error)?;
        // The API defines an empty list as a request to upload block zero.
        if pre.block_list.is_empty() {
            pre.block_list.push(0);
        }
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("method", "locateupload")
            .append_pair("appid", "250528")
            .append_pair("upload_version", "2.0")
            .append_pair("path", &path)
            .append_pair("uploadid", &pre.uploadid)
            .finish();
        let req = Request::get(format!("https://d.pcs.baidu.com/rest/2.0/pcs/file?{query}"))
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        let response = self.core.send(&self.ctx, req).await?;
        let locate: LocateUpload =
            serde_json::from_value(response).map_err(new_json_deserialize_error)?;
        let server = locate
            .servers
            .into_iter()
            .find(|item| item.server.starts_with("https://"))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Baidu Netdisk did not return an HTTPS upload server",
                )
            })?
            .server;
        for index in pre.block_list {
            let block = blocks.get(index).ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Baidu Netdisk requested an invalid block index",
                )
            })?;
            let query = form_urlencoded::Serializer::new(String::new())
                .append_pair("method", "upload")
                .append_pair("type", "tmpfile")
                .append_pair("path", &path)
                .append_pair("uploadid", &pre.uploadid)
                .append_pair("partseq", &index.to_string())
                .finish();
            let part = FormDataPart::new("file")
                .header(
                    header::CONTENT_DISPOSITION,
                    http::HeaderValue::from_static(r#"form-data; name="file"; filename="part""#),
                )
                .header(
                    header::CONTENT_TYPE,
                    http::HeaderValue::from_static("application/octet-stream"),
                )
                .content(block.clone());
            let req = Multipart::new().part(part).apply(Request::post(format!(
                "{}/rest/2.0/pcs/superfile2?{query}",
                server.trim_end_matches('/')
            )))?;
            let response = self.core.send(&self.ctx, req).await?;
            let uploaded: UploadPart =
                serde_json::from_value(response).map_err(new_json_deserialize_error)?;
            if uploaded.md5 != hashes[index] {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "Baidu Netdisk uploaded block MD5 mismatch",
                ));
            }
        }
        let form = form_urlencoded::Serializer::new(String::new())
            .append_pair("path", &path)
            .append_pair("size", &size)
            .append_pair("isdir", "0")
            .append_pair("rtype", "3")
            .append_pair("uploadid", &pre.uploadid)
            .append_pair("block_list", &block_list)
            .finish();
        let req = Request::post(format!("{FILE_API}?method=create"))
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(Buffer::from(form))
            .map_err(new_request_build_error)?;
        let response = self.core.send(&self.ctx, req).await?;
        let file: FileMetadata =
            serde_json::from_value(response).map_err(new_json_deserialize_error)?;
        file.metadata()
    }
}
