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

use crate::core::{BaiduNetdiskCore, FileList, parse_response};
use http::{Request, Response, StatusCode, header};
use opendal_core::{raw::*, *};
use std::sync::Arc;

pub struct BaiduNetdiskReader {
    pub core: Arc<BaiduNetdiskCore>,
    pub ctx: OperationContext,
    pub path: String,
}
impl oio::StreamRead for BaiduNetdiskReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let file = self.core.stat(&self.ctx, &self.path).await?;
        if file.isdir == 1 {
            return Err(Error::new(
                ErrorKind::IsADirectory,
                "cannot read a directory",
            ));
        }
        let query = form_urlencoded::Serializer::new(String::new())
            .append_pair("method", "filemetas")
            .append_pair("fsids", &format!("[{}]", file.fs_id))
            .append_pair("dlink", "1")
            .finish();
        let req = Request::get(format!(
            "https://pan.baidu.com/rest/2.0/xpan/multimedia?{query}"
        ))
        .body(Buffer::new())
        .map_err(new_request_build_error)?;
        let value = self.core.send(&self.ctx, req).await?;
        let files: FileList = serde_json::from_value(value).map_err(new_json_deserialize_error)?;
        let link = files
            .list
            .into_iter()
            .find(|item| item.fs_id == file.fs_id)
            .and_then(|item| item.dlink)
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Baidu Netdisk did not return a download URL",
                )
            })?;
        let mut builder = Request::get(link);
        if !range.is_full() {
            builder = builder.header(header::RANGE, range.to_header());
        }
        let req = self
            .core
            .sign(
                &self.ctx,
                builder
                    .body(Buffer::new())
                    .map_err(new_request_build_error)?,
            )
            .await?;
        let mut response = self.ctx.http_transport().fetch(req).await?;
        match response.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                // Baidu sends raw UTF-8 filenames in Content-Disposition, which
                // HeaderValue::to_str rejects because it only accepts visible ASCII.
                let disposition = response.headers_mut().remove(header::CONTENT_DISPOSITION);
                let mut metadata =
                    parse_into_metadata(&self.path, response.headers())?.into_builder();
                if let Some(value) = disposition {
                    let value = std::str::from_utf8(value.as_bytes()).map_err(|e| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "invalid UTF-8 in Baidu Netdisk Content-Disposition",
                        )
                        .set_source(e)
                    })?;
                    metadata.content_disposition(value);
                }
                Ok((
                    RpRead::new(metadata.build()),
                    Box::new(response.into_body()),
                ))
            }
            _ => {
                let (parts, mut body) = response.into_parts();
                parse_response(Response::from_parts(parts, body.to_buffer().await?))?;
                Err(Error::new(
                    ErrorKind::Unexpected,
                    "unexpected Baidu Netdisk download response",
                ))
            }
        }
    }
}
