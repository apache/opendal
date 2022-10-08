// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt;
use std::fmt::Write;
use std::io::Result;
use std::str;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::accessor::AccessorCapability;
use crate::error::new_other_object_error;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::new_response_consume_error;
use crate::http_util::parse_error_response;
use crate::http_util::percent_encode_path;
use crate::http_util::AsyncBody;
use crate::http_util::HttpClient;
use crate::http_util::IncomingAsyncBody;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::path::build_rooted_abs_path;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectStreamer;
use crate::Scheme;

/// Backend for IPFS service
#[derive(Clone)]
pub struct Backend {
    root: String,
    endpoint: String,
    client: HttpClient,
}

impl fmt::Debug for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl Backend {
    pub(crate) fn new(root: String, client: HttpClient, endpoint: String) -> Self {
        Self {
            root,
            client,
            endpoint,
        }
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Ipmfs)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            );

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        let resp = match args.mode() {
            ObjectMode::DIR => self.ipmfs_mkdir(path).await?,
            ObjectMode::FILE => self.ipmfs_write(path, AsyncBody::Empty).await?,
            _ => unreachable!(),
        };

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Create, path, err))?;
                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Create, path, er);
                Err(err)
            }
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let resp = self.ipmfs_read(path, args.offset(), args.size()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(resp.into_body().reader()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, path, er);
                Err(err)
            }
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: BytesReader) -> Result<u64> {
        let resp = self
            .ipmfs_write(path, AsyncBody::Multipart("data".to_string(), r))
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Write, path, err))?;
                Ok(args.size())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Write, path, er);
                Err(err)
            }
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }

        let resp = self.ipmfs_stat(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp
                    .into_body()
                    .bytes()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Stat, path, err))?;

                let res: IpfsStatResponse = serde_json::from_slice(&bs).map_err(|err| {
                    new_other_object_error(
                        Operation::Stat,
                        path,
                        anyhow!("deserialize json: {err:?}"),
                    )
                })?;

                let mode = match res.file_type.as_str() {
                    "file" => ObjectMode::FILE,
                    "directory" => ObjectMode::DIR,
                    _ => ObjectMode::Unknown,
                };

                let mut meta = ObjectMetadata::new(mode);
                meta.set_content_length(res.size);

                Ok(meta)
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, path, er);
                Err(err)
            }
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
        let resp = self.ipmfs_rm(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Delete, path, err))?;
                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Delete, path, er);
                Err(err)
            }
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        Ok(Box::new(DirStream::new(
            Arc::new(self.clone()),
            &self.root,
            path,
        )))
    }
}

impl Backend {
    async fn ipmfs_stat(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/stat?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(|err| new_request_build_error(Operation::Stat, path, err))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Stat, path, e))
    }

    async fn ipmfs_read(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let mut url = format!(
            "{}/api/v0/files/read?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );
        if let Some(offset) = offset {
            write!(url, "&offset={offset}").expect("write into string must succeed")
        }
        if let Some(count) = size {
            write!(url, "&count={count}").expect("write into string must succeed")
        }

        let req = Request::post(url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(|err| new_request_build_error(Operation::Read, path, err))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Read, path, e))
    }

    async fn ipmfs_rm(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/rm?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(|err| new_request_build_error(Operation::Delete, path, err))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Delete, path, e))
    }

    pub(crate) async fn ipmfs_ls(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/ls?arg={}&long=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(|err| new_request_build_error(Operation::List, path, err))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::List, path, e))
    }

    async fn ipmfs_mkdir(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/mkdir?arg={}&parents=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(|err| new_request_build_error(Operation::Create, path, err))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Create, path, e))
    }

    /// Support write from reader.
    async fn ipmfs_write(
        &self,
        path: &str,
        body: AsyncBody,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/write?arg={}&parents=true&create=true&truncate=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);

        let req = req
            .body(body)
            .map_err(|err| new_request_build_error(Operation::Write, path, err))?;

        let resp = self
            .client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Write, path, e))?;

        Ok(resp)
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsStatResponse {
    #[serde(rename = "Size")]
    size: u64,
    #[serde(rename = "Type")]
    file_type: String,
}
