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
use bytes::Bytes;
use futures::io;
use http::StatusCode;
use http::{Request, Response};
use serde::Deserialize;

use super::builder::Builder;
use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::accessor::AccessorCapability;
use crate::error::other;
use crate::error::ObjectError;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::new_response_consume_error;
use crate::http_util::parse_error_response;
use crate::http_util::percent_encode_path;
use crate::http_util::AsyncBody;
use crate::http_util::HttpClient;
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
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;
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

    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();

        for (key, val) in it {
            let val = val.as_str();
            match key.as_ref() {
                "root" => builder.root(val),
                "endpoint" => builder.endpoint(val),
                _ => continue,
            };
        }

        builder.build()
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

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, args.path());

        let resp = match args.mode() {
            ObjectMode::DIR => self.ipfs_mkdir(&path).await?,
            ObjectMode::FILE => self.ipfs_write(&path, &[]).await?,
            _ => unreachable!(),
        };

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Create, &path, err))?;
                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Create, args.path(), er);
                Err(err)
            }
        }
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = build_rooted_abs_path(&self.root, args.path());

        let offset = args.offset().and_then(|val| i64::try_from(val).ok());
        let size = args.size().and_then(|val| i64::try_from(val).ok());
        let resp = self.ipfs_read(&path, offset, size).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(resp.into_body().reader()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, args.path(), er);
                Err(err)
            }
        }
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = build_rooted_abs_path(&self.root, args.path());

        // TODO: Accept a reader directly.
        let mut buf = Vec::with_capacity(args.size() as usize);
        io::copy(r, &mut buf).await?;

        let resp = self.ipfs_write(&path, &buf).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Write, &path, err))?;
                Ok(args.size())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Write, args.path(), er);
                Err(err)
            }
        }
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = build_rooted_abs_path(&self.root, args.path());

        // Stat root always returns a DIR.
        if path == self.root {
            let mut m = ObjectMetadata::default();
            m.set_mode(ObjectMode::DIR);

            return Ok(m);
        }

        let resp = self.ipfs_stat(&path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp
                    .into_body()
                    .bytes()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Stat, &path, err))?;

                let res: IpfsStatResponse = serde_json::from_slice(&bs).map_err(|err| {
                    other(ObjectError::new(
                        Operation::Stat,
                        &path,
                        anyhow!("deserialize json: {err:?}"),
                    ))
                })?;

                let mut meta = ObjectMetadata::default();
                meta.set_mode(match res.file_type.as_str() {
                    "file" => ObjectMode::FILE,
                    "directory" => ObjectMode::DIR,
                    _ => ObjectMode::Unknown,
                });
                meta.set_content_length(res.size);

                Ok(meta)
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, args.path(), er);
                Err(err)
            }
        }
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = build_rooted_abs_path(&self.root, args.path());

        let resp = self.ipfs_rm(&path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body()
                    .consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Delete, &path, err))?;
                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Delete, args.path(), er);
                Err(err)
            }
        }
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = build_rooted_abs_path(&self.root, args.path());

        Ok(Box::new(DirStream::new(
            Arc::new(self.clone()),
            &self.root,
            &path,
        )))
    }
}

impl Backend {
    async fn ipfs_stat(&self, path: &str) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/stat?arg={}",
            self.endpoint,
            percent_encode_path(path)
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

    async fn ipfs_read(
        &self,
        path: &str,
        offset: Option<i64>,
        size: Option<i64>,
    ) -> Result<Response<AsyncBody>> {
        let mut url = format!(
            "{}/api/v0/files/read?arg={}",
            self.endpoint,
            percent_encode_path(path)
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

    async fn ipfs_rm(&self, path: &str) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/rm?arg={}",
            self.endpoint,
            percent_encode_path(path)
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

    pub(crate) async fn ipfs_ls(&self, path: &str) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/ls?arg={}&long=true",
            self.endpoint,
            percent_encode_path(path)
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

    async fn ipfs_mkdir(&self, path: &str) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/mkdir?arg={}&parents=true",
            self.endpoint,
            percent_encode_path(path)
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
    async fn ipfs_write(&self, path: &str, data: &[u8]) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/write?arg={}&parents=true&create=true&truncate=true",
            self.endpoint,
            percent_encode_path(path)
        );

        let mut req = Request::post(url);

        req = req.header(
            http::header::CONTENT_TYPE,
            "multipart/form-data; boundary=custom-boundary",
        );
        let left =
            "--custom-boundary\nContent-Disposition: form-data; name=\"data\";\n\n".as_bytes();
        let right = "\n--custom-boundary".as_bytes();

        // TODO: we need to accept a reader.
        let mut buf = Vec::with_capacity(left.len() + data.len() + right.len());
        buf.extend_from_slice(left);
        buf.extend_from_slice(data);
        buf.extend_from_slice(right);

        let body = AsyncBody::Bytes(Bytes::from(buf));
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
