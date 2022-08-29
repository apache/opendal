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
use futures::AsyncReadExt;
use http::Response;
use http::StatusCode;
use isahc;
use isahc::AsyncBody;
use isahc::AsyncReadResponseExt;
use serde::Deserialize;

use super::builder::Builder;
use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::error::other;
use crate::error::ObjectError;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::new_response_consume_error;
use crate::http_util::parse_error_response;
use crate::http_util::percent_encode_path;
use crate::http_util::HttpClient;
use crate::io_util::unshared_reader;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
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

#[derive(Deserialize, Debug)]
struct StatBody {
    #[serde(rename(deserialize = "Size"))]
    size: u64,
    #[serde(rename(deserialize = "Type"))]
    file_type: String,
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

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == self.root {
            return path.to_string();
        }

        format!("{}{}", self.root, path.trim_start_matches(&self.root))
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Ipfs).set_root(&self.root);

        am
    }

    /// TODO: support creating dir.
    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = self.get_abs_path(args.path());

        let mut resp = self.ipfs_write(&path, "").await?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                resp.consume()
                    .await
                    .map_err(|err| new_response_consume_error(Operation::Write, &path, err))?;
                Ok(())
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Write, args.path(), er);
                Err(err)
            }
        }
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = self.get_abs_path(args.path());

        let offset = args.offset().and_then(|val| i64::try_from(val).ok());
        let size = args.size().and_then(|val| i64::try_from(val).ok());
        let reader = self.ipfs_read(&path, offset, size).await?;
        Ok(Box::new(reader.into_body()))
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = self.get_abs_path(args.path());

        let mut string_buff: String = "".to_string();
        unshared_reader(r).read_to_string(&mut string_buff).await?;

        let mut resp = self.ipfs_write(&path, &string_buff).await?;

        match resp.status() {
            StatusCode::CREATED | StatusCode::OK => {
                resp.consume()
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
        let path = self.get_abs_path(args.path());

        let mut resp = self.ipfs_stat(&path).await?;

        let bs = resp
            .bytes()
            .await
            .map_err(|err| new_response_consume_error(Operation::Stat, &path, err))?;

        let res: StatBody = serde_json::from_slice(&bs).map_err(|err| {
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

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = self.get_abs_path(args.path());

        let mut resp = self.ipfs_rm(&path).await?;

        match resp.status() {
            StatusCode::OK => {
                resp.consume()
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
        let path = self.get_abs_path(args.path());
        Ok(Box::new(DirStream::new(Arc::new(self.clone()), &path)))
    }
}

impl Backend {
    async fn ipfs_stat(&self, path: &str) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/stat?arg={}",
            self.endpoint,
            percent_encode_path(path)
        );

        let req = isahc::Request::post(url);
        let req = req
            .body(AsyncBody::empty())
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

        let req = isahc::Request::post(url);
        let req = req
            .body(AsyncBody::empty())
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

        let req = isahc::Request::post(url);
        let req = req
            .body(AsyncBody::empty())
            .map_err(|err| new_request_build_error(Operation::Delete, path, err))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Delete, path, e))
    }

    pub(crate) async fn ipfs_ls(&self, path: &str) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/ls?arg={}",
            self.endpoint,
            percent_encode_path(path)
        );

        let req = isahc::Request::post(url);
        let req = req
            .body(AsyncBody::empty())
            .map_err(|err| new_request_build_error(Operation::Delete, path, err))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Delete, path, e))
    }

    /// Support write from reader.
    async fn ipfs_write(&self, path: &str, data: &str) -> Result<Response<AsyncBody>> {
        let url = format!(
            "{}/api/v0/files/write?arg={}&create=true&truncate=false",
            self.endpoint,
            percent_encode_path(path)
        );

        let mut req = isahc::Request::post(url);

        req = req.header(
            http::header::CONTENT_TYPE,
            "multipart/form-data; boundary=custom-boundary",
        );
        let left = "--custom-boundary\nContent-Disposition: form-data; name=data;\n\n";
        let right = "\n--custom-boundary";

        let buff = format!("{}{}{}", left, data, right);

        let body = AsyncBody::from_bytes_static(buff);
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
