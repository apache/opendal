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

use std::fmt;
use std::fmt::Write;
use std::str;
use std::sync::Arc;

use bytes::Buf;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;

use super::delete::IpmfsDeleter;
use super::error::parse_error;
use super::lister::IpmfsLister;
use super::writer::IpmfsWriter;
use crate::raw::*;
use crate::*;

/// IPFS Mutable File System (IPMFS) backend.
#[doc = include_str!("docs.md")]
#[derive(Clone)]
pub struct IpmfsBackend {
    info: Arc<AccessorInfo>,
    root: String,
    endpoint: String,
    client: HttpClient,
}

impl fmt::Debug for IpmfsBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backend")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl IpmfsBackend {
    pub(crate) fn new(root: String, client: HttpClient, endpoint: String) -> Self {
        Self {
            info: {
                let am = AccessorInfo::default();
                am.set_scheme(Scheme::Ipmfs)
                    .set_root(&root)
                    .set_native_capability(Capability {
                        stat: true,
                        stat_has_content_length: true,

                        read: true,

                        write: true,
                        delete: true,

                        list: true,
                        list_has_content_length: true,

                        shared: true,

                        ..Default::default()
                    });

                am.into()
            },
            root,
            client,
            endpoint,
        }
    }
}

impl Access for IpmfsBackend {
    type Reader = HttpBody;
    type Writer = oio::OneShotWriter<IpmfsWriter>;
    type Lister = oio::PageLister<IpmfsLister>;
    type Deleter = oio::OneShotDeleter<IpmfsDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let resp = self.ipmfs_mkdir(path).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => Ok(RpCreateDir::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.ipmfs_stat(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let res: IpfsStatResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                let mode = match res.file_type.as_str() {
                    "file" => EntryMode::FILE,
                    "directory" => EntryMode::DIR,
                    _ => EntryMode::Unknown,
                };

                let mut meta = Metadata::new(mode);
                meta.set_content_length(res.size);

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.ipmfs_read(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                Ok((RpRead::default(), resp.into_body()))
            }
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(IpmfsWriter::new(self.clone(), path.to_string())),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(IpmfsDeleter::new(Arc::new(self.clone()))),
        ))
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let l = IpmfsLister::new(Arc::new(self.clone()), &self.root, path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

impl IpmfsBackend {
    async fn ipmfs_stat(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/stat?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn ipmfs_read(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let mut url = format!(
            "{}/api/v0/files/read?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        write!(url, "&offset={}", range.offset()).expect("write into string must succeed");
        if let Some(count) = range.size() {
            write!(url, "&count={count}").expect("write into string must succeed")
        }

        let req = Request::post(url);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.fetch(req).await
    }

    pub async fn ipmfs_rm(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/rm?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub(crate) async fn ipmfs_ls(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/ls?arg={}&long=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn ipmfs_mkdir(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/mkdir?arg={}&parents=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    /// Support write from reader.
    pub async fn ipmfs_write(&self, path: &str, body: Buffer) -> Result<Response<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/write?arg={}&parents=true&create=true&truncate=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let multipart = Multipart::new().part(FormDataPart::new("data").content(body));

        let req: http::request::Builder = Request::post(url);
        let req = multipart.apply(req)?;

        self.client.send(req).await
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
