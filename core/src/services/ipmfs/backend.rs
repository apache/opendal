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

use async_trait::async_trait;
use bytes::Buf;
use bytes::Bytes;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;

use super::error::parse_error;
use super::lister::IpmfsLister;
use super::writer::IpmfsWriter;
use crate::raw::*;
use crate::services::ipmfs::reader::IpmfsReader;
use crate::*;

/// IPFS Mutable File System (IPMFS) backend.
#[doc = include_str!("docs.md")]
#[derive(Clone)]
pub struct IpmfsBackend {
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
            root,
            client,
            endpoint,
        }
    }
}

#[async_trait]
impl Accessor for IpmfsBackend {
    type Reader = IpmfsReader;
    type Writer = oio::OneShotWriter<IpmfsWriter>;
    type Lister = oio::PageLister<IpmfsLister>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Ipmfs)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,
                delete: true,

                list: true,

                ..Default::default()
            });

        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.ipmfs_mkdir(path).await.map(|_| RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        self.ipmfs_stat(path).await.map(RpStat::new)
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        Ok((
            RpRead::default(),
            IpmfsReader::new(self.clone(), path, args),
        ))
    }

    async fn write(&self, path: &str, _: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(IpmfsWriter::new(self.clone(), path.to_string())),
        ))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        self.ipmfs_rm(path).await.map(|_| RpDelete::new())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let l = IpmfsLister::new(Arc::new(self.clone()), &self.root, path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

impl IpmfsBackend {
    async fn ipmfs_stat(&self, path: &str) -> Result<Metadata> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/stat?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                let res: IpfsStatResponse = body.to_json().await?;

                let mode = match res.file_type.as_str() {
                    "file" => EntryMode::FILE,
                    "directory" => EntryMode::DIR,
                    _ => EntryMode::Unknown,
                };

                let mut meta = Metadata::new(mode);
                meta.set_content_length(res.size);

                Ok(meta)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub async fn ipmfs_read(
        &self,
        path: &str,
        range: BytesRange,
        buf: oio::WritableBuf,
    ) -> Result<usize> {
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
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => body.read(buf).await,
            StatusCode::RANGE_NOT_SATISFIABLE => {
                body.consume().await?;
                Ok(0)
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    async fn ipmfs_rm(&self, path: &str) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/rm?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    pub(crate) async fn ipmfs_ls(&self, path: &str) -> Result<IpfsLsResponse> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/ls?arg={}&long=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        if parts.status != StatusCode::OK {
            let bs = body.to_bytes().await?;
            return Err(parse_error(parts, bs)?);
        }

        let output: IpfsLsResponse = body.to_json().await?;
        Ok(output)
    }

    async fn ipmfs_mkdir(&self, path: &str) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/mkdir?arg={}&parents=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let req = Request::post(url);
        let req = req
            .body(RequestBody::Empty)
            .map_err(new_request_build_error)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED | StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
    }

    /// Support write from reader.
    pub async fn ipmfs_write(&self, path: &str, body: Bytes) -> Result<()> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!(
            "{}/api/v0/files/write?arg={}&parents=true&create=true&truncate=true",
            self.endpoint,
            percent_encode_path(&p)
        );

        let multipart = Multipart::new().part(FormDataPart::new("data").content(body));

        let req: http::request::Builder = Request::post(url);
        let req = multipart.apply(req)?;

        let (parts, body) = self.client.send(req).await?.into_parts();
        match parts.status {
            StatusCode::CREATED | StatusCode::OK => {
                body.consume().await?;
                Ok(())
            }
            _ => {
                let bs = body.to_bytes().await?;
                Err(parse_error(parts, bs)?)
            }
        }
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

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponseEntry {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Type")]
    pub file_type: i64,
    #[serde(rename = "Size")]
    pub size: u64,
}

impl IpfsLsResponseEntry {
    /// ref: <https://github.com/ipfs/specs/blob/main/UNIXFS.md#data-format>
    ///
    /// ```protobuf
    /// enum DataType {
    ///     Raw = 0;
    ///     Directory = 1;
    ///     File = 2;
    ///     Metadata = 3;
    ///     Symlink = 4;
    ///     HAMTShard = 5;
    /// }
    /// ```
    pub fn mode(&self) -> EntryMode {
        match &self.file_type {
            1 => EntryMode::DIR,
            0 | 2 => EntryMode::FILE,
            _ => EntryMode::Unknown,
        }
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponse {
    #[serde(rename = "Entries")]
    pub entries: Option<Vec<IpfsLsResponseEntry>>,
}
