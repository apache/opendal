// Copyright 2022 Datafuse Labs
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
use std::str;
use std::sync::Arc;

use async_trait::async_trait;
use http::Request;
use http::Response;
use http::StatusCode;
use serde::Deserialize;

use super::dir_stream::DirStream;
use super::error::parse_error;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Backend for IPFS service
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
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Pager = DirStream;
    type BlockingPager = ();

    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Ipmfs)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            )
            .set_hints(AccessorHint::ReadStreamable);

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let resp = match args.mode() {
            ObjectMode::DIR => self.ipmfs_mkdir(path).await?,
            ObjectMode::FILE => self.ipmfs_write(path, AsyncBody::Empty).await?,
            _ => unreachable!(),
        };

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreate::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.ipmfs_read(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let meta = parse_into_object_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let resp = self
            .ipmfs_write(path, AsyncBody::Multipart("data".to_string(), r))
            .await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpWrite::new(args.size()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)));
        }

        let resp = self.ipmfs_stat(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body().bytes().await?;

                let res: IpfsStatResponse =
                    serde_json::from_slice(&bs).map_err(new_json_deserialize_error)?;

                let mode = match res.file_type.as_str() {
                    "file" => ObjectMode::FILE,
                    "directory" => ObjectMode::DIR,
                    _ => ObjectMode::Unknown,
                };

                let mut meta = ObjectMetadata::new(mode);
                meta.set_content_length(res.size);

                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.ipmfs_rm(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Pager)> {
        Ok((
            RpList::default(),
            DirStream::new(Arc::new(self.clone()), &self.root, path),
        ))
    }
}

impl IpmfsBackend {
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
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn ipmfs_read(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let mut url = format!(
            "{}/api/v0/files/read?arg={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        if let Some(offset) = range.offset() {
            write!(url, "&offset={offset}").expect("write into string must succeed")
        }
        if let Some(count) = range.size() {
            write!(url, "&count={count}").expect("write into string must succeed")
        }

        let req = Request::post(url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
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
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
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
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
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
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
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

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send_async(req).await
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
