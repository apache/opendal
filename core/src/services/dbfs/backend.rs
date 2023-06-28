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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use http::header;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use serde::Deserialize;
use serde_json::json;

use super::error::parse_dbfs_read_error;
use super::error::parse_error;
use super::pager::DbfsPager;
use super::reader::IncomingDbfsAsyncBody;
use super::writer::DbfsWriter;
use crate::raw::*;
use crate::*;

/// [Dbfs](https://docs.databricks.com/api/azure/workspace/dbfs)'s REST API support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct DbfsBuilder {
    root: Option<String>,
    endpoint: String,
    token: Option<String>,
}

impl Debug for DbfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);

        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

impl DbfsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    ///
    /// - Azure: `https://adb-1234567890123456.78.azuredatabricks.net`
    /// - Aws: `https://dbc-123a5678-90bc.cloud.databricks.com`
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        assert!(!endpoint.is_empty());
        self.endpoint = endpoint.trim_end_matches('/').to_string();
        self
    }

    /// Set the token of this backend.
    pub fn token(&mut self, token: &str) -> &mut Self {
        if !token.is_empty() {
            self.token = Some(token.to_string());
        }
        self
    }
}

impl Builder for DbfsBuilder {
    const SCHEME: Scheme = Scheme::Dbfs;
    type Accessor = DbfsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = DbfsBuilder::default();

        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("token").map(|v| builder.token(v));

        builder
    }

    /// Build a DbfsBackend.
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = match self.endpoint.is_empty() {
            false => Ok(&self.endpoint),
            true => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Dbfs)),
        }?;
        debug!("backend use endpoint: {}", &endpoint);

        let token = match self.token.take() {
            Some(token) => token,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "missing token for Dbfs",
                ));
            }
        };

        let client = HttpClient::new()?;

        debug!("backend build finished: {:?}", &self);
        Ok(DbfsBackend {
            root,
            endpoint: self.endpoint.clone(),
            token,
            client,
        })
    }
}

/// Backend for DBFS service
#[derive(Debug, Clone)]
pub struct DbfsBackend {
    root: String,
    endpoint: String,
    token: String,
    pub(super) client: HttpClient,
}

impl DbfsBackend {
    fn dbfs_create_dir_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let url = format!("{}/api/2.0/dbfs/mkdirs", self.endpoint);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let p = build_rooted_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let req_body = format!("{{\"path\": \"{}\"}}", percent_encode_path(&p));
        let body = AsyncBody::Bytes(Bytes::from(req_body));

        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    async fn dbfs_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let url = format!("{}/api/2.0/dbfs/delete", self.endpoint);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let p = build_rooted_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let request_body = &json!({
            "path": percent_encode_path(&p),
            // TODO: support recursive toggle, should we add a new field in OpDelete?
            "recursive": true,
        });

        let body = AsyncBody::Bytes(Bytes::from(request_body.to_string()));

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn dbfs_rename(&self, from: &str, to: &str) -> Result<Response<IncomingAsyncBody>> {
        let source = build_rooted_abs_path(&self.root, from);
        let target = build_rooted_abs_path(&self.root, to);

        let url = format!("{}/api/2.0/dbfs/move", self.endpoint);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req_body = &json!({
            "source_path": percent_encode_path(&source),
            "destination_path": percent_encode_path(&target),
        });

        let body = AsyncBody::Bytes(Bytes::from(req_body.to_string()));

        let req = req.body(body).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn dbfs_list(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/2.0/dbfs/list?path={}",
            self.endpoint,
            percent_encode_path(&p)
        );
        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub fn dbfs_create_file_request(&self, path: &str, body: Bytes) -> Result<Request<AsyncBody>> {
        let url = format!("{}/api/2.0/dbfs/put", self.endpoint);

        let contents = BASE64_STANDARD.encode(body);
        let mut req = Request::post(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req_body = &json!({
            "path": path,
            "contents": contents,
            // TODO: support overwrite toggle, should we add a new field in OpWrite?
            "overwrite": true,
        });

        let body = AsyncBody::Bytes(Bytes::from(req_body.to_string()));

        req.body(body).map_err(new_request_build_error)
    }

    async fn dbfs_read(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingDbfsAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let mut url = format!(
            "{}/api/2.0/dbfs/read?path={}",
            self.endpoint,
            percent_encode_path(&p)
        );

        if let Some(offset) = range.offset() {
            url.push_str(&format!("&offset={}", offset));
        }

        if let Some(length) = range.size() {
            url.push_str(&format!("&length={}", length));
        }

        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_dbfs(req).await
    }

    async fn dbfs_get_properties(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path)
            .trim_end_matches('/')
            .to_string();

        let url = format!(
            "{}/api/2.0/dbfs/get-status?path={}",
            &self.endpoint,
            percent_encode_path(&p)
        );

        let mut req = Request::get(&url);

        let auth_header_content = format!("Bearer {}", self.token);
        req = req.header(header::AUTHORIZATION, auth_header_content);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn dbfs_ensure_parent_path(&self, path: &str) -> Result<()> {
        let resp = self.dbfs_get_properties(path).await?;

        match resp.status() {
            StatusCode::OK => return Ok(()),
            StatusCode::NOT_FOUND => {
                self.create_dir(path, OpCreateDir::default()).await?;
            }
            _ => return Err(parse_error(resp).await?),
        }
        Ok(())
    }
}

#[async_trait]
impl Accessor for DbfsBackend {
    type Reader = IncomingDbfsAsyncBody;
    type BlockingReader = ();
    type Writer = oio::OneShotWriter<DbfsWriter>;
    type BlockingWriter = ();
    type Pager = DbfsPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Dbfs)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,

                read: true,
                read_can_next: true,
                read_with_range: true,

                write: true,
                create_dir: true,
                delete: true,
                rename: true,

                list: true,
                list_with_delimiter_slash: true,

                ..Default::default()
            });
        am
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let req = self.dbfs_create_dir_request(path)?;

        let resp = self.client.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::CREATED | StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpCreateDir::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.dbfs_read(path, args.range()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                // NOTE: If range is not specified, we need to get content length from stat API.
                if let Some(size) = args.range().size() {
                    let mut meta = parse_into_metadata(path, resp.headers())?;
                    meta.set_content_length(size);
                    Ok((RpRead::with_metadata(meta), resp.into_body()))
                } else {
                    let stat_resp = self.dbfs_get_properties(path).await?;
                    let meta = match stat_resp.status() {
                        StatusCode::OK => {
                            let mut meta = parse_into_metadata(path, stat_resp.headers())?;
                            let bs = stat_resp.into_body().bytes().await?;
                            let decoded_response = serde_json::from_slice::<DbfsStatus>(&bs)
                                .map_err(new_json_deserialize_error)?;
                            meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
                                decoded_response.modification_time,
                            )?);
                            match decoded_response.is_dir {
                                true => meta.set_mode(EntryMode::DIR),
                                false => {
                                    meta.set_mode(EntryMode::FILE);
                                    meta.set_content_length(decoded_response.file_size as u64)
                                }
                            };
                            meta
                        }
                        _ => return Err(parse_error(stat_resp).await?),
                    };
                    Ok((RpRead::with_metadata(meta), resp.into_body()))
                }
            }
            _ => Err(parse_dbfs_read_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        Ok((
            RpWrite::default(),
            oio::OneShotWriter::new(DbfsWriter::new(self.clone(), args, path.to_string())),
        ))
    }

    async fn rename(&self, from: &str, to: &str, _args: OpRename) -> Result<RpRename> {
        self.dbfs_ensure_parent_path(to).await?;

        let resp = self.dbfs_rename(from, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpRename::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.dbfs_get_properties(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut meta = parse_into_metadata(path, resp.headers())?;
                let bs = resp.into_body().bytes().await?;
                let decoded_response = serde_json::from_slice::<DbfsStatus>(&bs)
                    .map_err(new_json_deserialize_error)?;
                meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
                    decoded_response.modification_time,
                )?);
                match decoded_response.is_dir {
                    true => meta.set_mode(EntryMode::DIR),
                    false => {
                        meta.set_mode(EntryMode::FILE);
                        meta.set_content_length(decoded_response.file_size as u64)
                    }
                };
                Ok(RpStat::new(meta))
            }
            StatusCode::NOT_FOUND if path.ends_with('/') => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    /// NOTE: Server will return 200 even if the path doesn't exist.
    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.dbfs_delete(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpDelete::default()),
            _ => {
                let err = parse_error(resp).await?;
                match err.kind() {
                    ErrorKind::NotFound => Ok(RpDelete::default()),
                    _ => Err(err),
                }
            }
        }
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Pager)> {
        let op = DbfsPager::new(self.clone(), path.to_string());

        Ok((RpList::default(), op))
    }
}

#[derive(Deserialize)]
struct DbfsStatus {
    // Not used fields.
    // path: String,
    is_dir: bool,
    file_size: i64,
    modification_time: i64,
}
