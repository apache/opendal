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

use core::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use serde::Deserialize;
use tokio::sync::OnceCell;

use super::delete::WebhdfsDeleter;
use super::error::parse_error;
use super::lister::WebhdfsLister;
use super::message::BooleanResp;
use super::message::FileStatusType;
use super::message::FileStatusWrapper;
use super::writer::WebhdfsWriter;
use super::writer::WebhdfsWriters;
use crate::raw::*;
use crate::services::WebhdfsConfig;
use crate::*;

const WEBHDFS_DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9870";

impl Configurator for WebhdfsConfig {
    type Builder = WebhdfsBuilder;
    fn into_builder(self) -> Self::Builder {
        WebhdfsBuilder { config: self }
    }
}

/// [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)'s REST API support.
#[doc = include_str!("docs.md")]
#[derive(Default, Clone)]
pub struct WebhdfsBuilder {
    config: WebhdfsConfig,
}

impl Debug for WebhdfsBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("WebhdfsBuilder");
        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl WebhdfsBuilder {
    /// Set the working directory of this backend
    ///
    /// All operations will happen under this root
    ///
    /// # Note
    ///
    /// The root will be automatically created if not exists.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set the remote address of this backend
    /// default to `http://127.0.0.1:9870`
    ///
    /// Endpoints should be full uri, e.g.
    ///
    /// - `https://webhdfs.example.com:9870`
    /// - `http://192.168.66.88:9870`
    ///
    /// If user inputs endpoint without scheme, we will
    /// prepend `http://` to it.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // trim tailing slash so we can accept `http://127.0.0.1:9870/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }
        self
    }

    /// Set the username of this backend,
    /// used for authentication
    ///
    pub fn user_name(mut self, user_name: &str) -> Self {
        if !user_name.is_empty() {
            self.config.user_name = Some(user_name.to_string());
        }
        self
    }

    /// Set the delegation token of this backend,
    /// used for authentication
    ///
    /// # Note
    /// The builder prefers using delegation token over username.
    /// If both are set, delegation token will be used.
    pub fn delegation(mut self, delegation: &str) -> Self {
        if !delegation.is_empty() {
            self.config.delegation = Some(delegation.to_string());
        }
        self
    }

    /// Disable batch listing
    ///
    /// # Note
    ///
    /// When listing a directory, the backend will default to use batch listing.
    /// If disabled, the backend will list all files/directories in one request.
    pub fn disable_list_batch(mut self) -> Self {
        self.config.disable_list_batch = true;
        self
    }

    /// Set temp dir for atomic write.
    ///
    /// # Notes
    ///
    /// If not set, write multi not support, eg: `.opendal_tmp/`.
    pub fn atomic_write_dir(mut self, dir: &str) -> Self {
        self.config.atomic_write_dir = if dir.is_empty() {
            None
        } else {
            Some(String::from(dir))
        };
        self
    }
}

impl Builder for WebhdfsBuilder {
    const SCHEME: Scheme = Scheme::Webhdfs;
    type Config = WebhdfsConfig;

    /// build the backend
    ///
    /// # Note
    ///
    /// when building backend, the built backend will check if the root directory
    /// exits.
    /// if the directory does not exit, the directory will be automatically created
    fn build(self) -> Result<impl Access> {
        debug!("start building backend: {:?}", self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        // check scheme
        let endpoint = match self.config.endpoint {
            Some(endpoint) => {
                if endpoint.starts_with("http") {
                    endpoint
                } else {
                    format!("http://{endpoint}")
                }
            }
            None => WEBHDFS_DEFAULT_ENDPOINT.to_string(),
        };
        debug!("backend use endpoint {}", endpoint);

        let atomic_write_dir = self.config.atomic_write_dir;

        let auth = self.config.delegation.map(|dt| format!("delegation={dt}"));

        let client = HttpClient::new()?;

        let backend = WebhdfsBackend {
            root,
            endpoint,
            user_name: self.config.user_name,
            auth,
            client,
            root_checker: OnceCell::new(),
            atomic_write_dir,
            disable_list_batch: self.config.disable_list_batch,
        };

        Ok(backend)
    }
}

/// Backend for WebHDFS service
#[derive(Debug, Clone)]
pub struct WebhdfsBackend {
    root: String,
    endpoint: String,
    user_name: Option<String>,
    auth: Option<String>,
    root_checker: OnceCell<()>,

    pub atomic_write_dir: Option<String>,
    pub disable_list_batch: bool,
    pub client: HttpClient,
}

impl WebhdfsBackend {
    pub fn webhdfs_create_dir_request(&self, path: &str) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=MKDIRS&overwrite=true&noredirect=true",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::put(&url);

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    /// create object
    pub async fn webhdfs_create_object_request(
        &self,
        path: &str,
        size: Option<u64>,
        args: &OpWrite,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=CREATE&overwrite=true&noredirect=true",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::put(&url);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = self.client.send(req).await?;

        let status = resp.status();

        if status != StatusCode::CREATED && status != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();

        let resp: LocationResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let mut req = Request::put(&resp.location);

        if let Some(size) = size {
            req = req.header(CONTENT_LENGTH, size);
        };

        if let Some(content_type) = args.content_type() {
            req = req.header(CONTENT_TYPE, content_type);
        };
        let req = req.body(body).map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn webhdfs_init_append_request(&self, path: &str) -> Result<String> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=APPEND&noredirect=true",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let req = Request::post(url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        let resp = self.client.send(req).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();
                let resp: LocationResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

                Ok(resp.location)
            }
            _ => Err(parse_error(resp)),
        }
    }

    pub async fn webhdfs_rename_object(&self, from: &str, to: &str) -> Result<Response<Buffer>> {
        let from = build_abs_path(&self.root, from);
        let to = build_rooted_abs_path(&self.root, to);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=RENAME&destination={}",
            self.endpoint,
            percent_encode_path(&from),
            percent_encode_path(&to)
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let req = Request::put(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub fn webhdfs_append_request(
        &self,
        location: &str,
        size: u64,
        body: Buffer,
    ) -> Result<Request<Buffer>> {
        let mut url = location.to_string();
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let mut req = Request::post(&url);

        req = req.header(CONTENT_LENGTH, size.to_string());

        req.body(body).map_err(new_request_build_error)
    }

    /// CONCAT will concat sources to the path
    pub fn webhdfs_concat_request(
        &self,
        path: &str,
        sources: Vec<String>,
    ) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let sources = sources
            .iter()
            .map(|p| build_rooted_abs_path(&self.root, p))
            .collect::<Vec<String>>()
            .join(",");

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=CONCAT&sources={}",
            self.endpoint,
            percent_encode_path(&p),
            percent_encode_path(&sources),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        let req = Request::post(url);

        req.body(Buffer::new()).map_err(new_request_build_error)
    }

    fn webhdfs_open_request(&self, path: &str, range: &BytesRange) -> Result<Request<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=OPEN",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        if !range.is_full() {
            url += &format!("&offset={}", range.offset());
            if let Some(size) = range.size() {
                url += &format!("&length={size}")
            }
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    pub async fn webhdfs_list_status_request(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=LISTSTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.client.send(req).await
    }

    pub async fn webhdfs_list_status_batch_request(
        &self,
        path: &str,
        start_after: &str,
    ) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);

        let mut url = format!(
            "{}/webhdfs/v1/{}?op=LISTSTATUS_BATCH",
            self.endpoint,
            percent_encode_path(&p),
        );
        if !start_after.is_empty() {
            url += format!("&startAfter={}", start_after).as_str();
        }
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;
        self.client.send(req).await
    }

    pub async fn webhdfs_read_file(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<HttpBody>> {
        let req = self.webhdfs_open_request(path, &range)?;
        self.client.fetch(req).await
    }

    pub(super) async fn webhdfs_get_file_status(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=GETFILESTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    pub async fn webhdfs_delete(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=DELETE&recursive=false",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(user) = &self.user_name {
            url += format!("&user.name={user}").as_str();
        }
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::delete(&url)
            .body(Buffer::new())
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn check_root(&self) -> Result<()> {
        let resp = self.webhdfs_get_file_status("/").await?;
        match resp.status() {
            StatusCode::OK => {
                let bs = resp.into_body();

                let file_status = serde_json::from_reader::<_, FileStatusWrapper>(bs.reader())
                    .map_err(new_json_deserialize_error)?
                    .file_status;

                if file_status.ty == FileStatusType::File {
                    return Err(Error::new(
                        ErrorKind::ConfigInvalid,
                        "root path must be dir",
                    ));
                }
            }
            StatusCode::NOT_FOUND => {
                self.create_dir("/", OpCreateDir::new()).await?;
            }
            _ => return Err(parse_error(resp)),
        }
        Ok(())
    }
}

impl Access for WebhdfsBackend {
    type Reader = HttpBody;
    type Writer = WebhdfsWriters;
    type Lister = oio::PageLister<WebhdfsLister>;
    type Deleter = oio::OneShotDeleter<WebhdfsDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Webhdfs)
            .set_root(&self.root)
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_last_modified: true,

                read: true,

                write: true,
                write_can_append: true,
                write_can_multi: self.atomic_write_dir.is_some(),

                create_dir: true,
                delete: true,

                list: true,
                list_has_content_length: true,
                list_has_last_modified: true,

                shared: true,

                ..Default::default()
            });
        am.into()
    }

    /// Create a file or directory
    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let req = self.webhdfs_create_dir_request(path)?;

        let resp = self.client.send(req).await?;

        let status = resp.status();

        // WebHDFS's has a two-step create/append to prevent clients to send out
        // data before creating it.
        // According to the redirect policy of `reqwest` HTTP Client we are using,
        // the redirection should be done automatically.
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                let bs = resp.into_body();

                let resp = serde_json::from_reader::<_, BooleanResp>(bs.reader())
                    .map_err(new_json_deserialize_error)?;

                if resp.boolean {
                    Ok(RpCreateDir::default())
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "webhdfs create dir failed",
                    ))
                }
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // if root exists and is a directory, stat will be ok
        self.root_checker
            .get_or_try_init(|| async { self.check_root().await })
            .await?;

        let resp = self.webhdfs_get_file_status(path).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let file_status = serde_json::from_reader::<_, FileStatusWrapper>(bs.reader())
                    .map_err(new_json_deserialize_error)?
                    .file_status;

                let meta = match file_status.ty {
                    FileStatusType::Directory => Metadata::new(EntryMode::DIR),
                    FileStatusType::File => Metadata::new(EntryMode::FILE)
                        .with_content_length(file_status.length)
                        .with_last_modified(parse_datetime_from_from_timestamp_millis(
                            file_status.modification_time,
                        )?),
                };

                Ok(RpStat::new(meta))
            }

            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.webhdfs_read_file(path, args.range()).await?;

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

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let w = WebhdfsWriter::new(self.clone(), args.clone(), path.to_string());

        let w = if args.append() {
            WebhdfsWriters::Two(oio::AppendWriter::new(w))
        } else {
            WebhdfsWriters::One(oio::BlockWriter::new(
                w,
                args.executor().cloned(),
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(WebhdfsDeleter::new(Arc::new(self.clone()))),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        if args.recursive() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "WebHDFS doesn't support list with recursive",
            ));
        }

        let path = path.trim_end_matches('/');
        let l = WebhdfsLister::new(self.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(super) struct LocationResponse {
    pub location: String,
}
