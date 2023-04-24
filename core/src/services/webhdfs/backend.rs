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
use std::collections::HashMap;

use async_trait::async_trait;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use tokio::sync::OnceCell;

use super::error::parse_error;
use super::message::BooleanResp;
use super::message::FileStatusType;
use super::message::FileStatusWrapper;
use super::message::FileStatusesWrapper;
use super::pager::WebhdfsPager;
use super::writer::WebhdfsWriter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

const WEBHDFS_DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9870";

/// [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)'s REST API support.
///
/// There two implementations of WebHDFS REST API:
///
/// - Native via HDFS Namenode and Datanode, data are transferred between nodes directly.
/// - [HttpFS](https://hadoop.apache.org/docs/stable/hadoop-hdfs-httpfs/index.html) is a gateway before hdfs nodes, data are proxied.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [ ] ~~scan~~
/// - [ ] ~~presign~~
/// - [ ] blocking
///
/// # Differences with hdfs
///
/// [Hdfs][crate::services::Hdfs] is powered by HDFS's native java client. Users need to setup the hdfs services correctly. But webhdfs can access from HTTP API and no extra setup needed.
///
/// # Configurations
///
/// - `root`: The root path of the WebHDFS service.
/// - `endpoint`: The endpoint of the WebHDFS service.
/// - `delegation`: The delegation token for WebHDFS.
///
/// Refer to [`Builder`]'s public API docs for more information
///
/// # Examples
///
/// ## Via Builder
///
/// ```no_run
/// use std::sync::Arc;
///
/// use anyhow::Result;
/// use opendal::services::Webhdfs;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     let mut builder = Webhdfs::default();
///     // set the root for WebHDFS, all operations will happen under this root
///     //
///     // Note:
///     // if the root is not exists, the builder will automatically create the
///     // root directory for you
///     // if the root exists and is a directory, the builder will continue working
///     // if the root exists and is a folder, the builder will fail on building backend
///     builder.root("/path/to/dir");
///     // set the endpoint of webhdfs namenode, controlled by dfs.namenode.http-address
///     // default is http://127.0.0.1:9870
///     builder.endpoint("http://127.0.0.1:9870");
///     // set the delegation_token for builder
///     builder.delegation("delegation_token");
///
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
#[derive(Default, Clone)]
pub struct WebhdfsBuilder {
    root: Option<String>,
    endpoint: Option<String>,
    delegation: Option<String>,
}

impl Debug for WebhdfsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
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
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // trim tailing slash so we can accept `http://127.0.0.1:9870/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }
        self
    }

    /// Set the delegation token of this backend,
    /// used for authentication
    ///
    /// # Note
    /// The builder prefers using delegation token over username.
    /// If both are set, delegation token will be used.
    pub fn delegation(&mut self, delegation: &str) -> &mut Self {
        if !delegation.is_empty() {
            self.delegation = Some(delegation.to_string());
        }
        self
    }
}

impl Builder for WebhdfsBuilder {
    const SCHEME: Scheme = Scheme::Webhdfs;
    type Accessor = WebhdfsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = WebhdfsBuilder::default();

        map.get("root").map(|v| builder.root(v));
        map.get("endpoint").map(|v| builder.endpoint(v));
        map.get("delegation").map(|v| builder.delegation(v));

        builder
    }

    /// build the backend
    ///
    /// # Note
    ///
    /// when building backend, the built backend will check if the root directory
    /// exits.
    /// if the directory does not exits, the directory will be automatically created
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("start building backend: {:?}", self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {root}");

        // check scheme
        let endpoint = match self.endpoint.take() {
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

        let auth = self
            .delegation
            .take()
            .map(|dt| format!("delegation_token={dt}"));

        let client = HttpClient::new()?;

        let backend = WebhdfsBackend {
            root,
            endpoint,
            auth,
            client,
            root_checker: OnceCell::new(),
        };

        Ok(backend)
    }
}

/// Backend for WebHDFS service
#[derive(Debug, Clone)]
pub struct WebhdfsBackend {
    root: String,
    endpoint: String,
    auth: Option<String>,
    root_checker: OnceCell<()>,

    pub client: HttpClient,
}

impl WebhdfsBackend {
    /// create object or make a directory
    ///
    /// TODO: we should split it into mkdir and create
    pub async fn webhdfs_create_object_request(
        &self,
        path: &str,
        size: Option<usize>,
        content_type: Option<&str>,
        body: AsyncBody,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let op = if path.ends_with('/') {
            "MKDIRS"
        } else {
            "CREATE"
        };
        let mut url = format!(
            "{}/webhdfs/v1/{}?op={}&overwrite=true",
            self.endpoint,
            percent_encode_path(&p),
            op,
        );
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::put(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        // mkdir does not redirect
        if path.ends_with('/') {
            return Ok(req);
        }

        let resp = self.client.send(req).await?;

        // should be a 307 TEMPORARY_REDIRECT
        if resp.status() != StatusCode::TEMPORARY_REDIRECT {
            return Err(parse_error(resp).await?);
        }
        let re_url = self.follow_redirect(resp)?;

        let mut re_builder = Request::put(re_url);
        if let Some(size) = size {
            re_builder = re_builder.header(CONTENT_LENGTH, size.to_string());
        }
        if let Some(content_type) = content_type {
            re_builder = re_builder.header(CONTENT_TYPE, content_type);
        }

        re_builder.body(body).map_err(new_request_build_error)
    }

    async fn webhdfs_open_request(
        &self,
        path: &str,
        range: &BytesRange,
    ) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=OPEN",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(auth) = &self.auth {
            url += &format!("&{auth}");
        }

        if !range.is_full() {
            // Webhdfs does not support read from end
            if range.offset().is_none() && range.size().is_some() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "webhdfs doesn't support read with suffix range",
                ));
            };

            if let Some(offset) = range.offset() {
                url += &format!("&offset={offset}");
            }
            if let Some(size) = range.size() {
                url += &format!("&length={size}")
            }
        }

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn webhdfs_list_status_request(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=LISTSTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        Ok(req)
    }

    async fn webhdfs_read_file(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let req = self.webhdfs_open_request(path, &range).await?;
        let resp = self.client.send(req).await?;

        // webhdfs namenode will redirect us to datanode for data transfer.
        if resp.status() != StatusCode::TEMPORARY_REDIRECT {
            return Err(parse_error(resp).await?);
        }

        let location = self.follow_redirect(resp)?;
        let req = Request::get(&location)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        self.client.send(req).await
    }

    async fn webhdfs_get_file_status(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=GETFILESTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );

        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn webhdfs_delete(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=DELETE&recursive=false",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::delete(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    /// get redirect destination from 307 TEMPORARY_REDIRECT http response
    fn follow_redirect(&self, resp: Response<IncomingAsyncBody>) -> Result<String> {
        let location = parse_location(resp.headers())?.ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "webhdfs expect to have redirect location but got none",
            )
        })?;

        let location = if location.starts_with('/') {
            // location starts with `/` means it's a relative path to current
            // endpoint. We should prepend the endpoint to it so that we can
            // send request to the correct location.
            format!("{}/{location}", self.endpoint)
        } else {
            location.to_string()
        };

        Ok(location)
    }

    async fn check_root(&self) -> Result<()> {
        let resp = self.webhdfs_get_file_status("/").await?;
        match resp.status() {
            StatusCode::OK => {
                let bs = resp.into_body().bytes().await?;

                let file_status = serde_json::from_slice::<FileStatusWrapper>(&bs)
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
                self.create_dir("/", OpCreate::new()).await?;
            }
            _ => return Err(parse_error(resp).await?),
        }
        Ok(())
    }
}

#[async_trait]
impl Accessor for WebhdfsBackend {
    type Reader = IncomingAsyncBody;
    type BlockingReader = ();
    type Writer = WebhdfsWriter;
    type BlockingWriter = ();
    type Pager = WebhdfsPager;
    type BlockingPager = ();

    fn info(&self) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_scheme(Scheme::Webhdfs)
            .set_root(&self.root)
            .set_capability(Capability {
                read: true,
                read_can_next: true,
                write: true,
                list: true,
                ..Default::default()
            });
        am
    }

    /// Create a file or directory
    async fn create_dir(&self, path: &str, _: OpCreate) -> Result<RpCreate> {
        let req = self
            .webhdfs_create_object_request(path, Some(0), None, AsyncBody::Empty)
            .await?;

        let resp = self.client.send(req).await?;

        let status = resp.status();

        // WebHDFS's has a two-step create/append to prevent clients to send out
        // data before creating it.
        // According to the redirect policy of `reqwest` HTTP Client we are using,
        // the redirection should be done automatically.
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                let bs = resp.into_body().bytes().await?;

                let resp = serde_json::from_slice::<BooleanResp>(&bs)
                    .map_err(new_json_deserialize_error)?;

                if resp.boolean {
                    Ok(RpCreate::default())
                } else {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "webhdfs create dir failed",
                    ))
                }
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let range = args.range();
        let resp = self.webhdfs_read_file(path, range).await?;
        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.content_length().is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "write without content length is not supported",
            ));
        }

        Ok((
            RpWrite::default(),
            WebhdfsWriter::new(self.clone(), args, path.to_string()),
        ))
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
                let bs = resp.into_body().bytes().await?;

                let file_status = serde_json::from_slice::<FileStatusWrapper>(&bs)
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

            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.webhdfs_delete(path).await?;

        match resp.status() {
            StatusCode::OK => {
                resp.into_body().consume().await?;
                Ok(RpDelete::default())
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Pager)> {
        let path = path.trim_end_matches('/');
        let req = self.webhdfs_list_status_request(path)?;

        let resp = self.client.send(req).await?;
        match resp.status() {
            StatusCode::OK => {
                let bs = resp.into_body().bytes().await?;
                let file_statuses = serde_json::from_slice::<FileStatusesWrapper>(&bs)
                    .map_err(new_json_deserialize_error)?
                    .file_statuses
                    .file_status;

                let objects = WebhdfsPager::new(path, file_statuses);
                Ok((RpList::default(), objects))
            }
            StatusCode::NOT_FOUND => {
                let objects = WebhdfsPager::new(path, vec![]);
                Ok((RpList::default(), objects))
            }
            _ => Err(parse_error(resp).await?),
        }
    }
}
