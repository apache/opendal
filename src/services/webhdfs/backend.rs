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
use bytes::Buf;
use http::header::CONTENT_LENGTH;
use http::header::CONTENT_TYPE;
use http::response::Parts;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use log::error;
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
        let mut ds = f.debug_struct("Builder");
        ds.field("root", &self.root)
            .field("endpoint", &self.endpoint);
        if self.delegation.is_some() {
            ds.field("delegation", &"<redacted>");
        }
        ds.finish()
    }
}

impl WebhdfsBuilder {
    /// Set the working directory of this backend
    ///
    /// All operations will happen under this root
    /// # Note
    /// The root will be automatically created if not exists.
    /// If the root is occupied by a file, building of directory will fail
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // trim tailing slash so we can accept `http://127.0.0.1:9870/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }
        self
    }

    /// Set the delegation token of this backend,
    /// used for authentication
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

impl WebhdfsBuilder {
    fn auth_str(&mut self) -> Option<String> {
        if let Some(dt) = self.delegation.take() {
            return Some(format!("delegation_token={dt}"));
        }
        None
    }
}

impl Builder for WebhdfsBuilder {
    const SCHEME: Scheme = Scheme::Webhdfs;
    type Accessor = WebhdfsBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = WebhdfsBuilder::default();

        for (k, v) in map.iter() {
            let v = v.as_str();
            match k.as_str() {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                "delegation" => builder.delegation(v),
                _ => continue,
            };
        }

        builder
    }

    /// build the backend
    ///
    /// # Note:
    /// when building backend, the built backend will check if the root directory
    /// exits.
    /// if the directory does not exits, the directory will be automatically created
    /// if the root path is occupied by a file, a failure will be returned
    fn build(&mut self) -> Result<Self::Accessor> {
        debug!("building backend: {:?}", self);

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

        let auth = self.auth_str();
        let client = HttpClient::new()?;

        let backend = WebhdfsBackend {
            root: root.clone(),
            endpoint,
            auth,
            client,
            root_checker: OnceCell::new(),
        };

        debug!("checking working directory: {}", root);

        Ok(backend)
    }
}

/// Backend for WebHDFS service
#[derive(Debug, Clone)]
pub struct WebhdfsBackend {
    root: String,
    endpoint: String,
    pub client: HttpClient,
    auth: Option<String>,
    root_checker: OnceCell<()>,
}

impl WebhdfsBackend {
    // create object or make a directory
    pub async fn webhdfs_create_object_req(
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

        let resp = self.client.send_async(req).await?;

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

    async fn webhdfs_open_req(&self, path: &str, range: &BytesRange) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=OPEN",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        // make a Webhdfs compatible bytes range
        //
        // Webhdfs does not support read from end
        // have to solve manually
        let range = match (range.offset(), range.size()) {
            // avoiding reading the whole file
            (None, Some(size)) => {
                debug!("converting bytes range to webhdfs compatible");
                let status = self.stat(path, OpStat::default()).await?;
                let total_size = status.into_metadata().content_length();
                let offset = total_size - size;
                BytesRange::new(Some(offset), Some(size))
            }
            _ => *range,
        };

        let (offset, size) = (range.offset(), range.size());

        match (offset, size) {
            (Some(offset), Some(size)) => {
                url += format!("&offset={offset}&length={size}").as_str();
            }
            (Some(offset), None) => {
                url += format!("&offset={offset}").as_str();
            }
            (None, None) => {
                // read all, do nothing
            }
            (None, Some(_)) => {
                // already handled
                unreachable!()
            }
        }

        let req = Request::get(&url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        Ok(req)
    }

    fn webhdfs_list_status_req(&self, path: &str) -> Result<Request<AsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=LISTSTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        Ok(req)
    }
}

impl WebhdfsBackend {
    /// get object from webhdfs
    ///
    /// # Notes
    ///
    /// looks like webhdfs doesn't support range request from file end.
    /// so if we want to read the tail of object, the whole object should be transferred.
    async fn webhdfs_get_object(
        &self,
        path: &str,
        range: BytesRange,
    ) -> Result<Response<IncomingAsyncBody>> {
        let req = self.webhdfs_open_req(path, &range).await?;
        let resp = self.client.send_async(req).await?;

        // this should be a 307 redirect
        if resp.status() != StatusCode::TEMPORARY_REDIRECT {
            return Err(parse_error(resp).await?);
        }

        let re_url = self.follow_redirect(resp)?;
        let re_req = Request::get(&re_url)
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;
        self.client.send_async(re_req).await
    }

    async fn webhdfs_status_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=GETFILESTATUS",
            self.endpoint,
            percent_encode_path(&p),
        );
        debug!("webhdfs status url: {}", url);
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::get(&url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }

    async fn webhdfs_delete_object(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_abs_path(&self.root, path);
        let mut url = format!(
            "{}/webhdfs/v1/{}?op=DELETE&recursive=false",
            self.endpoint,
            percent_encode_path(&p),
        );
        if let Some(auth) = &self.auth {
            url += format!("&{auth}").as_str();
        }

        let req = Request::delete(&url);
        let req = req
            .body(AsyncBody::Empty)
            .map_err(new_request_build_error)?;

        self.client.send_async(req).await
    }
}

impl WebhdfsBackend {
    /// get redirect destination from 307 TEMPORARY_REDIRECT http response
    fn follow_redirect(&self, resp: Response<IncomingAsyncBody>) -> Result<String> {
        let loc = match parse_location(resp.headers())? {
            Some(p) => {
                if !p.starts_with('/') {
                    // is not relative path
                    p.to_string()
                } else {
                    // is relative path
                    // prefix with endpoint url
                    let url = self.endpoint.clone();
                    format!("{url}/{p}")
                }
            }
            None => {
                let err = Error::new(
                    ErrorKind::Unexpected,
                    "redirection fail: no location header",
                );
                return Err(err);
            }
        };
        Ok(loc)
    }

    fn consume_success_mkdir(&self, path: &str, parts: Parts, body: &str) -> Result<RpCreate> {
        let mkdir_rsp = serde_json::from_str::<BooleanResp>(body).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "cannot parse mkdir response")
                .set_temporary()
                .with_context("service", Scheme::Webhdfs)
                .with_context("response", format!("{parts:?}"))
                .set_source(e)
        })?;

        if mkdir_rsp.boolean {
            Ok(RpCreate::default())
        } else {
            Err(Error::new(
                ErrorKind::Unexpected,
                &format!("mkdir failed: {path}"),
            ))
        }
    }

    async fn check_root(&self) -> Result<()> {
        let resp = self.webhdfs_status_object("/").await?;
        match resp.status() {
            StatusCode::OK => {
                let body_bs = resp.into_body().bytes().await?;

                let file_status = serde_json::from_reader::<_, FileStatusWrapper>(body_bs.reader())
                    .map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "cannot parse returned json")
                            .with_context("service", Scheme::Webhdfs)
                            .set_source(e)
                    })?
                    .file_status;

                match file_status.ty {
                    FileStatusType::File => {
                        error!("working directory is occupied!");
                        return Err(Error::new(ErrorKind::ConfigInvalid, "root is occupied!")
                            .with_context("service", Scheme::Webhdfs));
                    }
                    FileStatusType::Directory => {
                        debug!("working directory exists, do nothing");
                    }
                }
            }

            StatusCode::NOT_FOUND => {
                debug!("working directory does not exists, creating...");
                self.create("/", OpCreate::new(EntryMode::DIR)).await?;
            }

            _ => return Err(parse_error(resp).await?),
        }
        debug!("working directory is ready!");
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
            .set_capabilities(
                AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::List,
            )
            .set_hints(AccessorHint::ReadStreamable);
        am
    }

    /// Create a file or directory
    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        // if the path ends with '/', it will be treated as a directory
        // otherwise, it will be treated as a file
        let path = if args.mode().is_file() && path.ends_with('/') {
            path.trim_end_matches('/').to_owned()
        } else if args.mode().is_dir() && !path.ends_with('/') {
            path.to_owned() + "/"
        } else {
            path.to_owned()
        };

        let req = self
            .webhdfs_create_object_req(&path, Some(0), None, AsyncBody::Empty)
            .await?;

        let resp = self.client.send_async(req).await?;

        let status = resp.status();

        // WebHDFS's has a two-step create/append to prevent clients to send out
        // data before creating it.
        // According to the redirect policy of `reqwest` HTTP Client we are using,
        // the redirection should be done automatically.
        match status {
            StatusCode::CREATED | StatusCode::OK => {
                if !path.ends_with('/') {
                    // create file's http resp could be ignored
                    resp.into_body().consume().await?;
                    return Ok(RpCreate::default());
                }
                let (parts, body) = resp.into_parts();
                let bs = body.bytes().await?;
                let s = String::from_utf8_lossy(&bs);
                self.consume_success_mkdir(&path, parts, &s)
            }
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let range = args.range();
        let resp = self.webhdfs_get_object(path, range).await?;
        match resp.status() {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => {
                let meta = parse_into_metadata(path, resp.headers())?;
                Ok((RpRead::with_metadata(meta), resp.into_body()))
            }
            StatusCode::NOT_FOUND => Err(Error::new(ErrorKind::NotFound, "object not found")
                .with_context("service", Scheme::Webhdfs)),
            _ => Err(parse_error(resp).await?),
        }
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.append() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "append write is not supported",
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

        let resp = self.webhdfs_status_object(path).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                debug!("stat object: {} ok", path);
                let mut meta = parse_into_metadata(path, resp.headers())?;
                let body_bs = resp.into_body().bytes().await?;

                let file_status = serde_json::from_reader::<_, FileStatusWrapper>(body_bs.reader())
                    .map_err(|e| {
                        Error::new(ErrorKind::Unexpected, "cannot parse returned json")
                            .with_context("service", Scheme::Webhdfs)
                            .set_source(e)
                    })?
                    .file_status;
                debug!("file status: {:?}", file_status);
                let status_meta: Metadata = file_status.try_into()?;

                // is ok to unwrap here
                // all metadata field of status meta is present and checked by `TryFrom`
                meta.set_last_modified(status_meta.last_modified().unwrap())
                    .set_content_length(status_meta.content_length());
                Ok(RpStat::new(meta))
            }

            _ => Err(parse_error(resp).await?),
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let resp = self.webhdfs_delete_object(path).await?;
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
        let req = self.webhdfs_list_status_req(path)?;

        let resp = self.client.send_async(req).await?;
        match resp.status() {
            StatusCode::OK => {
                let body_bs = resp.into_body().bytes().await?;
                let file_statuses =
                    serde_json::from_reader::<_, FileStatusesWrapper>(body_bs.reader())
                        .map_err(|e| {
                            Error::new(ErrorKind::Unexpected, "cannot parse returned json")
                                .with_context("service", Scheme::Webhdfs)
                                .set_source(e)
                        })?
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
