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

use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use core::fmt::Debug;
use http::Response;
use http::StatusCode;
use log::debug;
use tokio::sync::OnceCell;

use super::core::WebhdfsCore;
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

        let info = AccessorInfo::default();
        info.set_scheme(Scheme::Webhdfs)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,
                stat_has_content_length: true,
                stat_has_last_modified: true,

                read: true,

                write: true,
                write_can_append: true,
                write_can_multi: atomic_write_dir.is_some(),

                create_dir: true,
                delete: true,

                list: true,
                list_has_content_length: true,
                list_has_last_modified: true,

                shared: true,

                ..Default::default()
            });

        let accessor_info = Arc::new(info);
        let core = Arc::new(WebhdfsCore {
            info: accessor_info,
            root,
            endpoint,
            user_name: self.config.user_name,
            auth,
            root_checker: OnceCell::new(),
            atomic_write_dir,
            disable_list_batch: self.config.disable_list_batch,
        });

        Ok(WebhdfsBackend { core })
    }
}

/// Backend for WebHDFS service
#[derive(Debug, Clone)]
pub struct WebhdfsBackend {
    core: Arc<WebhdfsCore>,
}

impl WebhdfsBackend {
    async fn check_root(&self) -> Result<()> {
        let resp = self.core.webhdfs_get_file_status("/").await?;
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
        self.core.info.clone()
    }

    /// Create a file or directory
    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        let req = self.core.webhdfs_create_dir_request(path)?;

        let resp = self.info().http_client().send(req).await?;

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
        self.core
            .root_checker
            .get_or_try_init(|| async { self.check_root().await })
            .await?;

        let resp = self.core.webhdfs_get_file_status(path).await?;
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
        let resp = self.core.webhdfs_read_file(path, args.range()).await?;

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
        let w = WebhdfsWriter::new(self.core.clone(), args.clone(), path.to_string());

        let w = if args.append() {
            WebhdfsWriters::Two(oio::AppendWriter::new(w))
        } else {
            WebhdfsWriters::One(oio::BlockWriter::new(
                self.info().clone(),
                w,
                args.concurrent(),
            ))
        };

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(WebhdfsDeleter::new(self.core.clone())),
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
        let l = WebhdfsLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}
