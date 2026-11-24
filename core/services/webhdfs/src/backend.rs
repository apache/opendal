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

use std::fmt::Debug;
use std::sync::Arc;

use asyncband::once::OnceCell;
use bytes::Buf;
use http::StatusCode;
use log::debug;

use super::WEBHDFS_SCHEME;
use super::config::WebhdfsConfig;
use super::core::parse_error;
use super::core::{ErrorContext, WebhdfsCore};
use super::deleter::WebhdfsDeleter;
use super::lister::WebhdfsLister;
use super::message::BooleanResp;
use super::message::FileStatusType;
use super::message::FileStatusWrapper;
use super::reader::*;
use super::writer::WebhdfsWriter;
use super::writer::WebhdfsWriters;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

const WEBHDFS_DEFAULT_ENDPOINT: &str = "http://127.0.0.1:9870";

/// [WebHDFS](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)'s REST API support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct WebhdfsBuilder {
    pub(super) config: WebhdfsConfig,
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
            self.config.endpoint = Some(endpoint.to_string());
        }
        self
    }

    /// Set the username of this backend,
    /// used for authentication
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
    type Config = WebhdfsConfig;

    /// build the backend
    ///
    /// # Note
    ///
    /// when building backend, the built backend will check if the root directory
    /// exits.
    /// if the directory does not exit, the directory will be automatically created
    fn build(self) -> Result<impl Service> {
        debug!("start building backend: {self:?}");

        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let endpoint = normalize_endpoint(self.config.endpoint)?;
        debug!("backend use endpoint {endpoint}");

        let atomic_write_dir = normalize_atomic_write_dir(self.config.atomic_write_dir)?;
        let user_name = self.config.user_name.filter(|v| !v.is_empty());
        let delegation = self.config.delegation.filter(|v| !v.is_empty());

        let info = ServiceInfo::new(WEBHDFS_SCHEME, &root, "");
        let capability = Capability {
            stat: true,

            read: true,

            write: true,
            write_can_append: true,
            write_can_multi: atomic_write_dir.is_some(),

            create_dir: true,
            delete: true,
            delete_with_recursive: true,

            list: true,

            shared: true,

            ..Default::default()
        };

        let accessor_info = info;
        let core = Arc::new(WebhdfsCore {
            info: accessor_info,
            capability,
            root,
            endpoint,
            user_name,
            delegation,
            root_checker: OnceCell::new(),
            atomic_write_dir,
            disable_list_batch: self.config.disable_list_batch,
        });

        Ok(WebhdfsBackend { core })
    }
}

fn normalize_endpoint(endpoint: Option<String>) -> Result<String> {
    let Some(endpoint) = endpoint else {
        return Ok(WEBHDFS_DEFAULT_ENDPOINT.to_string());
    };

    let endpoint = endpoint.trim();
    if endpoint.is_empty() {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "endpoint must not be empty",
        ));
    }

    let normalized = if let Some((scheme, rest)) = endpoint.split_once("://") {
        if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("endpoint scheme must be http or https, got: {scheme}"),
            ));
        }
        let rest = rest.trim_end_matches('/');
        if rest.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "endpoint authority is missing",
            ));
        }
        format!("{}://{rest}", scheme.to_ascii_lowercase())
    } else {
        let rest = endpoint.trim_end_matches('/');
        if rest.is_empty() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "endpoint must not be empty",
            ));
        }
        format!("http://{rest}")
    };

    let uri = normalized.parse::<http::Uri>().map_err(|err| {
        Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid").set_source(err)
    })?;

    if uri.host().is_none_or(str::is_empty) || uri.query().is_some() {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "endpoint must be a valid http or https base URI",
        ));
    }

    Ok(normalized)
}

fn normalize_atomic_write_dir(dir: Option<String>) -> Result<Option<String>> {
    let Some(dir) = dir else {
        return Ok(None);
    };
    if dir.is_empty() {
        return Ok(None);
    }

    let normalized = normalize_path(&dir);
    if normalized == "/" {
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            "atomic_write_dir must be a subdirectory, not root",
        ));
    }

    if normalized.ends_with('/') {
        Ok(Some(normalized))
    } else {
        Ok(Some(format!("{normalized}/")))
    }
}

/// Backend for WebHDFS service
#[derive(Debug, Clone)]
pub struct WebhdfsBackend {
    pub(crate) core: Arc<WebhdfsCore>,
}

impl WebhdfsBackend {
    async fn check_root(&self, ctx: &OperationContext) -> Result<()> {
        let resp = self.core.webhdfs_get_file_status(ctx, "/").await?;
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
                self.create_dir(ctx, "/", OpCreateDir::new()).await?;
            }
            _ => {
                return Err(parse_error(
                    ErrorContext::new(ServiceOperation("GetFileStatus")),
                    resp,
                ));
            }
        }
        Ok(())
    }
}

impl Service for WebhdfsBackend {
    type Reader = oio::StreamReader<WebhdfsReader>;
    type Writer = WebhdfsWriters;
    type Lister = oio::PageLister<WebhdfsLister>;
    type Deleter = oio::OneShotDeleter<WebhdfsDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    /// Create a file or directory
    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        let resp = self.core.webhdfs_create_dir(ctx, path).await?;

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
            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("Mkdirs")),
                resp,
            )),
        }
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        // if root exists and is a directory, stat will be ok
        self.core
            .root_checker
            .get_or_try_init(|| async { self.check_root(ctx).await })
            .await?;

        let resp = self.core.webhdfs_get_file_status(ctx, path).await?;
        let status = resp.status();
        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let file_status = serde_json::from_reader::<_, FileStatusWrapper>(bs.reader())
                    .map_err(new_json_deserialize_error)?
                    .file_status;

                let meta = match file_status.ty {
                    FileStatusType::Directory => MetadataBuilder::dir().build(),
                    FileStatusType::File => {
                        let mut metadata = MetadataBuilder::file(file_status.length);
                        metadata.last_modified(Timestamp::from_millisecond(
                            file_status.modification_time,
                        )?);
                        metadata.build()
                    }
                    FileStatusType::Symlink => MetadataBuilder::unknown().build(),
                };

                Ok(RpStat::new(meta))
            }

            _ => Err(parse_error(
                ErrorContext::new(ServiceOperation("GetFileStatus")),
                resp,
            )),
        }
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<WebhdfsReader> = {
            Ok(oio::StreamReader::new(WebhdfsReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let output: WebhdfsWriters = {
            let w = WebhdfsWriter::new(
                self.core.clone(),
                ctx.clone(),
                args.clone(),
                path.to_string(),
            );

            let w = if args.append() {
                WebhdfsWriters::Two(oio::AppendWriter::new(w))
            } else {
                WebhdfsWriters::One(oio::BlockWriter::new(
                    ctx.executor().clone(),
                    w,
                    args.concurrent(),
                ))
            };

            Ok(w)
        }?;

        Ok(output)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<WebhdfsDeleter> = {
            Ok(oio::OneShotDeleter::new(WebhdfsDeleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<WebhdfsLister> = {
            if args.recursive() {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "WebHDFS doesn't support list with recursive",
                ));
            }

            let path = path.trim_end_matches('/');
            let l = WebhdfsLister::new(self.core.clone(), ctx.clone(), path);
            Ok(oio::PageLister::new(l))
        }?;

        Ok(output)
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use http::{Request, Response};

    use super::*;

    #[test]
    fn test_endpoint_normalization_and_validation() {
        // Bare hostname starting with "http" must still get http:// prepended.
        let endpoint = normalize_endpoint(Some("http-hdfs:9870".to_string())).unwrap();
        assert_eq!(endpoint, "http://http-hdfs:9870");

        // Uppercase scheme and trailing slash are normalized.
        let endpoint = normalize_endpoint(Some("HTTPS://host:9870/".to_string())).unwrap();
        assert_eq!(endpoint, "https://host:9870");

        // Trailing slash from config/from_iter is trimmed.
        let cfg = WebhdfsConfig::from_iter(vec![(
            "endpoint".to_string(),
            "http://127.0.0.1:9870/".to_string(),
        )])
        .unwrap();
        assert_eq!(
            normalize_endpoint(cfg.endpoint).unwrap(),
            "http://127.0.0.1:9870"
        );

        // Empty endpoint via WebhdfsConfig or whitespace via from_iter is rejected with ConfigInvalid.
        let cfg = WebhdfsConfig {
            endpoint: Some(String::new()),
            ..Default::default()
        };
        let err = cfg.into_builder().build().err().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let cfg =
            WebhdfsConfig::from_iter(vec![("endpoint".to_string(), "   ".to_string())]).unwrap();
        let err = cfg.into_builder().build().err().unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        // Non-HTTP schemes and missing authority are rejected with ConfigInvalid.
        for invalid in [
            "",
            "   ",
            "ftp://host:9870",
            "http://",
            "http://host:9870?a=1",
        ] {
            let err = normalize_endpoint(Some(invalid.to_string())).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "input: {invalid:?}");
        }
    }

    #[test]
    fn test_atomic_write_dir_normalization_and_validation() {
        assert_eq!(
            normalize_atomic_write_dir(Some(".opendal_tmp".to_string())).unwrap(),
            Some(".opendal_tmp/".to_string())
        );
        assert_eq!(
            normalize_atomic_write_dir(Some("/.opendal_tmp/".to_string())).unwrap(),
            Some(".opendal_tmp/".to_string())
        );
        assert_eq!(
            normalize_atomic_write_dir(Some("/.opendal_tmp".to_string())).unwrap(),
            Some(".opendal_tmp/".to_string())
        );
        assert_eq!(
            normalize_atomic_write_dir(Some(String::new())).unwrap(),
            None
        );
        assert_eq!(normalize_atomic_write_dir(None).unwrap(), None);

        // Root cannot be used as atomic_write_dir.
        for invalid in ["/", "///", "."] {
            let err = normalize_atomic_write_dir(Some(invalid.to_string())).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "input: {invalid:?}");
        }

        // Verify capability and build_absolute_path safety through Builder / Config::from_iter.
        let cfg = WebhdfsConfig::from_iter(vec![(
            "atomic_write_dir".to_string(),
            "/.opendal_tmp/".to_string(),
        )])
        .unwrap();
        let service = cfg.into_builder().build().unwrap();
        assert!(service.capability().write_can_multi);

        // Empty atomic_write_dir via WebhdfsConfig disables write_can_multi.
        let cfg = WebhdfsConfig {
            atomic_write_dir: Some(String::new()),
            ..Default::default()
        };
        let service = cfg.into_builder().build().unwrap();
        assert!(!service.capability().write_can_multi);

        // Normalized path must work with build_absolute_path without debug_assert panic.
        let dir = normalize_atomic_write_dir(Some("/.opendal_tmp".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            build_absolute_path("/root/", &format!("{dir}block-1")),
            "root/.opendal_tmp/block-1"
        );
    }

    struct SymlinkStatTransport;

    impl HttpTransport for SymlinkStatTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let uri = req.uri().to_string();
            let body = if uri.contains("/webhdfs/v1/?op=GETFILESTATUS") {
                r#"{"FileStatus":{"length":0,"modificationTime":1700000000000,"pathSuffix":"","type":"DIRECTORY"}}"#
            } else {
                r#"{"FileStatus":{"length":0,"modificationTime":1700000000000,"pathSuffix":"link","type":"SYMLINK"}}"#
            };
            let body = Buffer::from(body);
            let size = body.len() as u64;
            Ok(Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(futures::stream::iter([Ok(body)]), Some(size)))
                .unwrap())
        }
    }

    #[tokio::test]
    async fn test_stat_handles_symlink() -> Result<()> {
        let backend = WebhdfsBuilder::default()
            .endpoint("http-hdfs:9870")
            .build()?;
        let ctx = OperationContext::default()
            .with_http_transport(HttpTransporter::new(SymlinkStatTransport));

        let rp = backend.stat(&ctx, "link", OpStat::new()).await?;
        assert_eq!(rp.into_metadata().mode(), EntryMode::Unknown);
        Ok(())
    }
}
