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

use asyncband::rwlock::RwLock;
use bytes::Buf;
use http::Request;
use http::StatusCode;
use log::debug;

use super::B2_SCHEME;
use super::config::B2Config;
use super::core::B2Signer;
use super::core::constants;
use super::core::parse_error;
use super::core::parse_file_info;
use super::core::{
    B2Core, DownloadResponseOverrides, ErrorContext, FileVersion, FileVersionAction,
    ListFileVersionsRequest, ListFileVersionsResponse,
};
use super::deleter::B2Deleter;
use super::lister::{B2Lister, B2Listers, B2VersionLister};
use super::reader::*;
use super::writer::B2Writer;
use super::writer::B2Writers;
use opendal_core::raw::*;
use opendal_core::*;

/// [b2](https://www.backblaze.com/cloud-storage) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct B2Builder {
    pub(super) config: B2Config,
}

impl Debug for B2Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("B2Builder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl B2Builder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// application_key_id of this backend.
    pub fn application_key_id(mut self, application_key_id: &str) -> Self {
        self.config.application_key_id = if application_key_id.is_empty() {
            None
        } else {
            Some(application_key_id.to_string())
        };

        self
    }

    /// application_key of this backend.
    pub fn application_key(mut self, application_key: &str) -> Self {
        self.config.application_key = if application_key.is_empty() {
            None
        } else {
            Some(application_key.to_string())
        };

        self
    }

    /// Set bucket name of this backend.
    /// You can find it in <https://secure.backblaze.com/b2_buckets.html>
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();

        self
    }

    /// Set bucket id of this backend.
    /// You can find it in <https://secure.backblaze.com/b2_buckets.html>
    pub fn bucket_id(mut self, bucket_id: &str) -> Self {
        self.config.bucket_id = bucket_id.to_string();

        self
    }
}

impl Builder for B2Builder {
    type Config = B2Config;

    /// Builds the backend and returns the result of B2Backend.
    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", root);

        // Handle bucket.
        if self.config.bucket.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket is empty")
                .with_operation("Builder::build")
                .with_context("service", B2_SCHEME));
        }

        debug!("backend use bucket {}", self.config.bucket);

        // Handle bucket_id.
        if self.config.bucket_id.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket_id is empty")
                .with_operation("Builder::build")
                .with_context("service", B2_SCHEME));
        }

        debug!("backend bucket_id {}", self.config.bucket_id);

        let application_key_id = match &self.config.application_key_id {
            Some(application_key_id) => Ok(application_key_id.clone()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "application_key_id is empty")
                    .with_operation("Builder::build")
                    .with_context("service", B2_SCHEME),
            ),
        }?;

        let application_key = match &self.config.application_key {
            Some(key_id) => Ok(key_id.clone()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "application_key is empty")
                    .with_operation("Builder::build")
                    .with_context("service", B2_SCHEME),
            ),
        }?;

        let signer = B2Signer {
            application_key_id,
            application_key,
            ..Default::default()
        };

        Ok(B2Backend {
            core: Arc::new(B2Core {
                info: ServiceInfo::new(B2_SCHEME, &root, ""),
                capability: Capability {
                    stat: true,
                    stat_with_version: true,
                    stat_with_override_cache_control: true,
                    stat_with_override_content_disposition: true,
                    stat_with_override_content_type: true,

                    read: true,
                    read_with_version: true,
                    read_with_override_cache_control: true,
                    read_with_override_content_disposition: true,
                    read_with_override_content_type: true,
                    read_with_suffix: true,

                    write: true,
                    write_can_empty: true,
                    write_can_multi: true,
                    write_with_content_type: true,
                    write_with_user_metadata: true,
                    write_with_checksum: true,
                    // The min multipart size of b2 is 5 MiB.
                    //
                    // ref: <https://www.backblaze.com/docs/cloud-storage-large-files>
                    write_multi_min_size: Some(5 * 1024 * 1024),
                    // The max multipart size of b2 is 5 Gb.
                    //
                    // ref: <https://www.backblaze.com/docs/cloud-storage-large-files>
                    write_multi_max_size: if cfg!(target_pointer_width = "64") {
                        Some(5 * 1024 * 1024 * 1024)
                    } else {
                        Some(usize::MAX)
                    },

                    delete: true,
                    copy: true,
                    restore: true,
                    restore_with_version: true,

                    list: true,
                    list_with_limit: true,
                    list_with_start_after: true,
                    list_with_recursive: true,
                    list_with_versions: true,

                    presign: true,
                    presign_read: true,
                    presign_write: true,
                    presign_stat: true,

                    shared: true,

                    ..Default::default()
                },
                signer: Arc::new(RwLock::new(signer)),
                root,

                bucket: self.config.bucket.clone(),
                bucket_id: self.config.bucket_id.clone(),
            }),
        })
    }
}

/// Backend for b2 services.
#[derive(Debug, Clone)]
pub struct B2Backend {
    pub(crate) core: Arc<B2Core>,
}

impl Service for B2Backend {
    type Reader = oio::StreamReader<B2Reader>;
    type Writer = B2Writers;
    type Lister = B2Listers;
    type Deleter = oio::OneShotDeleter<B2Deleter>;
    type Copier = oio::OneShotCopier;
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.core.info.clone()
    }

    fn capability(&self) -> Capability {
        self.core.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        if let Some(version) = args.version() {
            if path.ends_with('/') {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "no such data version for this path",
                ));
            }
            if version.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "empty stat version ID",
                ));
            }

            let key = build_absolute_path(&self.core.root, path);
            let body = self
                .core
                .get_data_version(ctx, &key, version, Operation::Stat)
                .await?;
            let file =
                serde_json::from_reader(body.reader()).map_err(new_json_deserialize_error)?;
            return Ok(RpStat::new(parse_file_info(&file).build()));
        }

        if path == "/" {
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }

        // Listing by name returns the current file's metadata without first resolving its ID.

        let delimiter = if path.ends_with('/') { Some("/") } else { None };

        let file_info = self.core.get_file_info(ctx, path, delimiter).await?;
        let meta = parse_file_info(&file_info).build();
        Ok(RpStat::new(meta))
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        if let Some(version) = args.version() {
            if version.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "empty read version ID",
                ));
            }
            if args.override_cache_control().is_some()
                || args.override_content_disposition().is_some()
                || args.override_content_type().is_some()
            {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "B2 does not support response overrides for version-selected reads",
                ));
            }
        }
        let output: oio::StreamReader<B2Reader> = {
            Ok(oio::StreamReader::new(B2Reader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let concurrent = args.concurrent();
        let writer = B2Writer::new(self.core.clone(), ctx.clone(), path, args)?;
        Ok(oio::MultipartWriter::new(
            ctx.executor().clone(),
            writer,
            concurrent,
        ))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<B2Deleter> = {
            Ok(oio::OneShotDeleter::new(B2Deleter::new(
                self.core.clone(),
                ctx.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        if args.versions() {
            Ok(TwoWays::Two(B2VersionLister::new(
                self.core.clone(),
                ctx.clone(),
                path,
                &args,
            )))
        } else {
            Ok(TwoWays::One(oio::PageLister::new(B2Lister::new(
                self.core.clone(),
                ctx.clone(),
                path,
                args.recursive(),
                args.limit(),
                args.start_after(),
            ))))
        }
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        let core = self.core.clone();
        let ctx = ctx.clone();
        let from = from.to_string();
        let to = to.to_string();

        Ok(oio::OneShotCopier::new(async move {
            if from.ends_with('/') {
                return Err(Error::new(ErrorKind::IsADirectory, "is a directory"));
            }

            let file_info = core.get_file_info(&ctx, &from, None).await?;
            let source_file_id = file_info.file_id;
            let source_content_length = file_info.content_length;

            let Some(source_file_id) = source_file_id else {
                return Err(Error::new(ErrorKind::IsADirectory, "is a directory"));
            };

            let req = core
                .copy_file_request(&ctx, &source_file_id, &to, Operation::Copy)
                .await?;
            let resp = core.send(&ctx, req).await?;

            let status = resp.status();

            match status {
                StatusCode::OK => {
                    let metadata = MetadataBuilder::file(source_content_length);
                    Ok(metadata.build())
                }
                _ => Err(parse_error(
                    ErrorContext::new(ServiceOperation("CopyFile")),
                    resp,
                )),
            }
        }))
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        args.check(&self.capability())?;

        let key = build_absolute_path(&self.core.root, path);
        if let Some(version) = args.version() {
            if version.is_empty() {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "empty restore version ID",
                ));
            }
            self.core
                .get_data_version(ctx, &key, version, Operation::Restore)
                .await?;

            let req = self
                .core
                .copy_file_request(ctx, version, path, Operation::Restore)
                .await?;
            // A lost copy response can otherwise cause a second promotion.
            let resp = self
                .core
                .send(ctx, req)
                .await
                .map_err(Error::set_permanent)?;
            return match resp.status() {
                StatusCode::OK => Ok(RpRestore::new()),
                status => Err(
                    parse_error(ErrorContext::new(ServiceOperation("CopyFile")), resp)
                        .with_permanent(status.is_server_error()),
                ),
            };
        }

        let mut start_file_id = None;
        let mut current_marker = None;
        let marker = 'history: loop {
            let request = ListFileVersionsRequest {
                bucket_id: &self.core.bucket_id,
                prefix: &key,
                max_file_count: Some(2),
                start_file_name: start_file_id.as_ref().map(|_| key.as_str()),
                start_file_id: start_file_id.as_deref(),
                delimiter: None,
            };
            let page: ListFileVersionsResponse<FileVersion> = self
                .core
                .list_file_versions(ctx, request, Operation::Restore)
                .await?;
            for file in page.files {
                if file.file_name != key {
                    break 'history None;
                }
                match file.action {
                    FileVersionAction::Upload | FileVersionAction::Copy => {
                        if current_marker.is_none() {
                            return Ok(RpRestore::new());
                        }
                        break 'history current_marker;
                    }
                    FileVersionAction::Hide if current_marker.is_none() => {
                        current_marker = Some(file.file_id);
                    }
                    FileVersionAction::Hide | FileVersionAction::Start => {}
                }
            }

            // A hide marker can remain after all data versions are gone.
            if page.next_file_name.as_deref() != Some(&key) {
                break None;
            }
            start_file_id = Some(
                page.next_file_id
                    .filter(|next| !next.is_empty() && Some(next) != start_file_id.as_ref())
                    .ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "version listing did not advance")
                    })?,
            );
        };
        let Some(marker) = marker else {
            return Err(Error::new(
                ErrorKind::NotFound,
                "no live or recoverable version exists",
            ));
        };
        if marker.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "current hide marker has no version ID",
            ));
        }

        let req = self
            .core
            .delete_file_version_request(ctx, &key, &marker)
            .await?;
        // Replaying discovery after a lost delete response can remove another marker.
        let resp = self
            .core
            .send(ctx, req)
            .await
            .map_err(Error::set_permanent)?;
        match resp.status() {
            StatusCode::OK => Ok(RpRestore::new()),
            status => Err(parse_error(
                ErrorContext::new(ServiceOperation("DeleteFileVersion")),
                resp,
            )
            .with_permanent(status.is_server_error())),
        }
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
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        match args.operation() {
            PresignOperation::Stat(stat) => {
                if stat.version().is_some() {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "B2 does not support presigning stat for a selected version",
                    ));
                }
                self.core
                    .presign_download(
                        ctx,
                        path,
                        args.expire(),
                        BytesRange::default(),
                        DownloadResponseOverrides {
                            content_type: stat.override_content_type(),
                            content_disposition: stat.override_content_disposition(),
                            cache_control: stat.override_cache_control(),
                        },
                    )
                    .await
            }
            PresignOperation::Read(range, read) => {
                if read.version().is_some() {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "B2 does not support presigning reads for a selected version",
                    ));
                }
                self.core
                    .presign_download(
                        ctx,
                        path,
                        args.expire(),
                        *range,
                        DownloadResponseOverrides {
                            content_type: read.override_content_type(),
                            content_disposition: read.override_content_disposition(),
                            cache_control: read.override_cache_control(),
                        },
                    )
                    .await
            }
            PresignOperation::Write(write) => {
                const CACHE_CONTROL_FILE_INFO_KEY: &str = "b2-cache-control";

                if write.checksum().is_some() {
                    return Err(Error::new(
                        ErrorKind::Unsupported,
                        "B2 does not support presigning writes with a checksum intent",
                    ));
                }
                let resp = self.core.get_upload_url(ctx).await?;

                let mut req = Request::post(&resp.upload_url);

                req = req.header(http::header::AUTHORIZATION, resp.authorization_token);
                req = req.header(
                    "X-Bz-File-Name",
                    percent_encode_path(&build_absolute_path(&self.core.root, path)),
                );
                let content_type = write.content_type().unwrap_or("b2/x-auto");
                req = req.header(http::header::CONTENT_TYPE, content_type);
                req = req.header(constants::X_BZ_CONTENT_SHA1, "do_not_verify");

                let cache_control = write.cache_control();
                if let Some(value) = cache_control {
                    req = req.header(
                        format!(
                            "{}{CACHE_CONTROL_FILE_INFO_KEY}",
                            constants::X_BZ_INFO_PREFIX
                        ),
                        percent_encode_path(value),
                    );
                }

                if let Some(user_metadata) = write.user_metadata() {
                    for (key, value) in user_metadata {
                        if cache_control.is_some()
                            && (key.eq_ignore_ascii_case("cache-control")
                                || key.eq_ignore_ascii_case(CACHE_CONTROL_FILE_INFO_KEY))
                        {
                            continue;
                        }
                        req = req.header(
                            format!("{}{}", constants::X_BZ_INFO_PREFIX, key),
                            percent_encode_path(value),
                        );
                    }
                }

                let req = req.body(Buffer::new()).map_err(new_request_build_error)?;
                // We don't need this request anymore, consume it directly.
                let (parts, _) = req.into_parts();

                Ok(RpPresign::new(PresignedRequest::new(
                    parts.method,
                    parts.uri,
                    parts.headers,
                )))
            }
            PresignOperation::Delete(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
            _ => Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::stream;
    use http::Response;
    use opendal_core::raw::oio::Copy as _;

    use super::*;
    use crate::core::tests::test_core;

    struct ScriptedTransport {
        list_body: String,
        requests: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl HttpTransport for ScriptedTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let uri = req.uri().to_string();
            let body_str = String::from_utf8_lossy(&req.into_body().to_bytes()).into_owned();
            self.requests.lock().unwrap().push((uri.clone(), body_str));

            let resp_body = if uri.contains("b2_list_file_names") {
                self.list_body.clone()
            } else if uri.contains("b2_copy_file") {
                r#"{"fileId":"4_zcopied","fileName":"dst","contentLength":5}"#.to_string()
            } else {
                panic!("unexpected request URI: {uri}");
            };

            let buf = Buffer::from(resp_body);
            let len = buf.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([Ok(buf)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    fn stat_operator(version_status: StatusCode, version_body: &'static str) -> Result<Operator> {
        version_operator(Operation::Stat, version_status, version_body)
    }

    fn version_operator(
        operation: Operation,
        version_status: StatusCode,
        version_body: &'static str,
    ) -> Result<Operator> {
        struct VersionTransport {
            operation: Operation,
            version_status: StatusCode,
            version_body: &'static str,
        }

        impl HttpTransport for VersionTransport {
            async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
                let (status, body) = match request.uri().path() {
                    "/b2api/v4/b2_authorize_account" => (
                        StatusCode::OK,
                        r#"{"authorizationToken":"token","apiInfo":{"storageApi":{"apiUrl":"https://api.example","downloadUrl":"https://download.example"}}}"#,
                    ),
                    "/b2api/v2/b2_get_file_info" => {
                        assert_eq!(request.method(), http::Method::POST);
                        assert_eq!(
                            request.extensions().get::<Operation>(),
                            Some(&self.operation)
                        );
                        assert_eq!(request.body().to_bytes().as_ref(), br#"{"fileId":"old"}"#);
                        (self.version_status, self.version_body)
                    }
                    "/b2api/v2/b2_copy_file" if self.operation == Operation::Restore => {
                        assert_eq!(
                            request.extensions().get::<Operation>(),
                            Some(&Operation::Restore)
                        );
                        let body: serde_json::Value =
                            serde_json::from_reader(request.body().clone().reader()).unwrap();
                        assert_eq!(body["sourceFileId"], "old");
                        assert_eq!(body["fileName"], "root/file");
                        (
                            StatusCode::FORBIDDEN,
                            r#"{"status":403,"code":"unauthorized","message":"copy denied"}"#,
                        )
                    }
                    "/b2api/v2/b2_list_file_names" => {
                        let query = request.uri().query().unwrap();
                        let body = if query.contains("prefix=root/dir/") {
                            r#"{"files":[{"fileName":"root/dir/","contentLength":0}]}"#
                        } else {
                            assert!(query.contains("prefix=root/file"));
                            r#"{"files":[{"fileName":"root/file","fileId":"new","contentLength":13}]}"#
                        };
                        (StatusCode::OK, body)
                    }
                    other => panic!("unexpected path: {other}"),
                };
                let buffer = Buffer::from(body);
                Response::builder()
                    .status(status)
                    .body(HttpBody::new(
                        stream::iter([Ok(buffer)]),
                        Some(body.len() as u64),
                    ))
                    .map_err(new_request_build_error)
            }
        }

        let op = Operator::new(
            B2Builder::default()
                .root("root")
                .bucket("test-bucket")
                .bucket_id("bucket-id")
                .application_key_id("test-key")
                .application_key("test-secret"),
        )?;
        let transport = HttpTransporter::new(VersionTransport {
            operation,
            version_status,
            version_body,
        });
        Ok(op.with_context(OperationContext::new().with_http_transport(transport)))
    }

    #[tokio::test]
    async fn test_version_stat_preserves_metadata_from_selected_file() -> Result<()> {
        let op = stat_operator(
            StatusCode::OK,
            r#"{"bucketId":"bucket-id","fileName":"root/file","fileId":"old","action":"upload","contentLength":5,"contentType":"text/plain","contentMd5":"md5","fileInfo":{"Owner":"a%2Bb"}}"#,
        )?;
        assert!(op.info().capability().stat_with_version);
        let old = op.stat_with("file").version("old").await?;
        assert_eq!(old.mode(), EntryMode::FILE);
        assert_eq!(old.version(), Some("old"));
        assert_eq!(old.content_length(), 5);
        assert_eq!(old.content_type(), Some("text/plain"));
        assert_eq!(old.content_md5(), Some("md5"));
        assert_eq!(old.user_metadata().unwrap().get("owner"), Some("a+b"));

        let current = op.stat("file").await?;
        assert_eq!(current.version(), Some("new"));
        assert_eq!(current.content_length(), 13);

        let op = stat_operator(
            StatusCode::OK,
            r#"{"bucketId":"bucket-id","fileName":"root/file","fileId":"old","action":"copy","contentLength":0}"#,
        )?;
        let old = op.stat_with("file").version("old").await?;
        let mut expected = MetadataBuilder::file(0);
        expected.version("old");
        assert_eq!(old, expected.build());
        Ok(())
    }

    #[tokio::test]
    async fn test_version_stat_requires_exact_data_version() -> Result<()> {
        for (status, body, expected) in [
            (
                StatusCode::OK,
                r#"{"bucketId":"bucket-id","fileName":"root/file-neighbor","fileId":"old","action":"upload"}"#,
                ErrorKind::NotFound,
            ),
            (
                StatusCode::OK,
                r#"{"bucketId":"other","fileName":"root/file","fileId":"old","action":"upload"}"#,
                ErrorKind::NotFound,
            ),
            (
                StatusCode::OK,
                r#"{"bucketId":"bucket-id","fileName":"root/file","fileId":"old","action":"hide"}"#,
                ErrorKind::NotFound,
            ),
            (
                StatusCode::OK,
                r#"{"bucketId":"bucket-id","fileName":"root/file","fileId":"old","action":"start"}"#,
                ErrorKind::NotFound,
            ),
            (
                StatusCode::OK,
                r#"{"bucketId":"bucket-id","fileName":"root/file","fileId":"other","action":"upload","contentLength":5}"#,
                ErrorKind::Unexpected,
            ),
            (
                StatusCode::OK,
                r#"{"bucketId":"bucket-id","fileName":"root/file","fileId":"old","action":"upload"}"#,
                ErrorKind::Unexpected,
            ),
            (
                StatusCode::NOT_FOUND,
                r#"{"status":404,"code":"not_found","message":"missing"}"#,
                ErrorKind::NotFound,
            ),
            (
                StatusCode::FORBIDDEN,
                r#"{"status":403,"code":"unauthorized","message":"denied"}"#,
                ErrorKind::PermissionDenied,
            ),
        ] {
            let op = stat_operator(status, body)?;
            let error = op.stat_with("file").version("old").await.unwrap_err();
            assert_eq!(error.kind(), expected, "{body}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_restore_reaches_copy_with_version_identity_alone() -> Result<()> {
        let op = version_operator(
            Operation::Restore,
            StatusCode::OK,
            r#"{"bucketId":"bucket-id","fileName":"root/file","fileId":"old","action":"upload"}"#,
        )?;
        let error = op.restore_with("file").version("old").await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::PermissionDenied);
        Ok(())
    }

    #[tokio::test]
    async fn test_version_stat_and_presign_respect_operator_options() -> Result<()> {
        use opendal_core::layers::SimulateLayer;
        use std::time::Duration;

        let op = stat_operator(StatusCode::FORBIDDEN, "")?;
        let configured = op
            .clone()
            .layer(SimulateLayer::default().with_create_dir(false));
        for stack in [&op, &configured] {
            for path in ["/", "dir/"] {
                assert_eq!(stack.stat(path).await?.mode(), EntryMode::DIR);
                let error = stack.stat_with(path).version("old").await.unwrap_err();
                assert_eq!(error.kind(), ErrorKind::NotFound);
            }
        }

        let error = op
            .presign_stat_options(
                "file",
                Duration::from_secs(60),
                options::StatOptions {
                    version: Some("old".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_rejects_prefix_mismatch_without_issuing_copy_file() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let backend = B2Backend {
            core: test_core("/"),
        };
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            ScriptedTransport {
                list_body: r#"{"files":[{"fileId":"4_zfoobar","fileName":"foobar","contentLength":99}],"nextFileName":null}"#.to_string(),
                requests: requests.clone(),
            },
        ));

        let mut copier = backend.copy(&ctx, "foo", "dst", OpCopy::new()).unwrap();
        let err = copier.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::NotFound);

        let recorded = requests.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert!(recorded[0].0.contains("b2_list_file_names"));
        assert!(!recorded.iter().any(|(uri, _)| uri.contains("b2_copy_file")));
    }

    #[tokio::test]
    async fn test_copy_exact_match_succeeds_and_dir_rejected() {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let backend = B2Backend {
            core: test_core("/"),
        };
        let ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            ScriptedTransport {
                list_body: r#"{"files":[{"fileId":"4_zfoo","fileName":"foo","contentLength":5}],"nextFileName":null}"#.to_string(),
                requests: requests.clone(),
            },
        ));

        let mut copier = backend.copy(&ctx, "foo", "dst", OpCopy::new()).unwrap();
        assert_eq!(copier.next().await.unwrap(), None);
        let meta = copier.close().await.unwrap();
        assert_eq!(meta.mode(), EntryMode::FILE);
        assert_eq!(meta.content_length(), 5);

        {
            let recorded = requests.lock().unwrap();
            assert_eq!(recorded.len(), 2);
            assert!(recorded[1].0.contains("b2_copy_file"));
            assert!(recorded[1].1.contains(r#""sourceFileId":"4_zfoo""#));
            assert!(recorded[1].1.contains(r#""fileName":"dst""#));
        }

        // Copying a directory path must return IsADirectory without issuing b2_copy_file.
        let dir_requests = Arc::new(Mutex::new(Vec::new()));
        let dir_ctx = OperationContext::default().with_http_transport(HttpTransporter::new(
            ScriptedTransport {
                list_body: r#"{"files":[{"fileId":"4_zdir","fileName":"dir/a.txt","contentLength":5}],"nextFileName":null}"#.to_string(),
                requests: dir_requests.clone(),
            },
        ));
        let mut copier = backend
            .copy(&dir_ctx, "dir/", "dst", OpCopy::new())
            .unwrap();
        let err = copier.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::IsADirectory);
        assert!(dir_requests.lock().unwrap().is_empty());
    }
}
