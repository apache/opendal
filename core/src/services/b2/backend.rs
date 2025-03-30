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
use std::fmt::Formatter;
use std::sync::Arc;

use bytes::Buf;
use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use tokio::sync::RwLock;

use super::core::constants;
use super::core::parse_file_info;
use super::core::B2Core;
use super::core::B2Signer;
use super::core::ListFileNamesResponse;
use super::delete::B2Deleter;
use super::error::parse_error;
use super::lister::B2Lister;
use super::writer::B2Writer;
use super::writer::B2Writers;
use crate::raw::*;
use crate::services::B2Config;
use crate::*;

impl Configurator for B2Config {
    type Builder = B2Builder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        B2Builder {
            config: self,
            http_client: None,
        }
    }
}

/// [b2](https://www.backblaze.com/cloud-storage) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct B2Builder {
    config: B2Config,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for B2Builder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("B2Builder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
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

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for B2Builder {
    const SCHEME: Scheme = Scheme::B2;
    type Config = B2Config;

    /// Builds the backend and returns the result of B2Backend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        // Handle bucket.
        if self.config.bucket.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::B2));
        }

        debug!("backend use bucket {}", &self.config.bucket);

        // Handle bucket_id.
        if self.config.bucket_id.is_empty() {
            return Err(Error::new(ErrorKind::ConfigInvalid, "bucket_id is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::B2));
        }

        debug!("backend bucket_id {}", &self.config.bucket_id);

        let application_key_id = match &self.config.application_key_id {
            Some(application_key_id) => Ok(application_key_id.clone()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "application_key_id is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::B2),
            ),
        }?;

        let application_key = match &self.config.application_key {
            Some(key_id) => Ok(key_id.clone()),
            None => Err(
                Error::new(ErrorKind::ConfigInvalid, "application_key is empty")
                    .with_operation("Builder::build")
                    .with_context("service", Scheme::B2),
            ),
        }?;

        let signer = B2Signer {
            application_key_id,
            application_key,
            ..Default::default()
        };

        Ok(B2Backend {
            core: Arc::new(B2Core {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::B2)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,
                            stat_has_content_length: true,
                            stat_has_content_md5: true,
                            stat_has_content_type: true,

                            read: true,

                            write: true,
                            write_can_empty: true,
                            write_can_multi: true,
                            write_with_content_type: true,
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

                            list: true,
                            list_with_limit: true,
                            list_with_start_after: true,
                            list_with_recursive: true,
                            list_has_content_length: true,
                            list_has_content_md5: true,
                            list_has_content_type: true,

                            presign: true,
                            presign_read: true,
                            presign_write: true,
                            presign_stat: true,

                            shared: true,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
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
    core: Arc<B2Core>,
}

impl Access for B2Backend {
    type Reader = HttpBody;
    type Writer = B2Writers;
    type Lister = oio::PageLister<B2Lister>;
    type Deleter = oio::OneShotDeleter<B2Deleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    /// B2 have a get_file_info api required a file_id field, but field_id need call list api, list api also return file info
    /// So we call list api to get file info
    async fn stat(&self, path: &str, _args: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let delimiter = if path.ends_with('/') { Some("/") } else { None };
        let resp = self
            .core
            .list_file_names(Some(path), delimiter, None, None)
            .await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let resp: ListFileNamesResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                if resp.files.is_empty() {
                    return Err(Error::new(ErrorKind::NotFound, "no such file or directory"));
                }
                let meta = parse_file_info(&resp.files[0]);
                Ok(RpStat::new(meta))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self
            .core
            .download_file_by_name(path, args.range(), &args)
            .await?;

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
        let concurrent = args.concurrent();
        let writer = B2Writer::new(self.core.clone(), path, args);

        let w = oio::MultipartWriter::new(self.core.info.clone(), writer, concurrent);

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(B2Deleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        Ok((
            RpList::default(),
            oio::PageLister::new(B2Lister::new(
                self.core.clone(),
                path,
                args.recursive(),
                args.limit(),
                args.start_after(),
            )),
        ))
    }

    async fn copy(&self, from: &str, to: &str, _args: OpCopy) -> Result<RpCopy> {
        let resp = self
            .core
            .list_file_names(Some(from), None, None, None)
            .await?;

        let status = resp.status();

        let source_file_id = match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let resp: ListFileNamesResponse =
                    serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;
                if resp.files.is_empty() {
                    return Err(Error::new(ErrorKind::NotFound, "no such file or directory"));
                }

                let file_id = resp.files[0].clone().file_id;
                Ok(file_id)
            }
            _ => Err(parse_error(resp)),
        }?;

        let Some(source_file_id) = source_file_id else {
            return Err(Error::new(ErrorKind::IsADirectory, "is a directory"));
        };

        let resp = self.core.copy_file(source_file_id, to).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => Ok(RpCopy::default()),
            _ => Err(parse_error(resp)),
        }
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        match args.operation() {
            PresignOperation::Stat(_) => {
                let resp = self
                    .core
                    .get_download_authorization(path, args.expire())
                    .await?;
                let path = build_abs_path(&self.core.root, path);

                let auth_info = self.core.get_auth_info().await?;

                let url = format!(
                    "{}/file/{}/{}?Authorization={}",
                    auth_info.download_url, self.core.bucket, path, resp.authorization_token
                );

                let req = Request::get(url);

                let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

                // We don't need this request anymore, consume
                let (parts, _) = req.into_parts();

                Ok(RpPresign::new(PresignedRequest::new(
                    parts.method,
                    parts.uri,
                    parts.headers,
                )))
            }
            PresignOperation::Read(_) => {
                let resp = self
                    .core
                    .get_download_authorization(path, args.expire())
                    .await?;
                let path = build_abs_path(&self.core.root, path);

                let auth_info = self.core.get_auth_info().await?;

                let url = format!(
                    "{}/file/{}/{}?Authorization={}",
                    auth_info.download_url, self.core.bucket, path, resp.authorization_token
                );

                let req = Request::get(url);

                let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

                // We don't need this request anymore, consume
                let (parts, _) = req.into_parts();

                Ok(RpPresign::new(PresignedRequest::new(
                    parts.method,
                    parts.uri,
                    parts.headers,
                )))
            }
            PresignOperation::Write(_) => {
                let resp = self.core.get_upload_url().await?;

                let mut req = Request::post(&resp.upload_url);

                req = req.header(http::header::AUTHORIZATION, resp.authorization_token);
                req = req.header("X-Bz-File-Name", build_abs_path(&self.core.root, path));
                req = req.header(http::header::CONTENT_TYPE, "b2/x-auto");
                req = req.header(constants::X_BZ_CONTENT_SHA1, "do_not_verify");

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
        }
    }
}
