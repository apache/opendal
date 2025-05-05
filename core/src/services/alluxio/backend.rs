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

use http::Response;
use log::debug;

use super::core::AlluxioCore;
use super::delete::AlluxioDeleter;
use super::error::parse_error;
use super::lister::AlluxioLister;
use super::writer::AlluxioWriter;
use super::writer::AlluxioWriters;
use crate::raw::*;
use crate::services::AlluxioConfig;
use crate::*;

impl Configurator for AlluxioConfig {
    type Builder = AlluxioBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        AlluxioBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// [Alluxio](https://www.alluxio.io/) services support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct AlluxioBuilder {
    config: AlluxioConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl Debug for AlluxioBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("AlluxioBuilder");

        d.field("config", &self.config);
        d.finish_non_exhaustive()
    }
}

impl AlluxioBuilder {
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

    /// endpoint of this backend.
    ///
    /// Endpoint must be full uri, mostly like `http://127.0.0.1:39999`.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:39999/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string())
        }

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

impl Builder for AlluxioBuilder {
    const SCHEME: Scheme = Scheme::Alluxio;
    type Config = AlluxioConfig;

    /// Builds the backend and returns the result of AlluxioBackend.
    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.clone().unwrap_or_default());
        debug!("backend use root {}", &root);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_operation("Builder::build")
                .with_context("service", Scheme::Alluxio)),
        }?;
        debug!("backend use endpoint {}", &endpoint);

        Ok(AlluxioBackend {
            core: Arc::new(AlluxioCore {
                info: {
                    let am = AccessorInfo::default();
                    am.set_scheme(Scheme::Alluxio)
                        .set_root(&root)
                        .set_native_capability(Capability {
                            stat: true,

                            // FIXME:
                            //
                            // alluxio's read support is not implemented correctly
                            // We need to refactor by use [page_read](https://github.com/Alluxio/alluxio-py/blob/main/alluxio/const.py#L18)
                            read: false,

                            write: true,
                            write_can_multi: true,

                            create_dir: true,
                            delete: true,

                            list: true,

                            shared: true,
                            stat_has_content_length: true,
                            stat_has_last_modified: true,
                            list_has_content_length: true,
                            list_has_last_modified: true,

                            ..Default::default()
                        });

                    // allow deprecated api here for compatibility
                    #[allow(deprecated)]
                    if let Some(client) = self.http_client {
                        am.update_http_client(|_| client);
                    }

                    am.into()
                },
                root,
                endpoint,
            }),
        })
    }
}

#[derive(Debug, Clone)]
pub struct AlluxioBackend {
    core: Arc<AlluxioCore>,
}

impl Access for AlluxioBackend {
    type Reader = HttpBody;
    type Writer = AlluxioWriters;
    type Lister = oio::PageLister<AlluxioLister>;
    type Deleter = oio::OneShotDeleter<AlluxioDeleter>;
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        self.core.create_dir(path).await?;
        Ok(RpCreateDir::default())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let file_info = self.core.get_status(path).await?;

        Ok(RpStat::new(file_info.try_into()?))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let stream_id = self.core.open_file(path).await?;

        let resp = self.core.read(stream_id, args.range()).await?;
        if !resp.status().is_success() {
            let (part, mut body) = resp.into_parts();
            let buf = body.to_buffer().await?;
            return Err(parse_error(Response::from_parts(part, buf)));
        }
        Ok((RpRead::new(), resp.into_body()))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let w = AlluxioWriter::new(self.core.clone(), args.clone(), path.to_string());

        Ok((RpWrite::default(), w))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        Ok((
            RpDelete::default(),
            oio::OneShotDeleter::new(AlluxioDeleter::new(self.core.clone())),
        ))
    }

    async fn list(&self, path: &str, _args: OpList) -> Result<(RpList, Self::Lister)> {
        let l = AlluxioLister::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }

    async fn rename(&self, from: &str, to: &str, _: OpRename) -> Result<RpRename> {
        self.core.rename(from, to).await?;

        Ok(RpRename::default())
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_builder_from_map() {
        let mut map = HashMap::new();
        map.insert("root".to_string(), "/".to_string());
        map.insert("endpoint".to_string(), "http://127.0.0.1:39999".to_string());

        let builder = AlluxioConfig::from_iter(map).unwrap();

        assert_eq!(builder.root, Some("/".to_string()));
        assert_eq!(builder.endpoint, Some("http://127.0.0.1:39999".to_string()));
    }

    #[test]
    fn test_builder_build() {
        let builder = AlluxioBuilder::default()
            .root("/root")
            .endpoint("http://127.0.0.1:39999")
            .build();

        assert!(builder.is_ok());
    }
}
