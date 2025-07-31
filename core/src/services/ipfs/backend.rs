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
use http::StatusCode;
use log::debug;
use prost::Message;

use super::core::IpfsCore;
use super::error::parse_error;
use super::ipld::PBNode;
use super::DEFAULT_SCHEME;
use crate::raw::*;
use crate::services::IpfsConfig;
use crate::*;
impl Configurator for IpfsConfig {
    type Builder = IpfsBuilder;

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        IpfsBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// IPFS file system support based on [IPFS HTTP Gateway](https://docs.ipfs.tech/concepts/ipfs-gateway/).
#[doc = include_str!("docs.md")]
#[derive(Default, Clone, Debug)]
pub struct IpfsBuilder {
    config: IpfsConfig,

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    http_client: Option<HttpClient>,
}

impl IpfsBuilder {
    /// Set root of ipfs backend.
    ///
    /// Root must be a valid ipfs address like the following:
    ///
    /// - `/ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ/` (IPFS with CID v0)
    /// - `/ipfs/bafybeibozpulxtpv5nhfa2ue3dcjx23ndh3gwr5vwllk7ptoyfwnfjjr4q/` (IPFS with  CID v1)
    /// - `/ipns/opendal.apache.org/` (IPNS)
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set endpoint if ipfs backend.
    ///
    /// Endpoint must be a valid ipfs gateway which passed the [IPFS Gateway Checker](https://ipfs.github.io/public-gateway-checker/)
    ///
    /// Popular choices including:
    ///
    /// - `https://ipfs.io`
    /// - `https://w3s.link`
    /// - `https://dweb.link`
    /// - `https://cloudflare-ipfs.com`
    /// - `http://127.0.0.1:8080` (ipfs daemon in local)
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
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

impl Builder for IpfsBuilder {
    type Config = IpfsConfig;

    fn build(self) -> Result<impl Access> {
        debug!("backend build started: {:?}", &self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        if !root.starts_with("/ipfs/") && !root.starts_with("/ipns/") {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root must start with /ipfs/ or /ipns/",
            )
            .with_context("service", Scheme::Ipfs)
            .with_context("root", &root));
        }
        debug!("backend use root {root}");

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Ipfs)
                .with_context("root", &root)),
        }?;
        debug!("backend use endpoint {}", &endpoint);

        let info = AccessorInfo::default();
        info.set_scheme(DEFAULT_SCHEME)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                list: true,

                shared: true,

                ..Default::default()
            });

        let accessor_info = Arc::new(info);
        let core = Arc::new(IpfsCore {
            info: accessor_info,
            root,
            endpoint,
        });

        Ok(IpfsBackend { core })
    }
}

/// Backend for IPFS.
#[derive(Clone)]
pub struct IpfsBackend {
    core: Arc<IpfsCore>,
}

impl Debug for IpfsBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpfsBackend")
            .field("core", &self.core)
            .finish()
    }
}

impl Access for IpfsBackend {
    type Reader = HttpBody;
    type Writer = ();
    type Lister = oio::PageLister<DirStream>;
    type Deleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.ipfs_stat(path).await?;
        Ok(RpStat::new(metadata))
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.core.ipfs_get(path, args.range()).await?;

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

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, Self::Lister)> {
        let l = DirStream::new(self.core.clone(), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

pub struct DirStream {
    core: Arc<IpfsCore>,
    path: String,
}

impl DirStream {
    fn new(core: Arc<IpfsCore>, path: &str) -> Self {
        Self {
            core,
            path: path.to_string(),
        }
    }
}

impl oio::PageList for DirStream {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self.core.ipfs_list(&self.path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp));
        }

        let bs = resp.into_body();
        let pb_node = PBNode::decode(bs).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "deserialize protobuf from response").set_source(e)
        })?;

        let names = pb_node
            .links
            .into_iter()
            .map(|v| v.name.unwrap())
            .collect::<Vec<String>>();

        for mut name in names {
            let meta = self.core.ipfs_stat(&name).await?;

            if meta.mode().is_dir() {
                name += "/";
            }

            ctx.entries.push_back(oio::Entry::new(&name, meta))
        }

        ctx.done = true;
        Ok(())
    }
}
