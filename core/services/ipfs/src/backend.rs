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

use http::StatusCode;
use log::debug;
use prost::Message;

use super::reader::*;
use crate::IPFS_SCHEME;
use crate::config::IpfsConfig;
use crate::core::parse_error;
use crate::core::{ErrorContext, IpfsCore};
use crate::ipld::PBNode;
use opendal_core::raw::*;
use opendal_core::*;

/// IPFS file system support based on [IPFS HTTP Gateway](https://docs.ipfs.tech/concepts/ipfs-gateway/).
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct IpfsBuilder {
    pub(super) config: IpfsConfig,
}

impl Debug for IpfsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpfsBuilder")
            .field("config", &self.config)
            .finish()
    }
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
}

impl Builder for IpfsBuilder {
    type Config = IpfsConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(&self.config.root.unwrap_or_default());
        if !root.starts_with("/ipfs/") && !root.starts_with("/ipns/") {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "root must start with /ipfs/ or /ipns/",
            )
            .with_context("service", IPFS_SCHEME)
            .with_context("root", &root));
        }
        debug!("backend use root {root}");

        let endpoint = self
            .config
            .endpoint
            .as_deref()
            .filter(|v| !v.trim_end_matches('/').is_empty())
            .ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                    .with_context("service", IPFS_SCHEME)
                    .with_context("root", &root)
            })?
            .to_string();
        debug!("backend use endpoint {}", endpoint);

        let info = ServiceInfo::new(IPFS_SCHEME, &root, "");
        let capability = Capability {
            stat: true,

            read: true,
            read_with_suffix: true,

            list: true,

            shared: true,

            ..Default::default()
        };

        let accessor_info = info;
        let core = Arc::new(IpfsCore {
            info: accessor_info,
            capability,
            root,
            endpoint,
        });

        Ok(IpfsBackend { core })
    }
}

/// Backend for IPFS.
#[derive(Clone, Debug)]
pub struct IpfsBackend {
    pub(crate) core: Arc<IpfsCore>,
}

impl Service for IpfsBackend {
    type Reader = oio::StreamReader<IpfsReader>;
    type Writer = ();
    type Lister = oio::PageLister<DirStream>;
    type Deleter = ();
    type Copier = ();
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

    async fn stat(&self, ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let metadata = self.core.ipfs_stat(ctx, path).await?;
        Ok(RpStat::new(metadata))
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<IpfsReader> = {
            Ok(oio::StreamReader::new(IpfsReader::new(
                self.clone(),
                ctx.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, _path: &str, _args: OpWrite) -> Result<Self::Writer> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn list(&self, ctx: &OperationContext, path: &str, _: OpList) -> Result<Self::Lister> {
        let output: oio::PageLister<DirStream> = {
            let l = DirStream::new(self.core.clone(), ctx.clone(), path);
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

pub struct DirStream {
    core: Arc<IpfsCore>,
    ctx: OperationContext,
    path: String,
}

impl DirStream {
    fn new(core: Arc<IpfsCore>, ctx: OperationContext, path: &str) -> Self {
        Self {
            core,
            ctx,
            path: path.to_string(),
        }
    }
}

impl oio::PageList for DirStream {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self.core.ipfs_list(&self.ctx, &self.path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("List")),
                resp,
            ));
        }

        let bs = resp.into_body();
        let pb_node = PBNode::decode(bs).map_err(|e| {
            Error::new(ErrorKind::Unexpected, "deserialize protobuf from response").set_source(e)
        })?;

        let names = pb_node
            .links
            .into_iter()
            .filter_map(|v| v.name.filter(|name| !name.is_empty()))
            .collect::<Vec<String>>();

        let trimmed = self.path.trim_matches('/');
        let prefix = if trimmed.is_empty() {
            String::new()
        } else {
            format!("{trimmed}/")
        };

        for name in names {
            let child = format!("{prefix}{name}");

            let meta = self.core.ipfs_stat(&self.ctx, &child).await?;

            let child = if meta.mode().is_dir() {
                format!("{child}/")
            } else {
                child
            };

            ctx.entries.push_back(oio::Entry::new(&child, meta))
        }

        ctx.done = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::stream;
    use http::{Method, Request, Response, header};
    use oio::PageList;

    use super::*;
    use crate::ipld::PBLink;

    const TEST_ROOT: &str = "/ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ/";

    struct MockIpfsTransport {
        requests: Arc<Mutex<Vec<(Method, String)>>>,
        list_status: StatusCode,
        pb_node: PBNode,
    }

    impl HttpTransport for MockIpfsTransport {
        async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
            let method = req.method().clone();
            let uri = req.uri().to_string();
            self.requests
                .lock()
                .expect("mutex poisoned")
                .push((method.clone(), uri.clone()));

            if method == Method::GET {
                if self.list_status != StatusCode::OK {
                    return Ok(Response::builder()
                        .status(self.list_status)
                        .body(HttpBody::new(stream::empty(), Some(0)))
                        .unwrap());
                }

                let body = Buffer::from(self.pb_node.encode_to_vec());
                let len = body.len() as u64;
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(HttpBody::new(stream::iter([Ok(body)]), Some(len)))
                    .unwrap());
            }

            if method == Method::HEAD {
                if uri.ends_with("/nested") || uri.ends_with("/dir") {
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .header(header::ETAG, "\"DirIndex-123\"")
                        .body(HttpBody::new(stream::empty(), Some(0)))
                        .unwrap());
                }

                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header(header::ETAG, "\"QmFile\"")
                    .header(header::CONTENT_LENGTH, "42")
                    .body(HttpBody::new(stream::empty(), Some(0)))
                    .unwrap());
            }

            panic!("unexpected request: {method} {uri}");
        }
    }

    #[test]
    fn test_build_rejects_empty_and_slash_only_endpoint() -> Result<()> {
        for endpoint in [Some(""), Some("///"), None] {
            let mut pairs = vec![("root".to_string(), TEST_ROOT.to_string())];
            if let Some(ep) = endpoint {
                pairs.push(("endpoint".to_string(), ep.to_string()));
            }
            let config = IpfsConfig::from_iter(pairs)?;
            let err = config.into_builder().build().err().unwrap();
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        let err = IpfsBuilder::default()
            .root(TEST_ROOT)
            .endpoint("///")
            .build()
            .err()
            .unwrap();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_skips_unnamed_and_empty_pb_links() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let pb_node = PBNode {
                data: None,
                links: vec![
                    PBLink {
                        hash: Some(vec![1, 2, 3]),
                        name: None,
                        tsize: Some(10),
                    },
                    PBLink {
                        hash: Some(vec![4, 5, 6]),
                        name: Some(String::new()),
                        tsize: Some(20),
                    },
                    PBLink {
                        hash: Some(vec![7, 8, 9]),
                        name: Some("file.txt".to_string()),
                        tsize: Some(42),
                    },
                    PBLink {
                        hash: Some(vec![10, 11, 12]),
                        name: Some("nested".to_string()),
                        tsize: Some(0),
                    },
                ],
            };

            let op = Operator::new(
                IpfsBuilder::default()
                    .endpoint("https://ipfs.io")
                    .root(TEST_ROOT),
            )?
            .with_context(OperationContext::new().with_http_transport(
                HttpTransporter::new(MockIpfsTransport {
                    requests: requests.clone(),
                    list_status: StatusCode::OK,
                    pb_node,
                }),
            ));

            let entries = op.list("/").await?;
            let got: Vec<(&str, EntryMode)> = entries
                .iter()
                .map(|e| (e.path(), e.metadata().mode()))
                .collect();
            assert_eq!(
                got,
                vec![("file.txt", EntryMode::FILE), ("nested/", EntryMode::DIR)]
            );
            Ok(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_list_non_trailing_slash_path_normalizes_prefix() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let pb_node = PBNode {
                data: None,
                links: vec![
                    PBLink {
                        hash: None,
                        name: Some("file.txt".to_string()),
                        tsize: Some(42),
                    },
                    PBLink {
                        hash: None,
                        name: Some("nested".to_string()),
                        tsize: Some(0),
                    },
                ],
            };

            let core = Arc::new(IpfsCore {
                info: ServiceInfo::new(IPFS_SCHEME, TEST_ROOT, ""),
                capability: Capability {
                    stat: true,
                    list: true,
                    ..Default::default()
                },
                endpoint: "https://ipfs.io".to_string(),
                root: TEST_ROOT.to_string(),
            });
            let op_ctx = OperationContext::new().with_http_transport(HttpTransporter::new(
                MockIpfsTransport {
                    requests: requests.clone(),
                    list_status: StatusCode::OK,
                    pb_node,
                },
            ));

            for input_path in ["dir", "dir/"] {
                let stream = DirStream::new(core.clone(), op_ctx.clone(), input_path);
                let mut page_ctx = oio::PageContext {
                    done: false,
                    token: String::new(),
                    entries: VecDeque::new(),
                };

                stream.next_page(&mut page_ctx).await?;
                assert!(page_ctx.done);
                let got: Vec<(String, EntryMode)> = page_ctx
                    .entries
                    .iter()
                    .map(|e| (e.path().to_string(), e.mode()))
                    .collect();
                assert_eq!(
                    got,
                    vec![
                        ("dir/file.txt".to_string(), EntryMode::FILE),
                        ("dir/nested/".to_string(), EntryMode::DIR),
                    ]
                );
            }

            let recorded = requests.lock().expect("mutex poisoned");
            assert!(recorded.contains(&(
                Method::HEAD,
                format!("https://ipfs.io{TEST_ROOT}dir/file.txt")
            )));
            assert!(recorded.contains(&(
                Method::HEAD,
                format!("https://ipfs.io{TEST_ROOT}dir/nested")
            )));
            Ok(())
        })
        .await
        .expect("test timed out")
    }
}
