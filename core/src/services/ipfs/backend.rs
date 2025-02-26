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

use http::Request;
use http::Response;
use http::StatusCode;
use log::debug;
use prost::Message;

use super::error::parse_error;
use super::ipld::PBNode;
use crate::raw::*;
use crate::services::IpfsConfig;
use crate::*;

impl Configurator for IpfsConfig {
    type Builder = IpfsBuilder;
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
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for IpfsBuilder {
    const SCHEME: Scheme = Scheme::Ipfs;
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
        debug!("backend use root {}", root);

        let endpoint = match &self.config.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "endpoint is empty")
                .with_context("service", Scheme::Ipfs)
                .with_context("root", &root)),
        }?;
        debug!("backend use endpoint {}", &endpoint);

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Ipfs)
            })?
        };

        Ok(IpfsBackend {
            info: {
                let ma = AccessorInfo::default();
                ma.set_scheme(Scheme::Ipfs)
                    .set_root(&root)
                    .set_native_capability(Capability {
                        stat: true,
                        stat_has_content_length: true,
                        stat_has_content_type: true,
                        stat_has_etag: true,
                        stat_has_content_disposition: true,

                        read: true,

                        list: true,

                        shared: true,

                        ..Default::default()
                    });

                ma.into()
            },
            root,
            endpoint,
            client,
        })
    }
}

/// Backend for IPFS.
#[derive(Clone)]
pub struct IpfsBackend {
    info: Arc<AccessorInfo>,
    endpoint: String,
    root: String,
    client: HttpClient,
}

impl Debug for IpfsBackend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("client", &self.client)
            .finish()
    }
}

impl Access for IpfsBackend {
    type Reader = HttpBody;
    type Writer = ();
    type Lister = oio::PageLister<DirStream>;
    type Deleter = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();
    type BlockingDeleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    /// IPFS's stat behavior highly depends on its implementation.
    ///
    /// Based on IPFS [Path Gateway Specification](https://github.com/ipfs/specs/blob/main/http-gateways/PATH_GATEWAY.md),
    /// response payload could be:
    ///
    /// > - UnixFS (implicit default)
    /// >   - File
    /// >     - Bytes representing file contents
    /// >   - Directory
    /// >     - Generated HTML with directory index
    /// >     - When `index.html` is present, gateway can skip generating directory index and return it instead
    /// > - Raw block (not this case)
    /// > - CAR (not this case)
    ///
    /// When we HEAD a given path, we could have the following responses:
    ///
    /// - File
    ///
    /// ```http
    /// :) curl -I https://ipfs.io/ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ/normal_file
    /// HTTP/1.1 200 Connection established
    ///
    /// HTTP/2 200
    /// server: openresty
    /// date: Thu, 08 Sep 2022 00:48:50 GMT
    /// content-type: application/octet-stream
    /// content-length: 262144
    /// access-control-allow-methods: GET
    /// cache-control: public, max-age=29030400, immutable
    /// etag: "QmdP6teFTLSNVhT4W5jkhEuUBsjQ3xkp1GmRvDU6937Me1"
    /// x-ipfs-gateway-host: ipfs-bank11-fr2
    /// x-ipfs-path: /ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ/normal_file
    /// x-ipfs-roots: QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ,QmdP6teFTLSNVhT4W5jkhEuUBsjQ3xkp1GmRvDU6937Me1
    /// x-ipfs-pop: ipfs-bank11-fr2
    /// timing-allow-origin: *
    /// x-ipfs-datasize: 262144
    /// access-control-allow-origin: *
    /// access-control-allow-methods: GET, POST, OPTIONS
    /// access-control-allow-headers: X-Requested-With, Range, Content-Range, X-Chunked-Output, X-Stream-Output
    /// access-control-expose-headers: Content-Range, X-Chunked-Output, X-Stream-Output
    /// x-ipfs-lb-pop: gateway-bank1-fr2
    /// strict-transport-security: max-age=31536000; includeSubDomains; preload
    /// x-proxy-cache: MISS
    /// accept-ranges: bytes
    /// ```
    ///
    /// - Dir with generated index
    ///
    /// ```http
    /// :( curl -I https://ipfs.io/ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ/normal_dir
    /// HTTP/1.1 200 Connection established
    ///
    /// HTTP/2 200
    /// server: openresty
    /// date: Wed, 07 Sep 2022 08:46:13 GMT
    /// content-type: text/html
    /// vary: Accept-Encoding
    /// access-control-allow-methods: GET
    /// etag: "DirIndex-2b567f6r5vvdg_CID-QmY44DyCDymRN1Qy7sGbupz1ysMkXTWomAQku5vBg7fRQW"
    /// x-ipfs-gateway-host: ipfs-bank6-sg1
    /// x-ipfs-path: /ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ/normal_dir
    /// x-ipfs-roots: QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ,QmY44DyCDymRN1Qy7sGbupz1ysMkXTWomAQku5vBg7fRQW
    /// x-ipfs-pop: ipfs-bank6-sg1
    /// timing-allow-origin: *
    /// access-control-allow-origin: *
    /// access-control-allow-methods: GET, POST, OPTIONS
    /// access-control-allow-headers: X-Requested-With, Range, Content-Range, X-Chunked-Output, X-Stream-Output
    /// access-control-expose-headers: Content-Range, X-Chunked-Output, X-Stream-Output
    /// x-ipfs-lb-pop: gateway-bank3-sg1
    /// strict-transport-security: max-age=31536000; includeSubDomains; preload
    /// x-proxy-cache: MISS
    /// ```
    ///
    /// - Dir with index.html
    ///
    /// ```http
    /// :) curl -I http://127.0.0.1:8080/ipfs/QmVturFGV3z4WsP7cRV8Ci4avCdGWYXk2qBKvtAwFUp5Az
    /// HTTP/1.1 302 Found
    /// Access-Control-Allow-Headers: Content-Type
    /// Access-Control-Allow-Headers: Range
    /// Access-Control-Allow-Headers: User-Agent
    /// Access-Control-Allow-Headers: X-Requested-With
    /// Access-Control-Allow-Methods: GET
    /// Access-Control-Allow-Origin: *
    /// Access-Control-Expose-Headers: Content-Length
    /// Access-Control-Expose-Headers: Content-Range
    /// Access-Control-Expose-Headers: X-Chunked-Output
    /// Access-Control-Expose-Headers: X-Ipfs-Path
    /// Access-Control-Expose-Headers: X-Ipfs-Roots
    /// Access-Control-Expose-Headers: X-Stream-Output
    /// Content-Type: text/html; charset=utf-8
    /// Location: /ipfs/QmVturFGV3z4WsP7cRV8Ci4avCdGWYXk2qBKvtAwFUp5Az/
    /// X-Ipfs-Path: /ipfs/QmVturFGV3z4WsP7cRV8Ci4avCdGWYXk2qBKvtAwFUp5Az
    /// X-Ipfs-Roots: QmVturFGV3z4WsP7cRV8Ci4avCdGWYXk2qBKvtAwFUp5Az
    /// Date: Thu, 08 Sep 2022 00:52:29 GMT
    /// ```
    ///
    /// In conclusion:
    ///
    /// - HTTP Status Code == 302 => directory
    /// - HTTP Status Code == 200 && ETag starts with `"DirIndex` => directory
    /// - HTTP Status Code == 200 && ETag not starts with `"DirIndex` => file
    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(RpStat::new(Metadata::new(EntryMode::DIR)));
        }

        let resp = self.ipfs_head(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut m = Metadata::new(EntryMode::Unknown);

                if let Some(v) = parse_content_length(resp.headers())? {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_content_type(resp.headers())? {
                    m.set_content_type(v);
                }

                if let Some(v) = parse_etag(resp.headers())? {
                    m.set_etag(v);

                    if v.starts_with("\"DirIndex") {
                        m.set_mode(EntryMode::DIR);
                    } else {
                        m.set_mode(EntryMode::FILE);
                    }
                } else {
                    // Some service will stream the output of DirIndex.
                    // If we don't have an etag, it's highly to be a dir.
                    m.set_mode(EntryMode::DIR);
                }

                if let Some(v) = parse_content_disposition(resp.headers())? {
                    m.set_content_disposition(v);
                }

                Ok(RpStat::new(m))
            }
            StatusCode::FOUND | StatusCode::MOVED_PERMANENTLY => {
                Ok(RpStat::new(Metadata::new(EntryMode::DIR)))
            }
            _ => Err(parse_error(resp)),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let resp = self.ipfs_get(path, args.range()).await?;

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
        let l = DirStream::new(Arc::new(self.clone()), path);
        Ok((RpList::default(), oio::PageLister::new(l)))
    }
}

impl IpfsBackend {
    pub async fn ipfs_get(&self, path: &str, range: BytesRange) -> Result<Response<HttpBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if !range.is_full() {
            req = req.header(http::header::RANGE, range.to_header());
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.fetch(req).await
    }

    async fn ipfs_head(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let req = Request::head(&url);

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }

    async fn ipfs_list(&self, path: &str) -> Result<Response<Buffer>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        // Use "application/vnd.ipld.raw" to disable IPLD codec deserialization
        // OpenDAL will parse ipld data directly.
        //
        // ref: https://github.com/ipfs/specs/blob/main/http-gateways/PATH_GATEWAY.md
        req = req.header(http::header::ACCEPT, "application/vnd.ipld.raw");

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        self.client.send(req).await
    }
}

pub struct DirStream {
    backend: Arc<IpfsBackend>,
    path: String,
}

impl DirStream {
    fn new(backend: Arc<IpfsBackend>, path: &str) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::PageList for DirStream {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self.backend.ipfs_list(&self.path).await?;

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
            let meta = self
                .backend
                .stat(&name, OpStat::new())
                .await?
                .into_metadata();

            if meta.mode().is_dir() {
                name += "/";
            }

            ctx.entries.push_back(oio::Entry::new(&name, meta))
        }

        ctx.done = true;
        Ok(())
    }
}
