// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Result;
use std::iter::Peekable;
use std::mem;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::vec::IntoIter;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::ready;
use futures::Future;
use futures::Stream;
use http::Request;
use http::Response;
use http::StatusCode;
use log::info;
use prost::Message;

use super::error::parse_error;
use super::ipld::PBNode;
use crate::accessor::AccessorCapability;
use crate::error::new_other_backend_error;
use crate::error::new_other_object_error;
use crate::http_util::new_request_build_error;
use crate::http_util::new_request_send_error;
use crate::http_util::parse_content_length;
use crate::http_util::parse_error_response;
use crate::http_util::parse_etag;
use crate::http_util::percent_encode_path;
use crate::http_util::AsyncBody;
use crate::http_util::HttpClient;
use crate::http_util::IncomingAsyncBody;
use crate::ops::BytesRange;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::Operation;
use crate::path::build_rooted_abs_path;
use crate::path::normalize_root;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::ObjectEntry;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectStreamer;
use crate::Scheme;

/// Builder for ipfs backend.
#[derive(Default, Clone, Debug)]
pub struct Builder {
    endpoint: Option<String>,
    root: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                _ => continue,
            };
        }

        builder
    }

    /// Set root of ipfs backend.
    ///
    /// Root must be a valid ipfs address like the following:
    ///
    /// - `/ipfs/QmPpCt1aYGb9JWJRmXRUnmJtVgeFFTJGzWFYEEX7bo9zGJ/` (IPFS with CID v0)
    /// - `/ipfs/bafybeibozpulxtpv5nhfa2ue3dcjx23ndh3gwr5vwllk7ptoyfwnfjjr4q/` (IPFS with  CID v1)
    /// - `/ipns/opendal.databend.rs/` (IPNS)
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.root = Some(root.to_string())
        }

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
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        if !endpoint.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }

        self
    }

    /// Consume builder to build an ipfs backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
        info!("backend build started: {:?}", &self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        if !root.starts_with("/ipfs/") && !root.starts_with("/ipns/") {
            return Err(new_other_backend_error(
                HashMap::from([("root".to_string(), root)]),
                anyhow!("root must start with /ipfs/ or /ipns/"),
            ));
        }
        info!("backend use root {}", root);

        let endpoint = match &self.endpoint {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(new_other_backend_error(
                HashMap::from([("endpoint".to_string(), "".to_string())]),
                anyhow!("endpoint is empty"),
            )),
        }?;
        info!("backend use endpoint {}", &endpoint);

        info!("backend build finished: {:?}", &self);
        Ok(Backend {
            root,
            endpoint,
            client: HttpClient::new(),
        })
    }
}

/// Backend for IPFS.
#[derive(Clone)]
pub struct Backend {
    endpoint: String,
    root: String,
    client: HttpClient,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend")
            .field("endpoint", &self.endpoint)
            .field("root", &self.root)
            .field("client", &self.client)
            .finish()
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut ma = AccessorMetadata::default();
        ma.set_scheme(Scheme::Ipfs)
            .set_root(&self.root)
            .set_capabilities(AccessorCapability::Read | AccessorCapability::List);

        ma
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<BytesReader> {
        let resp = self.ipfs_get(path, args.offset(), args.size()).await?;

        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(resp.into_body().reader()),
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Read, path, er);
                Err(err)
            }
        }
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
    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        // Stat root always returns a DIR.
        if path == "/" {
            return Ok(ObjectMetadata::new(ObjectMode::DIR));
        }

        let resp = self.ipfs_head(path).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let mut m = ObjectMetadata::new(ObjectMode::Unknown);

                if let Some(v) = parse_content_length(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_content_length(v);
                }

                if let Some(v) = parse_etag(resp.headers())
                    .map_err(|e| new_other_object_error(Operation::Stat, path, e))?
                {
                    m.set_etag(v);

                    if v.starts_with("\"DirIndex") {
                        m.set_mode(ObjectMode::DIR);
                    } else {
                        m.set_mode(ObjectMode::FILE);
                    }
                } else {
                    // Some service will stream the output of DirIndex.
                    // If we don't have an etag, it's highly to be a dir.
                    m.set_mode(ObjectMode::DIR);
                }

                Ok(m)
            }
            StatusCode::FOUND | StatusCode::MOVED_PERMANENTLY => {
                Ok(ObjectMetadata::new(ObjectMode::DIR))
            }
            _ => {
                let er = parse_error_response(resp).await?;
                let err = parse_error(Operation::Stat, path, er);
                Err(err)
            }
        }
    }

    async fn list(&self, path: &str, _: OpList) -> Result<ObjectStreamer> {
        Ok(Box::new(DirStream::new(Arc::new(self.clone()), path)))
    }
}

impl Backend {
    async fn ipfs_get(
        &self,
        path: &str,
        offset: Option<u64>,
        size: Option<u64>,
    ) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        if offset.is_some() || size.is_some() {
            req = req.header(
                http::header::RANGE,
                BytesRange::new(offset, size).to_string(),
            );
        }

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Read, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Read, path, e))
    }

    async fn ipfs_head(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let req = Request::head(&url);

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Stat, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Stat, path, e))
    }

    async fn ipfs_list(&self, path: &str) -> Result<Response<IncomingAsyncBody>> {
        let p = build_rooted_abs_path(&self.root, path);

        let url = format!("{}{}", self.endpoint, percent_encode_path(&p));

        let mut req = Request::get(&url);

        // Use "application/vnd.ipld.raw" to disable IPLD codec deserialization
        // OpenDAL will parse ipld data directly.
        //
        // ref: https://github.com/ipfs/specs/blob/main/http-gateways/PATH_GATEWAY.md
        req = req.header(http::header::ACCEPT, "application/vnd.ipld.raw");

        let req = req
            .body(AsyncBody::Empty)
            .map_err(|e| new_request_build_error(Operation::Read, path, e))?;

        self.client
            .send_async(req)
            .await
            .map_err(|e| new_request_send_error(Operation::Read, path, e))
    }
}

struct DirStream {
    backend: Arc<Backend>,
    path: String,
    state: State,
}

enum State {
    Idle,
    Listing(BoxFuture<'static, Result<Bytes>>),
    Walking(
        (
            Peekable<IntoIter<String>>,
            Option<BoxFuture<'static, Result<ObjectMetadata>>>,
        ),
    ),
}

impl DirStream {
    fn new(backend: Arc<Backend>, path: &str) -> Self {
        Self {
            backend,
            path: path.to_string(),
            state: State::Idle,
        }
    }
}

impl Stream for DirStream {
    type Item = Result<ObjectEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();
        let path = self.path.clone();

        match &mut self.state {
            State::Idle => {
                let fut = async move {
                    let resp = backend.ipfs_list(&path).await?;

                    if resp.status() != StatusCode::OK {
                        let er = parse_error_response(resp).await?;
                        let err = parse_error(Operation::List, &path, er);
                        return Err(err);
                    }

                    let bs = resp
                        .into_body()
                        .bytes()
                        .await
                        .map_err(|e| new_other_object_error(Operation::List, &path, e))?;

                    Ok(bs)
                };

                self.state = State::Listing(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Listing(fut) => {
                let bs = ready!(Pin::new(fut).poll(cx))?;

                let pb_node = PBNode::decode(bs).map_err(|e| {
                    new_other_object_error(
                        Operation::List,
                        &self.path,
                        anyhow!("deserialize protobuf: {e:?}"),
                    )
                })?;

                let names = pb_node
                    .links
                    .into_iter()
                    .map(|v| v.name.unwrap())
                    .collect::<Vec<String>>()
                    .into_iter()
                    .peekable();

                self.state = State::Walking((names, None));
                self.poll_next(cx)
            }
            State::Walking((names, fut)) => {
                if let Some(fut) = fut {
                    let meta = ready!(Pin::new(fut).poll(cx))?;

                    let mut name = names.next().expect("iterator must have next");

                    if meta.mode().is_dir() {
                        name += "/";
                    }

                    let de = ObjectEntry::new(backend, &name, meta).with_complete();

                    let names = mem::replace(names, vec![].into_iter().peekable());
                    self.state = State::Walking((names, None));
                    return Poll::Ready(Some(Ok(de)));
                }

                let name = if let Some(name) = names.peek() {
                    name.clone()
                } else {
                    return Poll::Ready(None);
                };

                let fut = async move { backend.stat(&name, OpStat::new()).await };
                let names = mem::replace(names, vec![].into_iter().peekable());

                self.state = State::Walking((names, Some(Box::pin(fut))));
                self.poll_next(cx)
            }
        }
    }
}
