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

use std::convert::Infallible;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use dav_server_opendalfs::{OpendalFs, dav_server::DavHandler};
use http::{Request, Response, StatusCode, header, request::Builder};
use http_body_util::{BodyExt, Full};
use hyper::{client, server, service::service_fn};
use hyper_util::rt::TokioIo;
use opendal::{Operator, services::Fs};
use roxmltree::{Document, Node};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

struct WebDav {
    _root: TempDir,
    handler: DavHandler,
    listener: TcpListener,
    addr: SocketAddr,
}

impl WebDav {
    async fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let fs = Fs::default().root(root.path().to_str().context("non-UTF-8 test root")?);
        let handler = DavHandler::builder()
            .filesystem(OpendalFs::new(Operator::new(fs)?))
            .build_handler();
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        Ok(Self {
            _root: root,
            handler,
            listener,
            addr,
        })
    }

    fn request(&self, method: &str, path: &str) -> Builder {
        Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, self.addr.to_string())
            .header(header::CONNECTION, "close")
    }

    async fn send(
        &self,
        request: Builder,
        body: &'static [u8],
        expected: StatusCode,
    ) -> Result<Response<Bytes>> {
        let request = request.body(Full::new(Bytes::from_static(body)))?;
        let operation = format!("{} {}", request.method(), request.uri());
        let serve = async {
            let (stream, _) = self.listener.accept().await?;
            let service = service_fn(|request| async {
                Ok::<_, Infallible>(self.handler.handle(request).await)
            });
            server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await?;
            Ok::<_, anyhow::Error>(())
        };
        let exchange = async {
            let stream = TcpStream::connect(self.addr).await?;
            let (mut sender, connection) =
                client::conn::http1::handshake(TokioIo::new(stream)).await?;
            let receive = async {
                let (parts, body) = sender.send_request(request).await?.into_parts();
                Ok(Response::from_parts(
                    parts,
                    body.collect().await?.to_bytes(),
                ))
            };
            let (response, ()) = tokio::try_join!(receive, connection)?;
            Ok(response)
        };
        let response = timeout(Duration::from_secs(15), async {
            let (response, ()) = tokio::try_join!(exchange, serve)?;
            Ok::<_, anyhow::Error>(response)
        })
        .await
        .with_context(|| format!("{operation} timed out"))?
        .with_context(|| operation.clone())?;
        assert_eq!(
            response.status(),
            expected,
            "{operation}: {:?}",
            response.body()
        );
        Ok(response)
    }

    async fn propfind(&self, path: &str, depth: &str) -> Result<Vec<(String, Resource)>> {
        let request = self
            .request("PROPFIND", path)
            .header("Depth", depth)
            .header(header::CONTENT_TYPE, "application/xml");
        let response = self
            .send(
                request,
                br#"<propfind xmlns="DAV:"><prop><resourcetype/><getcontentlength/></prop></propfind>"#,
                StatusCode::MULTI_STATUS,
            )
            .await?;
        let xml = std::str::from_utf8(response.body())?;
        let document = Document::parse(xml)?;
        let root = document.root_element();
        assert!(root.has_tag_name(("DAV:", "multistatus")), "{xml}");
        let mut resources = Vec::new();
        for entry in root.children().filter(Node::is_element) {
            assert!(entry.has_tag_name(("DAV:", "response")), "{xml}");
            let href = dav_child(entry, "href")?.text().context("empty DAV:href")?;
            let properties = entry
                .children()
                .filter(|node| node.has_tag_name(("DAV:", "propstat")))
                .find(|node| {
                    dav_child(*node, "status")
                        .ok()
                        .and_then(|status| status.text())
                        .and_then(|status| status.split_whitespace().nth(1))
                        == Some("200")
                })
                .with_context(|| format!("missing successful property status for {href}: {xml}"))?;
            let properties = dav_child(properties, "prop")?;
            let kind = dav_child(properties, "resourcetype")?;
            let resource = if kind
                .children()
                .any(|node| node.has_tag_name(("DAV:", "collection")))
            {
                Resource::Collection
            } else {
                let length = dav_child(properties, "getcontentlength")?
                    .text()
                    .context("empty DAV:getcontentlength")?;
                Resource::File(length.parse()?)
            };
            resources.push((href.to_string(), resource));
        }
        resources.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(resources)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Resource {
    Collection,
    File(usize),
}

fn dav_child<'a, 'input>(node: Node<'a, 'input>, name: &str) -> Result<Node<'a, 'input>> {
    node.children()
        .find(|child| child.has_tag_name(("DAV:", name)))
        .with_context(|| format!("missing DAV:{name} under {}", node.tag_name().name()))
}

async fn file_lifecycle(dav: &WebDav, parent: &str, source: &str, moved: &str) -> Result<()> {
    let source = format!("{parent}{source}");
    let moved = format!("{parent}{moved}");
    let original = b"\x00original\xff\r\n";
    let replacement = b"\x00new\xff";

    dav.send(dav.request("PUT", &source), original, StatusCode::CREATED)
        .await?;
    let head = dav
        .send(dav.request("HEAD", &source), b"", StatusCode::OK)
        .await?;
    assert_eq!(
        head.headers()[header::CONTENT_LENGTH],
        original.len().to_string()
    );
    assert!(head.body().is_empty());
    assert_eq!(
        dav.propfind(&source, "0").await?,
        [(source.clone(), Resource::File(original.len()))]
    );

    // A partial PUT must fail without replacing the file.
    let partial_put = dav.request("PUT", &source).header(
        header::CONTENT_RANGE,
        format!("bytes 2-3/{}", original.len()),
    );
    dav.send(partial_put, b"XX", StatusCode::RANGE_NOT_SATISFIABLE)
        .await?;
    let get = dav
        .send(dav.request("GET", &source), b"", StatusCode::OK)
        .await?;
    assert_eq!(get.body().as_ref(), original);

    dav.send(
        dav.request("PUT", &source),
        replacement,
        StatusCode::NO_CONTENT,
    )
    .await?;
    let range = dav
        .request("GET", &source)
        .header(header::RANGE, "bytes=1-3");
    let response = dav.send(range, b"", StatusCode::PARTIAL_CONTENT).await?;
    assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 1-3/5");
    assert_eq!(response.body().as_ref(), b"new");
    assert_eq!(
        dav.propfind(parent, "1").await?,
        [
            (parent.to_string(), Resource::Collection),
            (source.clone(), Resource::File(replacement.len()))
        ]
    );

    let rename = dav
        .request("MOVE", &source)
        .header("Destination", format!("http://{}{moved}", dav.addr));
    dav.send(rename, b"", StatusCode::CREATED).await?;
    dav.send(dav.request("HEAD", &source), b"", StatusCode::NOT_FOUND)
        .await?;
    let get = dav
        .send(dav.request("GET", &moved), b"", StatusCode::OK)
        .await?;
    assert_eq!(get.body().as_ref(), replacement);

    let copy = dav
        .request("COPY", &moved)
        .header("Destination", format!("http://{}{source}", dav.addr));
    dav.send(copy, b"", StatusCode::CREATED).await?;
    dav.send(dav.request("DELETE", &moved), b"", StatusCode::NO_CONTENT)
        .await?;
    dav.send(dav.request("HEAD", &moved), b"", StatusCode::NOT_FOUND)
        .await?;
    let get = dav
        .send(dav.request("GET", &source), b"", StatusCode::OK)
        .await?;
    assert_eq!(get.body().as_ref(), replacement);
    dav.send(dav.request("DELETE", &source), b"", StatusCode::NO_CONTENT)
        .await?;
    assert_eq!(
        dav.propfind(parent, "1").await?,
        [(parent.to_string(), Resource::Collection)]
    );
    Ok(())
}

#[tokio::test]
async fn root_file_lifecycle() -> Result<()> {
    let dav = WebDav::new().await?;
    file_lifecycle(&dav, "/", "original.bin", "renamed.bin").await
}

#[tokio::test]
async fn nested_utf8_file_lifecycle() -> Result<()> {
    // /notes & 雪/冬 #1/
    let outer = "/notes%20%26%20%E9%9B%AA/";
    let inner = format!("{outer}%E5%86%AC%20%231/");
    let dav = WebDav::new().await?;
    dav.send(dav.request("MKCOL", &inner), b"", StatusCode::CONFLICT)
        .await?;
    dav.send(dav.request("MKCOL", outer), b"", StatusCode::CREATED)
        .await?;
    dav.send(
        dav.request("MKCOL", inner.trim_end_matches('/')),
        b"",
        StatusCode::CREATED,
    )
    .await?;
    assert_eq!(
        dav.propfind(outer, "1").await?,
        [
            (outer.to_string(), Resource::Collection),
            (inner.clone(), Resource::Collection)
        ]
    );

    // été 100%.bin and 冬 #2.bin
    file_lifecycle(
        &dav,
        &inner,
        "%C3%A9t%C3%A9%20100%25.bin",
        "%E5%86%AC%20%232.bin",
    )
    .await?;

    dav.send(dav.request("DELETE", &inner), b"", StatusCode::NO_CONTENT)
        .await?;
    assert_eq!(
        dav.propfind(outer, "1").await?,
        [(outer.to_string(), Resource::Collection)]
    );
    dav.send(dav.request("DELETE", outer), b"", StatusCode::NO_CONTENT)
        .await?;
    assert_eq!(
        dav.propfind("/", "1").await?,
        [("/".to_string(), Resource::Collection)]
    );
    Ok(())
}

#[tokio::test]
async fn mkcol_root_and_missing_parent() -> Result<()> {
    // MKCOL / must fail with 405 instead of panicking on the missing parent
    // of "/"; the connection and server stay usable afterwards.
    let dav = WebDav::new().await?;
    dav.send(
        dav.request("MKCOL", "/"),
        b"",
        StatusCode::METHOD_NOT_ALLOWED,
    )
    .await?;

    dav.send(
        dav.request("MKCOL", "/missing/child/"),
        b"",
        StatusCode::CONFLICT,
    )
    .await?;
    dav.send(dav.request("MKCOL", "/alive/"), b"", StatusCode::CREATED)
        .await?;
    assert_eq!(
        dav.propfind("/", "1").await?,
        [
            ("/".to_string(), Resource::Collection),
            ("/alive/".to_string(), Resource::Collection)
        ]
    );
    Ok(())
}

#[tokio::test]
async fn move_non_empty_collection() -> Result<()> {
    // The fs backend supports native rename, so MOVE on a non-empty
    // collection must relocate it instead of failing with 501.
    let dav = WebDav::new().await?;
    dav.send(dav.request("MKCOL", "/dir/"), b"", StatusCode::CREATED)
        .await?;
    dav.send(
        dav.request("MKCOL", "/dir/nested/"),
        b"",
        StatusCode::CREATED,
    )
    .await?;
    let content = b"nested file";
    dav.send(
        dav.request("PUT", "/dir/nested/blob.bin"),
        content,
        StatusCode::CREATED,
    )
    .await?;
    dav.send(
        dav.request("PUT", "/dir/top.bin"),
        b"top",
        StatusCode::CREATED,
    )
    .await?;

    let move_request = dav
        .request("MOVE", "/dir/")
        .header("Destination", format!("http://{}/moved/", dav.addr));
    dav.send(move_request, b"", StatusCode::CREATED).await?;

    dav.send(
        dav.request("PROPFIND", "/dir/").header("Depth", "0"),
        b"",
        StatusCode::NOT_FOUND,
    )
    .await?;
    assert_eq!(
        dav.propfind("/moved/", "1").await?,
        [
            ("/moved/".to_string(), Resource::Collection),
            ("/moved/nested/".to_string(), Resource::Collection),
            ("/moved/top.bin".to_string(), Resource::File(3))
        ]
    );
    let get = dav
        .send(
            dav.request("GET", "/moved/nested/blob.bin"),
            b"",
            StatusCode::OK,
        )
        .await?;
    assert_eq!(get.body().as_ref(), content);

    // MOVE with `Overwrite: F` onto an existing collection fails with 412.
    dav.send(dav.request("MKCOL", "/occupied/"), b"", StatusCode::CREATED)
        .await?;
    let overwrite_f = dav
        .request("MOVE", "/moved/")
        .header("Destination", format!("http://{}/occupied/", dav.addr))
        .header("Overwrite", "F");
    dav.send(overwrite_f, b"", StatusCode::PRECONDITION_FAILED)
        .await?;

    // With `Overwrite: T` the destination is replaced atomically (204).
    let overwrite_t = dav
        .request("MOVE", "/moved/")
        .header("Destination", format!("http://{}/occupied/", dav.addr))
        .header("Overwrite", "T");
    dav.send(overwrite_t, b"", StatusCode::NO_CONTENT).await?;
    dav.send(
        dav.request("PROPFIND", "/moved/").header("Depth", "0"),
        b"",
        StatusCode::NOT_FOUND,
    )
    .await?;
    assert_eq!(
        dav.propfind("/occupied/", "1").await?,
        [
            ("/occupied/".to_string(), Resource::Collection),
            ("/occupied/nested/".to_string(), Resource::Collection),
            ("/occupied/top.bin".to_string(), Resource::File(3))
        ]
    );
    let get = dav
        .send(
            dav.request("GET", "/occupied/nested/blob.bin"),
            b"",
            StatusCode::OK,
        )
        .await?;
    assert_eq!(get.body().as_ref(), content);
    Ok(())
}
