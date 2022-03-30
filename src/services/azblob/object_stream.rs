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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use bytes::Buf;
use bytes::BufMut;
use futures::future::BoxFuture;
use futures::ready;
use futures::StreamExt;
use log::debug;
use quick_xml::de;
use serde::Deserialize;

use super::Backend;
use crate::error::Error;
use crate::error::Kind;
use crate::error::Result;
use crate::Object;
use crate::ObjectMode;

pub struct AzblobObjectStream {
    backend: Backend,
    path: String,

    next_marker: String,
    done: bool,
    state: State,
}
enum State {
    Idle,
    Sending(BoxFuture<'static, Result<bytes::Bytes>>),
    Listing((Output, usize, usize)),
}
impl AzblobObjectStream {
    pub fn new(backend: Backend, path: String) -> Self {
        Self {
            backend,
            path,

            next_marker: "".to_string(),
            done: false,
            state: State::Idle,
        }
    }
}

impl futures::Stream for AzblobObjectStream {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();

        match &mut self.state {
            State::Idle => {
                let backend = self.backend.clone();
                let path = self.path.clone();
                let next_marker = self.next_marker.clone();
                let fut = async move {
                    let mut resp = backend.list_objects(&path, &next_marker).await?;

                    if resp.status() != http::StatusCode::OK {
                        let e = Err(Error::Object {
                            kind: Kind::Unexpected,
                            op: "list",
                            path: path.clone(),
                            source: anyhow!("{:?}", resp),
                        });
                        debug!("error response: {:?}", resp);
                        return e;
                    }

                    let body = resp.body_mut();
                    let mut bs = bytes::BytesMut::new();
                    while let Some(b) = body.next().await {
                        let b = b.map_err(|e| Error::Object {
                            kind: Kind::Unexpected,
                            op: "list",
                            path: path.clone(),
                            source: anyhow!("read body: {:?}", e),
                        })?;
                        bs.put_slice(&b)
                    }

                    Ok(bs.freeze())
                };
                self.state = State::Sending(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Sending(fut) => {
                let bs = ready!(Pin::new(fut).poll(cx))?;
                let output: Output = de::from_reader(bs.reader()).map_err(|e| Error::Object {
                    kind: Kind::Unexpected,
                    op: "list",
                    path: self.path.clone(),
                    source: anyhow!("deserialize list_bucket output: {:?}", e),
                })?;

                // Try our best to check whether this list is done.
                //
                // - Check `next_marker`
                if let Some(next_marker) = output.next_marker.as_ref() {
                    self.done = next_marker.is_empty();
                };
                self.next_marker = output.next_marker.clone().unwrap_or_default();
                self.state = State::Listing((output, 0, 0));
                self.poll_next(cx)
            }
            State::Listing((output, common_prefixes_idx, objects_idx)) => {
                if let Some(prefixes) = &output.blobs.blob_prefix {
                    if *common_prefixes_idx < prefixes.len() {
                        *common_prefixes_idx += 1;
                        let prefix = &prefixes[*common_prefixes_idx - 1].name;

                        let mut o =
                            Object::new(Arc::new(backend.clone()), &backend.get_rel_path(prefix));
                        let meta = o.metadata_mut();
                        meta.set_mode(ObjectMode::DIR)
                            .set_content_length(0)
                            .set_complete();

                        debug!(
                            "object {} got entry, path: {}, mode: {}",
                            &self.path,
                            meta.path(),
                            meta.mode()
                        );
                        return Poll::Ready(Some(Ok(o)));
                    }
                };

                let objects = &output.blobs.blob;
                if *objects_idx < objects.len() {
                    *objects_idx += 1;
                    let object = &objects[*objects_idx - 1];

                    let mut o = Object::new(
                        Arc::new(backend.clone()),
                        &backend.get_rel_path(&object.name),
                    );
                    let meta = o.metadata_mut();
                    meta.set_mode(ObjectMode::FILE)
                        .set_content_length(object.properties.content_length as u64);

                    debug!(
                        "object {} got entry, path: {}, mode: {}",
                        &self.path,
                        meta.path(),
                        meta.mode()
                    );
                    return Poll::Ready(Some(Ok(o)));
                }

                if self.done {
                    debug!("object {} list done", &self.path);
                    return Poll::Ready(None);
                }

                self.state = State::Idle;
                self.poll_next(cx)
            }
        }
    }
}
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Output {
    blobs: Blobs,
    #[serde(rename = "NextMarker")]
    next_marker: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Blobs {
    blob: Vec<Blob>,
    blob_prefix: Option<Vec<BlobPrefix>>,
}
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct BlobPrefix {
    name: String,
}
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Blob {
    properties: Properties,
    name: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Properties {
    #[serde(rename = "Content-Length")]
    content_length: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse_xml() {
        let bs = bytes::Bytes::from(
            r#" 
            \xef\xbb\xbf
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults ServiceEndpoint="https://d2lark.blob.core.windows.net/" ContainerName="myazurebucket">
                <Prefix>dir1/</Prefix>
                <Delimiter>/</Delimiter>
                <Blobs>
                    <Blob>
                        <Name>dir1/2f018bb5-466f-4af1-84fa-2b167374ee06</Name>
                        <Properties>
                            <Creation-Time>Sun, 20 Mar 2022 11:29:03 GMT</Creation-Time>
                            <Last-Modified>Sun, 20 Mar 2022 11:29:03 GMT</Last-Modified>
                            <Etag>0x8DA0A64D66790C3</Etag>
                            <Content-Length>3485277</Content-Length>
                            <Content-Type>application/octet-stream</Content-Type>
                            <Content-Encoding />
                            <Content-Language />
                            <Content-CRC64 />
                            <Content-MD5>llJ/+jOlx5GdA1sL7SdKuw==</Content-MD5>
                            <Cache-Control />
                            <Content-Disposition />
                            <BlobType>BlockBlob</BlobType>
                            <AccessTier>Hot</AccessTier>
                            <AccessTierInferred>true</AccessTierInferred>
                            <LeaseStatus>unlocked</LeaseStatus>
                            <LeaseState>available</LeaseState>
                            <ServerEncrypted>true</ServerEncrypted>
                        </Properties>
                        <OrMetadata />
                    </Blob>
                    <Blob>
                        <Name>dir1/5b9432b2-79c0-48d8-90c2-7d3e153826ed</Name>
                        <Properties>
                            <Creation-Time>Tue, 29 Mar 2022 01:54:07 GMT</Creation-Time>
                            <Last-Modified>Tue, 29 Mar 2022 01:54:07 GMT</Last-Modified>
                            <Etag>0x8DA112702D88FE4</Etag>
                            <Content-Length>2471869</Content-Length>
                            <Content-Type>application/octet-stream</Content-Type>
                            <Content-Encoding />
                            <Content-Language />
                            <Content-CRC64 />
                            <Content-MD5>xmgUltSnopLSJOukgCHFtg==</Content-MD5>
                            <Cache-Control />
                            <Content-Disposition />
                            <BlobType>BlockBlob</BlobType>
                            <AccessTier>Hot</AccessTier>
                            <AccessTierInferred>true</AccessTierInferred>
                            <LeaseStatus>unlocked</LeaseStatus>
                            <LeaseState>available</LeaseState>
                            <ServerEncrypted>true</ServerEncrypted>
                        </Properties>
                        <OrMetadata />
                    </Blob>
                    <Blob>
                        <Name>dir1/b2d96f8b-d467-40d1-bb11-4632dddbf5b5</Name>
                        <Properties>
                            <Creation-Time>Sun, 20 Mar 2022 11:31:57 GMT</Creation-Time>
                            <Last-Modified>Sun, 20 Mar 2022 11:31:57 GMT</Last-Modified>
                            <Etag>0x8DA0A653DC82981</Etag>
                            <Content-Length>1259677</Content-Length>
                            <Content-Type>application/octet-stream</Content-Type>
                            <Content-Encoding />
                            <Content-Language />
                            <Content-CRC64 />
                            <Content-MD5>AxTiFXHwrXKaZC5b7ZRybw==</Content-MD5>
                            <Cache-Control />
                            <Content-Disposition />
                            <BlobType>BlockBlob</BlobType>
                            <AccessTier>Hot</AccessTier>
                            <AccessTierInferred>true</AccessTierInferred>
                            <LeaseStatus>unlocked</LeaseStatus>
                            <LeaseState>available</LeaseState>
                            <ServerEncrypted>true</ServerEncrypted>
                        </Properties>
                        <OrMetadata />
                    </Blob>
                    <BlobPrefix>
                        <Name>dir1/dir2/</Name>
                    </BlobPrefix>
                    <BlobPrefix>
                        <Name>dir1/dir21/</Name>
                    </BlobPrefix>
                </Blobs>
                <NextMarker />
            </EnumerationResults>"#,
        );
        let out: Output = de::from_reader(bs.reader()).expect("must success");
        println!("{:?}", out);

        assert_eq!(
            out.blobs
                .blob
                .iter()
                .map(|v| v.name.clone())
                .collect::<Vec<String>>(),
            [
                "dir1/2f018bb5-466f-4af1-84fa-2b167374ee06",
                "dir1/5b9432b2-79c0-48d8-90c2-7d3e153826ed",
                "dir1/b2d96f8b-d467-40d1-bb11-4632dddbf5b5"
            ]
        );
        assert_eq!(
            out.blobs
                .blob
                .iter()
                .map(|v| v.properties.content_length.clone())
                .collect::<Vec<u64>>(),
            [3485277, 2471869, 1259677]
        );
        assert_eq!(
            out.blobs
                .blob_prefix
                .unwrap()
                .iter()
                .map(|v| v.name.clone())
                .collect::<Vec<String>>(),
            ["dir1/dir2/", "dir1/dir21/"]
        );
    }
}
