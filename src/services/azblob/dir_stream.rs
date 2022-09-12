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
use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use bytes::Buf;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::ready;
use quick_xml::de;
use serde::Deserialize;
use time::format_description::well_known::Rfc2822;
use time::OffsetDateTime;

use super::error::parse_error;
use super::Backend;
use crate::error::other;
use crate::error::ObjectError;
use crate::http_util::parse_error_response;
use crate::ops::Operation;
use crate::path::build_rel_path;
use crate::DirEntry;
use crate::ObjectMode;

pub struct DirStream {
    backend: Arc<Backend>,
    root: String,
    path: String,

    next_marker: String,
    done: bool,
    state: State,
}

enum State {
    Idle,
    Sending(BoxFuture<'static, Result<Bytes>>),
    Listing((Output, usize, usize)),
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, root: String, path: String) -> Self {
        Self {
            backend,
            root,
            path,

            next_marker: "".to_string(),
            done: false,
            state: State::Idle,
        }
    }
}

impl futures::Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();
        let root = self.root.clone();

        match &mut self.state {
            State::Idle => {
                let path = self.path.clone();
                let next_marker = self.next_marker.clone();

                let fut = async move {
                    let resp = backend.azblob_list_blobs(&path, &next_marker).await?;

                    if resp.status() != http::StatusCode::OK {
                        let er = parse_error_response(resp).await?;
                        let err = parse_error(Operation::List, &path, er);
                        return Err(err);
                    }

                    let bs = resp
                        .into_body()
                        .bytes()
                        .await
                        .map_err(|e| other(ObjectError::new(Operation::List, &path, e)))?;

                    Ok(bs)
                };
                self.state = State::Sending(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Sending(fut) => {
                let bs = ready!(Pin::new(fut).poll(cx))?;

                let output: Output = de::from_reader(bs.reader()).map_err(|e| {
                    other(ObjectError::new(
                        Operation::List,
                        &self.path,
                        anyhow!("deserialize xml: {e:?}"),
                    ))
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

                        let de =
                            DirEntry::new(backend, ObjectMode::DIR, &build_rel_path(&root, prefix));

                        return Poll::Ready(Some(Ok(de)));
                    }
                };

                let objects = &output.blobs.blob;
                while *objects_idx < objects.len() {
                    let object = &objects[*objects_idx];
                    *objects_idx += 1;

                    // azblob could return the dir itself in contents
                    // which endswith `/`.
                    // We should ignore them.
                    if object.name.ends_with('/') {
                        continue;
                    }

                    let mut de = DirEntry::new(
                        backend,
                        ObjectMode::FILE,
                        &build_rel_path(&root, &object.name),
                    );

                    de.set_etag(object.properties.etag.as_str());
                    de.set_content_length(object.properties.content_length);
                    de.set_content_md5(object.properties.content_md5.as_str());

                    let dt =
                        OffsetDateTime::parse(object.properties.last_modified.as_str(), &Rfc2822)
                            .map_err(|e| {
                            other(ObjectError::new(
                                Operation::List,
                                &self.path,
                                anyhow!("parse last modified RFC2822 datetime: {e:?}"),
                            ))
                        })?;
                    de.set_last_modified(dt);

                    return Poll::Ready(Some(Ok(de)));
                }

                if self.done {
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
    #[serde(rename = "Last-Modified")]
    last_modified: String,
    #[serde(rename = "Content-MD5")]
    content_md5: String,
    etag: String,
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_parse_xml() {
        let bs = bytes::Bytes::from(
            r#"
            <?xml version="1.0" encoding="utf-8"?>
            <EnumerationResults ServiceEndpoint="https://test.blob.core.windows.net/" ContainerName="myazurebucket">
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
                .map(|v| v.properties.content_length)
                .collect::<Vec<u64>>(),
            [3485277, 2471869, 1259677]
        );
        assert_eq!(
            out.blobs
                .blob
                .iter()
                .map(|v| v.properties.content_md5.clone())
                .collect::<Vec<String>>(),
            [
                "llJ/+jOlx5GdA1sL7SdKuw==".to_string(),
                "xmgUltSnopLSJOukgCHFtg==".to_string(),
                "AxTiFXHwrXKaZC5b7ZRybw==".to_string()
            ]
        );
        assert_eq!(
            out.blobs
                .blob
                .iter()
                .map(|v| v.properties.last_modified.clone())
                .collect::<Vec<String>>(),
            [
                "Sun, 20 Mar 2022 11:29:03 GMT".to_string(),
                "Tue, 29 Mar 2022 01:54:07 GMT".to_string(),
                "Sun, 20 Mar 2022 11:31:57 GMT".to_string()
            ]
        );
        assert_eq!(
            out.blobs
                .blob
                .iter()
                .map(|v| v.properties.etag.clone())
                .collect::<Vec<String>>(),
            [
                "0x8DA0A64D66790C3".to_string(),
                "0x8DA112702D88FE4".to_string(),
                "0x8DA0A653DC82981".to_string()
            ]
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

    /// This case is copied from real environment for testing
    /// quick-xml overlapped-lists features. By default, quick-xml
    /// can't deserialize content with overlapped-lists.
    ///
    /// For example, this case list blobs in this way:
    ///
    /// ```xml
    /// <Blobs>
    ///     <Blob>xxx</Blob>
    ///     <BlobPrefix>yyy</BlobPrefix>
    ///     <Blob>zzz</Blob>
    /// </Blobs>
    /// ```
    ///
    /// If `overlapped-lists` feature not enabled, we will get error `duplicate field Blob`.
    #[test]
    fn test_parse_overlapped_lists() {
        let bs = "<?xml version=\"1.0\" encoding=\"utf-8\"?><EnumerationResults ServiceEndpoint=\"https://test.blob.core.windows.net/\" ContainerName=\"test\"><Prefix>9f7075e1-84d0-45ca-8196-ab9b71a8ef97/x/</Prefix><Delimiter>/</Delimiter><Blobs><Blob><Name>9f7075e1-84d0-45ca-8196-ab9b71a8ef97/x/</Name><Properties><Creation-Time>Thu, 01 Sep 2022 07:26:49 GMT</Creation-Time><Last-Modified>Thu, 01 Sep 2022 07:26:49 GMT</Last-Modified><Etag>0x8DA8BEB55D0EA35</Etag><Content-Length>0</Content-Length><Content-Type>application/octet-stream</Content-Type><Content-Encoding /><Content-Language /><Content-CRC64 /><Content-MD5>1B2M2Y8AsgTpgAmY7PhCfg==</Content-MD5><Cache-Control /><Content-Disposition /><BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred><LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState><ServerEncrypted>true</ServerEncrypted></Properties><OrMetadata /></Blob><BlobPrefix><Name>9f7075e1-84d0-45ca-8196-ab9b71a8ef97/x/x/</Name></BlobPrefix><Blob><Name>9f7075e1-84d0-45ca-8196-ab9b71a8ef97/x/y</Name><Properties><Creation-Time>Thu, 01 Sep 2022 07:26:50 GMT</Creation-Time><Last-Modified>Thu, 01 Sep 2022 07:26:50 GMT</Last-Modified><Etag>0x8DA8BEB55D99C08</Etag><Content-Length>0</Content-Length><Content-Type>application/octet-stream</Content-Type><Content-Encoding /><Content-Language /><Content-CRC64 /><Content-MD5>1B2M2Y8AsgTpgAmY7PhCfg==</Content-MD5><Cache-Control /><Content-Disposition /><BlobType>BlockBlob</BlobType><AccessTier>Hot</AccessTier><AccessTierInferred>true</AccessTierInferred><LeaseStatus>unlocked</LeaseStatus><LeaseState>available</LeaseState><ServerEncrypted>true</ServerEncrypted></Properties><OrMetadata /></Blob></Blobs><NextMarker /></EnumerationResults>";

        de::from_reader(Bytes::from(bs).reader()).expect("must success")
    }
}
