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

use anyhow::anyhow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bstr::ByteSlice;
use bytes::BufMut;
use futures::future::BoxFuture;
use futures::ready;
use futures::{Stream, StreamExt};
use log::debug;

use super::Backend;
use crate::error::{Error, Kind, Result};
use crate::Object;
use crate::ObjectMode;

pub struct S3ObjectStream {
    backend: Backend,
    path: String,

    token: String,
    done: bool,
    state: State,
}

// #[allow(clippy::large_enum_variant)]
enum State {
    Idle,
    Sending(BoxFuture<'static, Result<bytes::Bytes>>),
    /// # TODO
    ///
    /// It's better to move this large struct to heap as suggested by clippy.
    ///
    ///   --> src/services/s3/object_stream.rs:45:5
    ///    |
    /// 45 |     Listing((ListObjectsV2Output, usize, usize)),
    ///    |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ this variant is 256 bytes
    ///    |
    ///    = note: `-D clippy::large-enum-variant` implied by `-D warnings`
    /// note: and the second-largest variant is 16 bytes:
    ///   --> src/services/s3/object_stream.rs:44:5
    ///    |
    /// 44 |     Sending(BoxFuture<'static, Result<ListObjectsV2Output>>),
    ///    |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///    = help: for further information visit https://rust-lang.github.io/rust-clippy/master/index.html#large_enum_variant
    /// help: consider boxing the large fields to reduce the total size of the enum
    ///    |
    /// 45 |     Listing(Box<(ListObjectsV2Output, usize, usize)>),
    ///
    /// But stable rust doesn't support `State::Listing(box (output, common_prefixes_idx, objects_idx))` so far, let's wait a bit.
    Listing((ListOutput, usize, usize)),
}

impl S3ObjectStream {
    pub fn new(backend: Backend, path: String) -> Self {
        Self {
            backend,
            path,

            token: "".to_string(),
            done: false,
            state: State::Idle,
        }
    }
}

impl futures::Stream for S3ObjectStream {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();

        match &mut self.state {
            State::Idle => {
                let backend = self.backend.clone();
                let path = self.path.clone();
                let token = self.token.clone();
                let fut = async move {
                    let mut resp = backend.list_object(&path, &token).await?;

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
                        let b = b.unwrap();
                        bs.put_slice(&b)
                    }

                    Ok(bs.freeze())
                };
                self.state = State::Sending(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Sending(fut) => {
                let output = ListOutput::parse(ready!(Pin::new(fut).poll(cx))?)?;

                self.done = !output.is_truncated;
                self.token = output.next_continuation_token.clone();
                self.state = State::Listing((output, 0, 0));
                self.poll_next(cx)
            }
            State::Listing((output, common_prefixes_idx, objects_idx)) => {
                let prefixes = &output.common_prefixes;
                if *common_prefixes_idx < prefixes.len() {
                    *common_prefixes_idx += 1;
                    let prefix = &prefixes[*common_prefixes_idx - 1];

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

                let objects = &output.contents;
                if *objects_idx < objects.len() {
                    *objects_idx += 1;
                    let object = &objects[*objects_idx - 1];

                    let mut o = Object::new(
                        Arc::new(backend.clone()),
                        &backend.get_rel_path(&object.key),
                    );
                    let meta = o.metadata_mut();
                    meta.set_mode(ObjectMode::FILE)
                        .set_content_length(object.size as u64);

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

#[derive(Default, Debug)]
struct ListOutput {
    is_truncated: bool,
    next_continuation_token: String,
    common_prefixes: Vec<String>,
    contents: Vec<ListOutputContent>,
}

#[derive(Default, Debug, Eq, PartialEq)]
struct ListOutputContent {
    key: String,
    size: u64,
}

impl ListOutput {
    fn parse(bs: bytes::Bytes) -> Result<ListOutput> {
        let root = roxmltree::Document::parse(
            bs.as_bytes().to_str().expect("content must be valid utf-8"),
        )
        .map_err(|e| Error::Unexpected(anyhow::Error::from(e)))?;

        let mut output = ListOutput::default();

        // IsTruncated
        if let Some(n) = root
            .descendants()
            .find(|n| n.tag_name().name() == "IsTruncated")
        {
            output.is_truncated = n
                .text()
                .unwrap_or("false")
                .parse::<bool>()
                .map_err(|e| invalid_list_object_response(&e.to_string()))?;
        }

        // NextContinuationToken
        if let Some(n) = root
            .descendants()
            .find(|n| n.tag_name().name() == "NextContinuationToken")
        {
            output.next_continuation_token = n.text().unwrap_or_default().to_string();
        }

        // CommonPrefixes
        for item in root
            .descendants()
            .filter(|v| v.tag_name().name() == "CommonPrefixes")
        {
            output.common_prefixes.push(
                item.children()
                    .find(|v| v.tag_name().name() == "Prefix")
                    .ok_or_else(|| invalid_list_object_response("Prefix is not found"))?
                    .text()
                    .ok_or_else(|| invalid_list_object_response("Prefix is empty"))?
                    .to_string(),
            )
        }

        // Contents
        for item in root
            .descendants()
            .filter(|v| v.tag_name().name() == "Contents")
        {
            let mut content = ListOutputContent::default();

            // Key
            let n = item
                .children()
                .find(|n| n.tag_name().name() == "Key")
                .ok_or_else(|| invalid_list_object_response("Key is not found"))?;
            content.key = n
                .text()
                .ok_or_else(|| invalid_list_object_response("Key is empty"))?
                .to_string();

            // Size
            let n = item
                .children()
                .find(|n| n.tag_name().name() == "Size")
                .ok_or_else(|| invalid_list_object_response("Size is not found"))?;
            content.size = n
                .text()
                .ok_or_else(|| invalid_list_object_response("Size is empty"))?
                .parse::<u64>()
                .map_err(|e| invalid_list_object_response(&e.to_string()))?;

            output.contents.push(content)
        }

        Ok(output)
    }
}

fn invalid_list_object_response(cause: &str) -> Error {
    Error::Object {
        kind: Kind::Unexpected,
        op: "list",
        path: "".to_string(),
        source: anyhow!("invalid list object response: {}", cause),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_list_output() {
        let bs = bytes::Bytes::from(
            r#"<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>example-bucket</Name>
  <Prefix>photos/2006/</Prefix>
  <KeyCount>3</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <Delimiter>/</Delimiter>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>photos/2006</Key>
    <LastModified>2016-04-30T23:51:29.000Z</LastModified>
    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
    <Size>56</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>photos/2007</Key>
    <LastModified>2016-04-30T23:51:29.000Z</LastModified>
    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
    <Size>100</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>

  <CommonPrefixes>
    <Prefix>photos/2006/February/</Prefix>
  </CommonPrefixes>
  <CommonPrefixes>
    <Prefix>photos/2006/January/</Prefix>
  </CommonPrefixes>
</ListBucketResult>"#,
        );

        let out = ListOutput::parse(bs).expect("must success");

        assert!(!out.is_truncated);
        assert!(out.next_continuation_token.is_empty());
        assert_eq!(
            out.common_prefixes,
            vec!["photos/2006/February/", "photos/2006/January/"]
        );
        assert_eq!(
            out.contents,
            vec![
                ListOutputContent {
                    key: "photos/2006".to_string(),
                    size: 56
                },
                ListOutputContent {
                    key: "photos/2007".to_string(),
                    size: 100
                }
            ]
        )
    }
}
