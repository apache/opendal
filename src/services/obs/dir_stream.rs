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

use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use bytes::Buf;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::ready;
use futures::Future;
use quick_xml::de;
use serde::Deserialize;

use crate::error::new_other_object_error;
use crate::http_util::parse_error_response;
use crate::ops::Operation;
use crate::path::build_rel_path;
use crate::services::obs::error::parse_error;
use crate::services::obs::Backend;
use crate::ObjectEntry;
use crate::ObjectMetadata;
use crate::ObjectMode;

pub struct DirStream {
    backend: Arc<Backend>,
    root: String,
    path: String,

    next_marker: String,
    done: bool,
    fut: Option<BoxFuture<'static, Result<Bytes>>>,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, root: &str, path: &str) -> Self {
        Self {
            backend,
            root: root.to_string(),
            path: path.to_string(),
            next_marker: "".to_string(),
            done: false,
            fut: None,
        }
    }
}

impl futures::Stream for DirStream {
    type Item = Result<Vec<ObjectEntry>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();
        let root = self.root.clone();

        match &mut self.fut {
            None => {
                let path = self.path.clone();
                let next_marker = self.next_marker.clone();
                let fut = async move {
                    let resp = backend.obs_list_objects(&path, &next_marker).await?;

                    if resp.status() != http::StatusCode::OK {
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
                self.fut = Some(Box::pin(fut));
                self.poll_next(cx)
            }
            Some(fut) => {
                let bs = ready!(Pin::new(fut).poll(cx))?;
                let output: Output = de::from_reader(bs.reader())
                    .map_err(|e| new_other_object_error(Operation::List, &self.path, e))?;

                // Try our best to check whether this list is done.
                //
                // - Check `next_marker`
                self.done = match output.next_marker.as_ref() {
                    None => true,
                    Some(next_marker) => next_marker.is_empty(),
                };
                self.next_marker = output.next_marker.clone().unwrap_or_default();

                let common_prefixes = output.common_prefixes.unwrap_or_default();
                let mut entries = Vec::with_capacity(common_prefixes.len() + output.contents.len());

                for prefix in common_prefixes {
                    let de = ObjectEntry::new(
                        backend.clone(),
                        &build_rel_path(&root, &prefix.prefix),
                        ObjectMetadata::new(ObjectMode::DIR),
                    )
                    .with_complete();

                    entries.push(de);
                }

                for object in output.contents {
                    if object.key.ends_with('/') {
                        continue;
                    }

                    let meta =
                        ObjectMetadata::new(ObjectMode::FILE).with_content_length(object.size);

                    let de = ObjectEntry::new(
                        backend.clone(),
                        &build_rel_path(&root, &object.key),
                        meta,
                    );

                    entries.push(de);
                }

                Poll::Ready(Some(Ok(entries)))
            }
        }
    }
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Output {
    name: String,
    prefix: String,
    contents: Vec<Content>,
    common_prefixes: Option<Vec<CommonPrefix>>,
    marker: String,
    next_marker: Option<String>,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct CommonPrefix {
    prefix: String,
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Content {
    key: String,
    size: u64,
}

#[cfg(test)]
mod tests {
    use bytes::Buf;

    use super::*;

    #[test]
    fn test_parse_xml() {
        let bs = bytes::Bytes::from(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ListBucketResult xmlns="http://obs.cn-north-4.myhuaweicloud.com/doc/2015-06-30/">
    <Name>examplebucket</Name>
    <Prefix>obj</Prefix>
    <Marker>obj002</Marker>
    <NextMarker>obj004</NextMarker>
    <MaxKeys>1000</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Contents>
        <Key>obj002</Key>
        <LastModified>2015-07-01T02:11:19.775Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d"</ETag>
        <Size>9</Size>
        <Owner>
            <ID>b4bf1b36d9ca43d984fbcb9491b6fce9</ID>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <Contents>
        <Key>obj003</Key>
        <LastModified>2015-07-01T02:11:19.775Z</LastModified>
        <ETag>"a72e382246ac83e86bd203389849e71d"</ETag>
        <Size>10</Size>
        <Owner>
            <ID>b4bf1b36d9ca43d984fbcb9491b6fce9</ID>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
    </Contents>
    <CommonPrefixes>
        <Prefix>hello</Prefix>
    </CommonPrefixes>
    <CommonPrefixes>
        <Prefix>world</Prefix>
    </CommonPrefixes>
</ListBucketResult>"#,
        );
        let out: Output = de::from_reader(bs.reader()).expect("must success");

        assert_eq!(out.name, "examplebucket".to_string());
        assert_eq!(out.prefix, "obj".to_string());
        assert_eq!(out.marker, "obj002".to_string());
        assert_eq!(out.next_marker, Some("obj004".to_string()),);
        assert_eq!(
            out.contents
                .iter()
                .map(|v| v.key.clone())
                .collect::<Vec<String>>(),
            ["obj002", "obj003"],
        );
        assert_eq!(
            out.contents.iter().map(|v| v.size).collect::<Vec<u64>>(),
            [9, 10],
        );
        assert_eq!(
            out.common_prefixes
                .unwrap()
                .iter()
                .map(|v| v.prefix.clone())
                .collect::<Vec<String>>(),
            ["hello", "world"],
        )
    }
}
