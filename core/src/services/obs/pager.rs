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

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Buf;
use quick_xml::de;
use serde::Deserialize;

use super::backend::ObsBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::EntryMode;
use crate::Error;
use crate::ErrorKind;
use crate::Metadata;
use crate::Result;

pub struct ObsPager {
    backend: Arc<ObsBackend>,
    root: String,
    path: String,
    delimiter: String,
    limit: Option<usize>,

    next_marker: String,
    done: bool,
}

impl ObsPager {
    pub fn new(
        backend: Arc<ObsBackend>,
        root: &str,
        path: &str,
        delimiter: &str,
        limit: Option<usize>,
    ) -> Self {
        Self {
            backend,
            root: root.to_string(),
            path: path.to_string(),
            delimiter: delimiter.to_string(),
            limit,

            next_marker: "".to_string(),
            done: false,
        }
    }
}

#[async_trait]
impl oio::Page for ObsPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.done {
            return Ok(None);
        }

        let resp = self
            .backend
            .obs_list_objects(&self.path, &self.next_marker, &self.delimiter, self.limit)
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp).await?);
        }

        let bs = resp.into_body().bytes().await?;

        let output: Output = de::from_reader(bs.reader())
            .map_err(|e| Error::new(ErrorKind::Unexpected, "deserialize xml").set_source(e))?;

        // Try our best to check whether this list is done.
        //
        // - Check `next_marker`
        self.done = match output.next_marker.as_ref() {
            None => true,
            Some(next_marker) => next_marker.is_empty(),
        };
        self.next_marker = output.next_marker.clone().unwrap_or_default();

        let common_prefixes = output.common_prefixes;
        let mut entries = Vec::with_capacity(common_prefixes.len() + output.contents.len());

        for prefix in common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );

            entries.push(de);
        }

        for object in output.contents {
            if object.key.ends_with('/') {
                continue;
            }

            let meta = Metadata::new(EntryMode::FILE).with_content_length(object.size);

            let de = oio::Entry::new(&build_rel_path(&self.root, &object.key), meta);

            entries.push(de);
        }

        Ok(Some(entries))
    }
}

#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Output {
    name: String,
    prefix: String,
    contents: Vec<Content>,
    common_prefixes: Vec<CommonPrefix>,
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
                .iter()
                .map(|v| v.prefix.clone())
                .collect::<Vec<String>>(),
            ["hello", "world"],
        )
    }
}
