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
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use super::backend::S3Backend;
use super::error::parse_error;
use crate::raw::*;
use crate::EntryMode;
use crate::Error;
use crate::ErrorKind;
use crate::Metadata;
use crate::Result;

pub struct S3Pager {
    backend: Arc<S3Backend>,
    root: String,
    path: String,
    delimiter: String,
    limit: Option<usize>,

    token: String,
    done: bool,
}

impl S3Pager {
    pub fn new(
        backend: Arc<S3Backend>,
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

            token: "".to_string(),
            done: false,
        }
    }
}

#[async_trait]
impl oio::Page for S3Pager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.done {
            return Ok(None);
        }

        let resp = self
            .backend
            .s3_list_objects(&self.path, &self.token, &self.delimiter, self.limit)
            .await?;

        if resp.status() != http::StatusCode::OK {
            return Err(parse_error(resp).await?);
        }

        let bs = resp.into_body().bytes().await?;

        let output: Output = de::from_reader(bs.reader()).map_err(new_xml_deserialize_error)?;

        // Try our best to check whether this list is done.
        //
        // - Check `is_truncated`
        // - Check `next_continuation_token`
        // - Check the length of `common_prefixes` and `contents` (very rarely case)
        self.done = if let Some(is_truncated) = output.is_truncated {
            !is_truncated
        } else if let Some(next_continuation_token) = output.next_continuation_token.as_ref() {
            next_continuation_token.is_empty()
        } else {
            output.common_prefixes.is_empty() && output.contents.is_empty()
        };
        self.token = output.next_continuation_token.clone().unwrap_or_default();

        let mut entries = Vec::with_capacity(output.common_prefixes.len() + output.contents.len());

        for prefix in output.common_prefixes {
            let de = oio::Entry::new(
                &build_rel_path(&self.root, &prefix.prefix),
                Metadata::new(EntryMode::DIR),
            );

            entries.push(de);
        }

        for object in output.contents {
            // s3 could return the dir itself in contents
            // which endswith `/`.
            // We should ignore them.
            if object.key.ends_with('/') {
                continue;
            }

            let mut meta = Metadata::new(EntryMode::FILE);

            meta.set_etag(&object.etag);
            meta.set_content_md5(object.etag.trim_matches('"'));
            meta.set_content_length(object.size);

            // object.last_modified provides more precious time that contains
            // nanosecond, let's trim them.
            let dt = OffsetDateTime::parse(object.last_modified.as_str(), &Rfc3339)
                .map(|v| {
                    v.replace_nanosecond(0)
                        .expect("replace nanosecond of last modified must succeed")
                })
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "parse last modified RFC3339 datetime",
                    )
                    .set_source(e)
                })?;
            meta.set_last_modified(dt);

            let de = oio::Entry::new(&build_rel_path(&self.root, &object.key), meta);

            entries.push(de);
        }

        Ok(Some(entries))
    }
}

/// Output of ListBucket/ListObjects.
///
/// ## Note
///
/// Use `Option` in `is_truncated` and `next_continuation_token` to make
/// the behavior more clear so that we can be compatible to more s3 services.
///
/// And enable `serde(default)` so that we can keep going even when some field
/// is not exist.
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "PascalCase")]
struct Output {
    is_truncated: Option<bool>,
    next_continuation_token: Option<String>,
    common_prefixes: Vec<OutputCommonPrefix>,
    contents: Vec<OutputContent>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct OutputContent {
    key: String,
    size: u64,
    last_modified: String,
    #[serde(rename = "ETag")]
    etag: String,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct OutputCommonPrefix {
    prefix: String,
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

        let out: Output = de::from_reader(bs.reader()).expect("must success");

        assert!(!out.is_truncated.unwrap());
        assert!(out.next_continuation_token.is_none());
        assert_eq!(
            out.common_prefixes
                .iter()
                .map(|v| v.prefix.clone())
                .collect::<Vec<String>>(),
            vec!["photos/2006/February/", "photos/2006/January/"]
        );
        assert_eq!(
            out.contents,
            vec![
                OutputContent {
                    key: "photos/2006".to_string(),
                    size: 56,
                    etag: "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string(),
                    last_modified: "2016-04-30T23:51:29.000Z".to_string(),
                },
                OutputContent {
                    key: "photos/2007".to_string(),
                    size: 100,
                    last_modified: "2016-04-30T23:51:29.000Z".to_string(),
                    etag: "\"d41d8cd98f00b204e9800998ecf8427e\"".to_string(),
                }
            ]
        )
    }
}
