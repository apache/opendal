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

use anyhow::anyhow;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::ready;
use futures::Future;
use futures::Stream;
use serde::Deserialize;
use serde_json;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::error::new_other_object_error;
use crate::http_util::parse_error_response;
use crate::ops::Operation;
use crate::path::build_rel_path;
use crate::services::gcs::backend::Backend;
use crate::services::gcs::error::parse_error;
use crate::ObjectEntry;
use crate::ObjectMetadata;
use crate::ObjectMode;

/// DirStream takes over task of listing objects and
/// helps walking directory
pub struct DirStream {
    backend: Arc<Backend>,
    root: String,
    path: String,
    page_token: String,

    done: bool,
    fut: Option<BoxFuture<'static, Result<Bytes>>>,
}

impl DirStream {
    /// Generate a new directory walker
    pub fn new(backend: Arc<Backend>, root: &str, path: &str) -> Self {
        Self {
            backend,
            root: root.to_string(),
            path: path.to_string(),
            page_token: "".to_string(),

            done: false,
            fut: None,
        }
    }
}

impl Stream for DirStream {
    type Item = Result<Vec<ObjectEntry>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();
        let root = self.root.clone();
        let path = self.path.clone();

        match &mut self.fut {
            None => {
                let token = self.page_token.clone();

                let fut = async move {
                    let resp = backend.gcs_list_objects(&path, token.as_str()).await?;

                    if !resp.status().is_success() {
                        let er = parse_error_response(resp).await?;
                        let err = parse_error(Operation::List, &path, er);
                        return Err(err);
                    }
                    let bytes = resp.into_body().bytes().await.map_err(|e| {
                        new_other_object_error(
                            Operation::List,
                            &path,
                            anyhow!("read body: {:?}", e),
                        )
                    })?;

                    Ok(bytes)
                };

                self.fut = Some(Box::pin(fut));
                self.poll_next(cx)
            }
            Some(fut) => {
                let bytes = ready!(Pin::new(fut).poll(cx))?;
                let output: ListResponse = serde_json::from_slice(&bytes).map_err(|e| {
                    new_other_object_error(
                        Operation::List,
                        &self.path,
                        anyhow!("deserialize list_bucket output: {:?}", e),
                    )
                })?;

                if let Some(token) = &output.next_page_token {
                    self.page_token = token.clone();
                } else {
                    self.done = true;
                }

                let mut entries = Vec::with_capacity(output.prefixes.len() + output.items.len());

                for prefix in output.prefixes {
                    let de = ObjectEntry::new(
                        backend.clone(),
                        &build_rel_path(&root, &prefix),
                        ObjectMetadata::new(ObjectMode::DIR),
                    )
                    .with_complete();

                    entries.push(de);
                }

                for object in output.items {
                    if object.name.ends_with('/') {
                        continue;
                    }

                    let mut meta = ObjectMetadata::new(ObjectMode::FILE);

                    // set metadata fields
                    meta.set_content_md5(object.md5_hash.as_str());
                    meta.set_etag(object.etag.as_str());

                    let size = object.size.parse().map_err(|e| {
                        new_other_object_error(
                            Operation::List,
                            path.as_str(),
                            anyhow!("parse object size: {e:?}"),
                        )
                    })?;
                    meta.set_content_length(size);

                    let dt =
                        OffsetDateTime::parse(object.updated.as_str(), &Rfc3339).map_err(|e| {
                            new_other_object_error(
                                Operation::List,
                                &path,
                                anyhow!("parse last modified RFC3339 datetime: {e:?}"),
                            )
                        })?;
                    meta.set_last_modified(dt);

                    let de = ObjectEntry::new(
                        backend.clone(),
                        &build_rel_path(&root, &object.name),
                        meta,
                    )
                    .with_complete();

                    entries.push(de);
                }

                Poll::Ready(Some(Ok(entries)))
            }
        }
    }
}

/// Response JSON from GCS list objects API.
///
/// refer to https://cloud.google.com/storage/docs/json_api/v1/objects/list for details
#[derive(Default, Debug, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct ListResponse {
    /// The continuation token.
    ///
    /// If this is the last page of results, then no continuation token is returned.
    next_page_token: Option<String>,
    /// Object name prefixes for objects that matched the listing request
    /// but were excluded from [items] because of a delimiter.
    prefixes: Vec<String>,
    /// The list of objects, ordered lexicographically by name.
    items: Vec<ListResponseItem>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponseItem {
    name: String,
    size: String,
    // metadata
    etag: String,
    md5_hash: String,
    updated: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_list_response() {
        let content = r#"
    {
  "kind": "storage#objects",
  "prefixes": [
    "dir/",
    "test/"
  ],
  "items": [
    {
      "kind": "storage#object",
      "id": "example/1.png/1660563214863653",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/1.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/1.png?generation=1660563214863653&alt=media",
      "name": "1.png",
      "bucket": "example",
      "generation": "1660563214863653",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "56535",
      "md5Hash": "fHcEH1vPwA6eTPqxuasXcg==",
      "crc32c": "j/un9g==",
      "etag": "CKWasoTgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.866Z",
      "updated": "2022-08-15T11:33:34.866Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.866Z"
    },
    {
      "kind": "storage#object",
      "id": "example/2.png/1660563214883337",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/2.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/2.png?generation=1660563214883337&alt=media",
      "name": "2.png",
      "bucket": "example",
      "generation": "1660563214883337",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "45506",
      "md5Hash": "e6LsGusU7pFJZk+114NV1g==",
      "crc32c": "L00QAg==",
      "etag": "CIm0s4TgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.886Z",
      "updated": "2022-08-15T11:33:34.886Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.886Z"
    }
  ]
}
    "#;

        let output: ListResponse =
            serde_json::from_str(content).expect("JSON deserialize must succeed");
        assert!(output.next_page_token.is_none());
        assert_eq!(output.items.len(), 2);
        assert_eq!(output.items[0].name, "1.png");
        assert_eq!(output.items[0].size, "56535");
        assert_eq!(output.items[0].md5_hash, "fHcEH1vPwA6eTPqxuasXcg==");
        assert_eq!(output.items[0].etag, "CKWasoTgyPkCEAE=");
        assert_eq!(output.items[0].updated, "2022-08-15T11:33:34.866Z");
        assert_eq!(output.items[1].name, "2.png");
        assert_eq!(output.items[1].size, "45506");
        assert_eq!(output.items[1].md5_hash, "e6LsGusU7pFJZk+114NV1g==");
        assert_eq!(output.items[1].etag, "CIm0s4TgyPkCEAE=");
        assert_eq!(output.items[1].updated, "2022-08-15T11:33:34.886Z");
        assert_eq!(output.prefixes, vec!["dir/", "test/"])
    }

    #[test]
    fn test_deserialize_list_response_with_next_page_token() {
        let content = r#"
    {
  "kind": "storage#objects",
  "prefixes": [
    "dir/",
    "test/"
  ],
  "nextPageToken": "CgYxMC5wbmc=",
  "items": [
    {
      "kind": "storage#object",
      "id": "example/1.png/1660563214863653",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/1.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/1.png?generation=1660563214863653&alt=media",
      "name": "1.png",
      "bucket": "example",
      "generation": "1660563214863653",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "56535",
      "md5Hash": "fHcEH1vPwA6eTPqxuasXcg==",
      "crc32c": "j/un9g==",
      "etag": "CKWasoTgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.866Z",
      "updated": "2022-08-15T11:33:34.866Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.866Z"
    },
    {
      "kind": "storage#object",
      "id": "example/2.png/1660563214883337",
      "selfLink": "https://www.googleapis.com/storage/v1/b/example/o/2.png",
      "mediaLink": "https://content-storage.googleapis.com/download/storage/v1/b/example/o/2.png?generation=1660563214883337&alt=media",
      "name": "2.png",
      "bucket": "example",
      "generation": "1660563214883337",
      "metageneration": "1",
      "contentType": "image/png",
      "storageClass": "STANDARD",
      "size": "45506",
      "md5Hash": "e6LsGusU7pFJZk+114NV1g==",
      "crc32c": "L00QAg==",
      "etag": "CIm0s4TgyPkCEAE=",
      "timeCreated": "2022-08-15T11:33:34.886Z",
      "updated": "2022-08-15T11:33:34.886Z",
      "timeStorageClassUpdated": "2022-08-15T11:33:34.886Z"
    }
  ]
}
    "#;

        let output: ListResponse =
            serde_json::from_str(content).expect("JSON deserialize must succeed");
        assert_eq!(output.next_page_token, Some("CgYxMC5wbmc=".to_string()));
        assert_eq!(output.items.len(), 2);
        assert_eq!(output.items[0].name, "1.png");
        assert_eq!(output.items[0].size, "56535");
        assert_eq!(output.items[0].md5_hash, "fHcEH1vPwA6eTPqxuasXcg==");
        assert_eq!(output.items[0].etag, "CKWasoTgyPkCEAE=");
        assert_eq!(output.items[0].updated, "2022-08-15T11:33:34.866Z");
        assert_eq!(output.items[1].name, "2.png");
        assert_eq!(output.items[1].size, "45506");
        assert_eq!(output.items[1].md5_hash, "e6LsGusU7pFJZk+114NV1g==");
        assert_eq!(output.items[1].etag, "CIm0s4TgyPkCEAE=");
        assert_eq!(output.items[1].updated, "2022-08-15T11:33:34.886Z");
        assert_eq!(output.prefixes, vec!["dir/", "test/"])
    }
}
