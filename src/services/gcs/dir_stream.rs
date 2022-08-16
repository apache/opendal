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
use futures::future::BoxFuture;
use futures::ready;
use futures::Future;
use futures::Stream;
use isahc::AsyncReadResponseExt;
use log::debug;
use log::error;
use serde::Deserialize;
use serde_json;

use crate::error::other;
use crate::error::ObjectError;
use crate::http_util::parse_error_response;
use crate::services::gcs::backend::Backend;
use crate::services::gcs::error::parse_error;
use crate::DirEntry;
use crate::ObjectMode;

/// DirStream takes over task of listing objects and
/// helps walking directory
pub struct DirStream {
    backend: Arc<Backend>,
    path: String,
    page_token: String,

    done: bool,
    state: State,
}

enum State {
    Standby,
    Pending(BoxFuture<'static, Result<Vec<u8>>>),
    Walking((ListResponse, usize, usize)),
}

impl DirStream {
    /// Generate a new directory walker
    pub fn new(backend: Arc<Backend>, path: &str) -> Self {
        Self {
            backend,
            path: path.to_string(),
            page_token: "".to_string(),

            done: false,
            state: State::Standby,
        }
    }
}

impl Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();

        match &mut self.state {
            State::Standby => {
                let path = self.path.clone();
                let token = self.page_token.clone();

                let fut = async move {
                    let mut resp = backend.list_objects(&path, token.as_str()).await?;

                    if !resp.status().is_success() {
                        error!("GCS failed to list objects, status code: {}", resp.status());
                        let er = parse_error_response(resp).await?;
                        let err = parse_error("list", &path, er);
                        return Err(err);
                    }
                    let bytes = resp.bytes().await.map_err(|e| {
                        other(ObjectError::new(
                            "list",
                            &path,
                            anyhow!("read body: {:?}", e),
                        ))
                    })?;

                    Ok(bytes)
                };

                self.state = State::Pending(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Pending(fut) => {
                let bytes = ready!(Pin::new(fut).poll(cx))?;
                let output: ListResponse = serde_json::from_slice(&bytes).map_err(|e| {
                    other(ObjectError::new(
                        "list",
                        &self.path,
                        anyhow!("deserialize list_bucket output: {:?}", e),
                    ))
                })?;

                if let Some(token) = &output.next_page_token {
                    self.page_token = token.clone();
                } else {
                    self.done = true;
                }
                self.state = State::Walking((output, 0, 0));
                self.poll_next(cx)
            }
            State::Walking((output, common_prefixes_idx, objects_idx)) => {
                let prefixes = &output.prefixes;
                if *common_prefixes_idx < prefixes.len() {
                    let prefix = &prefixes[*common_prefixes_idx];
                    *common_prefixes_idx += 1;

                    let de = DirEntry::new(
                        backend.clone(),
                        ObjectMode::DIR,
                        &backend.get_rel_path(prefix),
                    );

                    debug!(
                        "object {} got entry, mode: {}, path: {}",
                        &self.path,
                        de.path(),
                        de.mode()
                    );
                    return Poll::Ready(Some(Ok(de)));
                }
                let objects = &output.items;
                while *objects_idx < objects.len() {
                    let object = &objects[*objects_idx];
                    *objects_idx += 1;

                    if object.name.ends_with('/') {
                        continue;
                    }

                    let de = DirEntry::new(
                        backend.clone(),
                        ObjectMode::FILE,
                        &backend.get_rel_path(&object.name),
                    );

                    debug!(
                        "dir object {} got entry, mode: {}, path: {}",
                        &self.path,
                        de.mode(),
                        de.path()
                    );
                    return Poll::Ready(Some(Ok(de)));
                }

                // end of asynchronous iteration
                if self.done {
                    debug!("object {} list done", &self.path);
                    return Poll::Ready(None);
                }

                self.state = State::Standby;
                self.poll_next(cx)
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
        assert_eq!(output.items[1].name, "2.png");
        assert_eq!(output.items[1].name, "45506");
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
        assert_eq!(output.items[1].name, "2.png");
        assert_eq!(output.items[1].name, "45506");
        assert_eq!(output.prefixes, vec!["dir/", "test/"])
    }
}
