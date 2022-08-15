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
use std::task::{Context, Poll};

use anyhow::anyhow;
use bytes::Buf;
use futures::{future::BoxFuture, ready, Future, Stream};
use isahc::AsyncReadResponseExt;
use serde::Deserialize;
use serde_json::de;

use crate::error::{other, ObjectError};
use crate::http_util::parse_error_response;
use crate::services::gcs::backend::Backend;
use crate::services::gcs::error::parse_error;
use crate::{DirEntry, ObjectMode};

enum State {
    Standby,
    Pending(BoxFuture<'static, Result<Vec<u8>>>),
    Walking((Output, usize, usize)),
}

/// DirStream takes over task of listing objects and
/// helps walking directory
pub struct DirStream {
    backend: Arc<Backend>,
    path: String,
    page_token: String,

    done: bool,
    state: State,
}

impl DirStream {
    /// Generate a new directory walker
    pub fn new(backend: Arc<Backend>, path: &str) -> Self {
        Self {
            backend,
            path: path.to_string(),
            page_token: "".to_string(), // don't know if it works?

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
                        log::error!("GCS failed to list objects, status code: {}", resp.status());
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
                let output: Output = de::from_reader(bytes.reader()).map_err(|e| {
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

                    log::debug!(
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

                    log::debug!(
                        "dir object {} got entry, mode: {}, path: {}",
                        &self.path,
                        de.mode(),
                        de.path()
                    );
                    return Poll::Ready(Some(Ok(de)));
                }

                // end of asynchronous iteration
                if self.done {
                    log::debug!("object {} list done", &self.path);
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
struct Output {
    /// The kind of item this is.
    /// For lists of objects, this is always "storage#objects"
    kind: String,
    /// The continuation token.
    ///
    /// If this is the last page of results, then no continuation token is returned.
    next_page_token: Option<String>,
    /// Object name prefixes for objects that matched the listing request
    /// but were excluded from [items] because of a delimiter.
    prefixes: Vec<String>,
    /// The list of objects, ordered lexicographically by name.
    items: Vec<OutputContent>,
}

#[derive(Default, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OutputContent {
    name: String,
    size: String,
}

#[cfg(test)]
mod ds_test {
    use crate::services::gcs::dir_stream::Output;

    #[test]
    fn test_de_list() {
        let names = vec![
            "/home/datafuselabs/",
            "/home/datafuselabs/databend.txt",
            "/home/datafuselabs/cache-server.md",
        ];
        let sizes = vec![10000, 5000, 5000];
        let objects = names
            .iter()
            .zip(sizes.iter())
            .map(|(&name, size)| mock_object(name.to_string(), *size))
            .collect();
        let list = mock_list(objects);

        let output_list = serde_json::de::from_str::<Output>(list.as_str());
        assert!(output_list.is_ok());
        let output_list = output_list.unwrap();
        assert_eq!(output_list.items.len(), 3);
        for (idx, item) in output_list.items.iter().enumerate() {
            assert_eq!(item.name, names[idx]);
            let size_res = item.size.as_str().parse();
            assert!(size_res.is_ok());
            let size: u64 = size_res.unwrap();
            assert_eq!(size, sizes[idx]);
        }
    }

    fn mock_object(name: String, size: u64) -> String {
        let template: &str = r#"
        {
            "kind": "storage#object",
            "id": "example value",
            "selfLink": "example value",
            "mediaLink": "example value",
            "name": "{name}",
            "bucket": "example value",
            "generation": "0",
            "metageneration": "0",
            "contentType": "example value",
            "storageClass": "example value",
            "size": "{size}",
            "md5Hash": "example value",
            "contentEncoding": "example value",
            "contentDisposition": "example value",
            "contentLanguage": "example value",
            "cacheControl": "example value",
            "crc32c": "example value",
            "componentCount": 0,
            "etag": "example value",
            "kmsKeyName": "example value",
            "temporaryHold": false,
            "eventBasedHold": false,
            "retentionExpirationTime": "2019-08-10T11:45:14+09:00",
            "timeCreated": "2019-08-10T11:45:14+09:00",
            "updated": "2019-08-10T11:45:14+09:00",
            "timeDeleted": "2019-08-10T11:45:14+09:00",
            "timeStorageClassUpdated": "2019-08-10T11:45:14+09:00",
            "customTime": "2019-08-10T11:45:14+09:00",
            "metadata": {
              "example_key": "example value"
            },
            "acl": [
              "<acl data>"
            ],
            "owner": {
              "entity": "example value",
              "entityId": "example value"
            },
            "customerEncryption": {
              "encryptionAlgorithm": "example value",
              "keySha256": "example value"
            }
        }
        "#;
        template
            .replace("{name}", name.as_str())
            .replace("{size}", size.to_string().as_str())
    }
    fn mock_list(items: Vec<String>) -> String {
        let template = r#"
        {
          "kind": "storage#objects",
          "nextPageToken": "4238898",
          "prefixes": [
            "4238898"
          ],
          "items": {items}
        }
        "#;
        let mut list = String::from("[");
        let nums = items.len();
        for (idx, item) in items.iter().enumerate() {
            if idx != 0 {
                list += ",";
            }
            list += item.as_str();
            if idx + 1 == nums {
                list += "]";
            }
        }
        template.replace("{items}", list.as_str())
    }
}
