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

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::ready;
use futures::Future;
use http::StatusCode;
use serde::Deserialize;

use super::error::parse_error;
use super::Backend;
use crate::error::new_other_object_error;

use crate::http_util::parse_error_response;
use crate::ops::Operation;
use crate::path::build_rel_path;
use crate::DirEntry;
use crate::ObjectMode;

pub struct DirStream {
    backend: Arc<Backend>,
    state: State,
    root: String,
    path: String,
}

enum State {
    Idle,
    Sending(BoxFuture<'static, io::Result<Bytes>>),
    Listing((Vec<IpfsLsResponseEntry>, usize)),
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, root: &str, path: &str) -> Self {
        Self {
            backend,
            state: State::Idle,
            root: root.to_string(),
            path: path.to_string(),
        }
    }
}

impl futures::Stream for DirStream {
    type Item = io::Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();
        let root = self.root.clone();
        let path = self.path.clone();

        match &mut self.state {
            State::Idle => {
                let fut = async move {
                    let resp = backend.ipmfs_ls(&path).await?;

                    if resp.status() != StatusCode::OK {
                        let er = parse_error_response(resp).await?;
                        let err = parse_error(Operation::List, &path, er);
                        return Err(err);
                    }

                    let bs = resp.into_body().bytes().await.map_err(|e| {
                        new_other_object_error(
                            Operation::List,
                            &path,
                            anyhow!("read body: {:?}", e),
                        )
                    })?;

                    Ok(bs)
                };
                self.state = State::Sending(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Sending(fut) => {
                let contents = ready!(Pin::new(fut).poll(cx))?;

                let entries_body: IpfsLsResponse =
                    serde_json::from_slice(&contents).map_err(|err| {
                        new_other_object_error(
                            Operation::List,
                            &path,
                            anyhow!(
                                "deserialize {} list response: {err:?}",
                                String::from_utf8_lossy(&contents)
                            ),
                        )
                    })?;

                self.state = State::Listing((entries_body.entries.unwrap_or_default(), 0));
                self.poll_next(cx)
            }
            State::Listing((entries, idx)) => {
                if *idx < entries.len() {
                    let object = &entries[*idx];
                    *idx += 1;

                    let path = match object.mode() {
                        ObjectMode::FILE => {
                            format!("{}{}", &path, object.name)
                        }
                        ObjectMode::DIR => {
                            format!("{}{}/", &path, object.name)
                        }
                        ObjectMode::Unknown => unreachable!(),
                    };
                    let path = build_rel_path(&root, &path);
                    let de = DirEntry::new(backend, object.mode(), &path);
                    return Poll::Ready(Some(Ok(de)));
                }

                Poll::Ready(None)
            }
        }
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponseEntry {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Type")]
    file_type: i64,
    #[serde(rename = "Size")]
    size: u64,
}

impl IpfsLsResponseEntry {
    /// ref: <https://github.com/ipfs/specs/blob/main/UNIXFS.md#data-format>
    ///
    /// ```protobuf
    /// enum DataType {
    ///     Raw = 0;
    ///     Directory = 1;
    ///     File = 2;
    ///     Metadata = 3;
    ///     Symlink = 4;
    ///     HAMTShard = 5;
    /// }
    /// ```
    fn mode(&self) -> ObjectMode {
        match &self.file_type {
            1 => ObjectMode::DIR,
            0 | 2 => ObjectMode::FILE,
            _ => ObjectMode::Unknown,
        }
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponse {
    #[serde(rename = "Entries")]
    entries: Option<Vec<IpfsLsResponseEntry>>,
}
