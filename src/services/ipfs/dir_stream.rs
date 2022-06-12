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

use futures::future::BoxFuture;
use futures::ready;
use futures::Future;
use serde::Deserialize;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use anyhow::anyhow;
use isahc::AsyncReadResponseExt;

use super::Backend;

use crate::error::other;
use crate::error::ObjectError;
use crate::ops::Operation;
use crate::DirEntry;
use crate::ObjectMode;

pub struct DirStream {
    backend: Arc<Backend>,
    state: State,
    path: String,
}

enum State {
    Idle,
    Sending(BoxFuture<'static, io::Result<Vec<u8>>>),
    Listing((ListEntriesBody, usize)),
}

#[derive(Deserialize, Debug)]
struct ListEntry {
    #[serde(rename(deserialize = "Name"))]
    name: String,
    #[serde(rename(deserialize = "Type"))]
    file_type: i64,
}

impl ListEntry {
    pub fn file_type(&self) -> ObjectMode {
        // https://github.com/ipfs/specs/blob/main/UNIXFS.md#data-format
        match &self.file_type {
            1 => ObjectMode::DIR,
            2 => ObjectMode::FILE,
            _ => ObjectMode::Unknown,
        }
    }
}

#[derive(Deserialize, Debug)]
struct ListEntriesBody {
    #[serde(rename(deserialize = "Entries"))]
    entries: Vec<ListEntry>,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, path: &str) -> Self {
        Self {
            backend,
            state: State::Idle,
            path: path.to_string(),
        }
    }
}

impl futures::Stream for DirStream {
    type Item = io::Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();

        match &mut self.state {
            State::Idle => {
                let path = self.path.clone();

                let fut = async move {
                    let mut resp = backend.files_list(&path).await?;

                    let bs = resp.bytes().await.map_err(|e| {
                        other(ObjectError::new(
                            Operation::List,
                            &path,
                            anyhow!("read body: {:?}", e),
                        ))
                    })?;

                    Ok(bs)
                };
                self.state = State::Sending(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Sending(fut) => {
                let contents = ready!(Pin::new(fut).poll(cx))?;

                let entries_body: ListEntriesBody = serde_json::from_slice(&contents)?;

                self.state = State::Listing((entries_body, 0));
                self.poll_next(cx)
            }
            State::Listing((contents, idx)) => {
                if *idx < contents.entries.len() {
                    let object = &contents.entries[*idx];
                    *idx += 1;

                    let de = DirEntry::new(backend, object.file_type(), &object.name);
                    return Poll::Ready(Some(Ok(de)));
                }

                Poll::Ready(None)
            }
        }
    }
}
