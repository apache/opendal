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

use futures::future::{BoxFuture, Future};
use futures::ready;
use futures::Stream;
use redis::aio::Connection;
use redis::AsyncCommands;
use redis::AsyncIter;

use crate::ops::Operation;
use crate::services::redis::backend::Backend;
use crate::services::redis::error::new_deserialize_metadata_error;
use crate::services::redis::error::new_exec_async_cmd_error;
use crate::services::redis::REDIS_API_VERSION;
use crate::DirEntry;
use crate::ObjectMetadata;

enum State<'a> {
    Idle,
    Awaiting(BoxFuture<'a, Option<String>>),
    Listing(Option<String>),
}

pub struct DirStream<'a: 'b, 'b> {
    backend: Arc<Backend>,
    connection: Arc<Connection>,
    iter: Arc<AsyncIter<'a, String>>,
    state: State<'b>,
    path: String,
}

impl<'a, 'b> DirStream<'a, 'b> {
    pub fn new(
        backend: Arc<Backend>,
        connection: Connection,
        path: &str,
        iter: AsyncIter<'a, String>,
    ) -> Self {
        Self {
            backend,
            connection: Arc::new(connection),
            path: path.to_string(),
            state: State::Idle,
            iter: Arc::new(iter),
        }
    }
}

impl<'a, 'b> Stream for DirStream<'a, 'b> {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let path = self.path.clone();
        let mut con = self.connection.clone();
        let backend = self.backend.clone();
        match &mut self.state {
            State::Idle => {
                let fut = self.iter.next_item();
                self.state = State::Awaiting(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Awaiting(fut) => {
                let item = ready!(Pin::new(fut).poll(cx));
                self.state = State::Listing(item);
                self.poll_next(cx)
            }
            State::Listing(item) => match item {
                Some(entry) => {
                    // get metadata of current entry
                    let m_path = format!("v{}:m:{}", REDIS_API_VERSION, entry);
                    let mut get_bin = con.get(m_path);
                    let bin: Vec<u8> = ready!(get_bin.as_mut().poll(cx)).map_err(|err| {
                        new_exec_async_cmd_error(err, Operation::List, path.as_str())
                    })?;
                    let metadata: ObjectMetadata =
                        bincode::deserialize(&bin[..]).map_err(|err| {
                            new_deserialize_metadata_error(err, Operation::List, path.as_str())
                        })?;
                    // record metadata to DirEntry
                    let mut entry = DirEntry::new(backend, metadata.mode(), entry.as_str());
                    let content_length = metadata.content_length();
                    // folders have no last_modified field
                    if let Some(last_modified) = metadata.last_modified() {
                        entry.set_last_modified(last_modified);
                    }
                    entry.set_content_length(content_length);
                    self.state = State::Idle;
                    Poll::Ready(Some(Ok(entry)))
                }
                None => Poll::Ready(None),
            },
        }
    }
}
