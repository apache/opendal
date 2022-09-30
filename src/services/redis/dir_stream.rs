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

use futures::future::BoxFuture;
use futures::future::Future;
use futures::ready;
use futures::stream::Stream;
use redis::aio::ConnectionManager;

use crate::ops::Operation;
use crate::services::redis::backend::Backend;
use crate::services::redis::error::new_exec_async_cmd_error;
use crate::ObjectEntry;
use crate::ObjectMode;

enum State {
    Idle,
    Awaiting(BoxFuture<'static, Result<(u64, Vec<String>)>>),
    Listing(&'static [String]),
}

pub struct DirStream {
    backend: Arc<Backend>,
    cm: ConnectionManager,
    path: String,

    done: bool,
    cursor: u64,
    state: State,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, cm: ConnectionManager, path: &str) -> Self {
        Self {
            backend,
            cm,
            path: path.to_string(),

            done: false,
            cursor: 0,
            state: State::Idle,
        }
    }
}

impl Stream for DirStream {
    type Item = Result<ObjectEntry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let path = self.path.clone();
        let mut con = self.cm.clone();
        let backend = self.backend.clone();
        match &mut self.state {
            State::Idle => {
                if self.done {
                    return Poll::Ready(None);
                }
                let cursor = self.cursor;
                let fut = async move {
                    let (cursor, children): (u64, Vec<String>) = redis::cmd("sscan")
                        .cursor_arg(cursor)
                        .arg(path.clone())
                        .query_async(&mut con)
                        .await
                        .map_err(|err| {
                            new_exec_async_cmd_error(err, Operation::List, path.as_str())
                        })?;
                    Ok((cursor, children))
                };
                self.state = State::Awaiting(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Awaiting(fut) => {
                let (cursor, children) = ready!(Pin::new(fut).poll(cx))?;
                self.cursor = cursor;
                if cursor == 0 {
                    self.done = true;
                }
                self.state = State::Listing(children.leak());
                self.poll_next(cx)
            }
            State::Listing(children) => {
                if let Some(child) = children.last() {
                    self.state = State::Listing(&children[..children.len() - 1]);

                    // only mode will be shown in listing
                    let mode = if child.ends_with('/') {
                        ObjectMode::DIR
                    } else {
                        ObjectMode::FILE
                    };

                    let entry = ObjectEntry::new(backend, mode, path.as_str());
                    Poll::Ready(Some(Ok(entry)))
                } else {
                    self.state = State::Idle;
                    self.poll_next(cx)
                }
            }
        }
    }
}
