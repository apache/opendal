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

use std::cmp;
use std::collections::VecDeque;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use flagset::FlagSet;
use futures::FutureExt;
use futures::Stream;
use tokio::task::JoinHandle;

use crate::raw::oio::List;
use crate::raw::*;
use crate::*;

/// Lister is designed to list entries at given path in an asynchronous
/// manner.
///
/// Users can construct Lister by [`Operator::lister`] or [`Operator::lister_with`].
///
/// - Lister implements `Stream<Item = Result<Entry>>`.
/// - Lister will return `None` if there is no more entries or error has been returned.
pub struct Lister {
    acc: FusedAccessor,
    lister: oio::Lister,
    /// required_metakey is the metakey required by users.
    required_metakey: FlagSet<Metakey>,

    /// listing is used to indicate whether we are listing entries or stat entries.
    listing: bool,
    /// task_queue is used to store tasks that are run in concurrent.
    task_queue: VecDeque<Task>,
    errored: bool,
}

enum Task {
    /// Handle is used to store the join handle of spawned task.
    Handle(JoinHandle<(String, Result<RpStat>)>),
    /// KnownEntry is used to store the entry that already contains the required metakey.
    KnownEntry(Box<Option<(String, Metadata)>>),
}

/// # Safety
///
/// Lister will only be accessed by `&mut Self`
unsafe impl Sync for Lister {}

impl Lister {
    /// Create a new lister.
    pub(crate) async fn create(acc: FusedAccessor, path: &str, args: OpList) -> Result<Self> {
        let required_metakey = args.metakey();
        let concurrent = cmp::max(1, args.concurrent());

        let (_, lister) = acc.list(path, args).await?;

        Ok(Self {
            acc,
            lister,
            required_metakey,

            listing: true,
            task_queue: VecDeque::with_capacity(concurrent),
            errored: false,
        })
    }
}

impl Stream for Lister {
    type Item = Result<Entry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Returns `None` if we have errored.
        if self.errored {
            return Poll::Ready(None);
        }

        if self.listing && self.task_queue.len() < self.task_queue.capacity() {
            return match self.lister.poll_next(cx) {
                Poll::Pending => {
                    if self.task_queue.is_empty() {
                        return Poll::Pending;
                    }
                    self.listing = false;
                    self.poll_next(cx)
                }
                Poll::Ready(Ok(Some(oe))) => {
                    let (path, metadata) = oe.into_entry().into_parts();
                    // TODO: we can optimize this by checking the provided metakey provided by services.
                    if metadata.contains_metakey(self.required_metakey) {
                        self.task_queue
                            .push_back(Task::KnownEntry(Box::new(Some((path, metadata)))));
                    } else {
                        let acc = self.acc.clone();
                        let fut = async move {
                            let res = acc.stat(&path, OpStat::default()).await;
                            (path, res)
                        };
                        self.task_queue.push_back(Task::Handle(tokio::spawn(fut)));
                    }
                    self.poll_next(cx)
                }
                Poll::Ready(Ok(None)) => {
                    if self.task_queue.is_empty() {
                        return Poll::Ready(None);
                    }
                    self.listing = false;
                    self.poll_next(cx)
                }
                Poll::Ready(Err(err)) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            };
        }

        if let Some(handle) = self.task_queue.front_mut() {
            return match handle {
                Task::Handle(handle) => {
                    let (path, rp) = ready!(handle.poll_unpin(cx)).map_err(new_task_join_error)?;

                    match rp {
                        Ok(rp) => {
                            self.task_queue.pop_back();
                            let metadata = rp.into_metadata();
                            Poll::Ready(Some(Ok(Entry::new(path, metadata))))
                        }
                        Err(err) => {
                            self.errored = true;
                            Poll::Ready(Some(Err(err)))
                        }
                    }
                }
                Task::KnownEntry(entry) => {
                    if let Some((path, metadata)) = entry.take() {
                        Poll::Ready(Some(Ok(Entry::new(path, metadata))))
                    } else {
                        self.task_queue.pop_back();
                        self.poll_next(cx)
                    }
                }
            };
        } else {
            self.listing = true;
        }

        Poll::Ready(None)
    }
}

/// BlockingLister is designed to list entries at given path in a blocking
/// manner.
///
/// Users can construct Lister by [`BlockingOperator::lister`] or [`BlockingOperator::lister_with`].
///
/// - Lister implements `Iterator<Item = Result<Entry>>`.
/// - Lister will return `None` if there is no more entries or error has been returned.
pub struct BlockingLister {
    acc: FusedAccessor,
    /// required_metakey is the metakey required by users.
    required_metakey: FlagSet<Metakey>,

    lister: oio::BlockingLister,
    errored: bool,
}

/// # Safety
///
/// BlockingLister will only be accessed by `&mut Self`
unsafe impl Sync for BlockingLister {}

impl BlockingLister {
    /// Create a new lister.
    pub(crate) fn create(acc: FusedAccessor, path: &str, args: OpList) -> Result<Self> {
        let required_metakey = args.metakey();
        let (_, lister) = acc.blocking_list(path, args)?;

        Ok(Self {
            acc,
            required_metakey,

            lister,
            errored: false,
        })
    }
}

/// TODO: we can implement next_chunk.
impl Iterator for BlockingLister {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        // Returns `None` if we have errored.
        if self.errored {
            return None;
        }

        let entry = match self.lister.next() {
            Ok(Some(entry)) => entry,
            Ok(None) => return None,
            Err(err) => {
                self.errored = true;
                return Some(Err(err));
            }
        };

        let (path, metadata) = entry.into_entry().into_parts();
        // TODO: we can optimize this by checking the provided metakey provided by services.
        if metadata.contains_metakey(self.required_metakey) {
            return Some(Ok(Entry::new(path, metadata)));
        }

        let metadata = match self.acc.blocking_stat(&path, OpStat::default()) {
            Ok(rp) => rp.into_metadata(),
            Err(err) => {
                self.errored = true;
                return Some(Err(err));
            }
        };
        Some(Ok(Entry::new(path, metadata)))
    }
}

#[cfg(test)]
mod tests {
    use futures::future;
    use futures::StreamExt;

    use super::*;
    use crate::services::Azblob;

    /// Inspired by <https://gist.github.com/kyle-mccarthy/1e6ae89cc34495d731b91ebf5eb5a3d9>
    ///
    /// Invalid lister should not panic nor endless loop.
    #[tokio::test]
    async fn test_invalid_lister() -> Result<()> {
        let _ = tracing_subscriber::fmt().try_init();

        let mut builder = Azblob::default();

        builder
            .container("container")
            .account_name("account_name")
            .account_key("account_key")
            .endpoint("https://account_name.blob.core.windows.net");

        let operator = Operator::new(builder)?.finish();

        let lister = operator.lister("/").await?;

        lister
            .filter_map(|entry| {
                dbg!(&entry);
                future::ready(entry.ok())
            })
            .for_each(|entry| {
                println!("{:?}", entry);
                future::ready(())
            })
            .await;

        Ok(())
    }
}
