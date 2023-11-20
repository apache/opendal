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
use futures::future::BoxFuture;
use futures::FutureExt;
use futures::Stream;
use tokio::task::JoinHandle;

use crate::raw::oio::List;
use crate::raw::*;
use crate::*;

/// Future constructed by stating.
type StatFuture = BoxFuture<'static, Result<Entry>>;

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

    buf: Option<Entry>,
    task_queue: VecDeque<JoinHandle<(String, Result<RpStat>)>>,
    stating: Option<StatFuture>,
    errored: bool,
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

            buf: None,
            task_queue: VecDeque::with_capacity(concurrent),
            stating: None,
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

        if let Some(fut) = self.stating.as_mut() {
            let entry = ready!(fut.poll_unpin(cx));

            // Make sure we will not poll this future again.
            self.stating = None;

            return Poll::Ready(Some(entry));
        }

        if !self.task_queue.is_empty() {
            let task = self.task_queue.pop_back();
            let fut = async move {
                if let Some(task) = task {
                    let (path, rp) = task.await.map_err(|err| {
                        Error::new(
                            ErrorKind::Unexpected,
                            format!("failed to stat: {}", err).as_str(),
                        )
                    })?;
                    let metadata = rp?.into_metadata();
                    Ok(Entry::new(path, metadata))
                } else {
                    Err(Error::new(ErrorKind::Unexpected, "stat task is None"))
                }
            };
            self.stating = Some(Box::pin(fut));
            return self.poll_next(cx);
        }

        if let Some(entry) = self.buf.take() {
            return Poll::Ready(Some(Ok(entry)));
        }

        loop {
            return match ready!(self.lister.poll_next(cx)) {
                Ok(Some(oe)) => {
                    let (path, metadata) = oe.into_entry().into_parts();
                    // TODO: we can optimize this by checking the provided metakey provided by services.
                    if metadata.contains_metakey(self.required_metakey) {
                        return if self.task_queue.is_empty() {
                            Poll::Ready(Some(Ok(Entry::new(path, metadata))))
                        } else {
                            self.buf = Some(Entry::new(path, metadata));
                            self.poll_next(cx)
                        };
                    }

                    if self.task_queue.len() < self.task_queue.capacity() {
                        let acc = self.acc.clone();

                        let fut = async move {
                            let res = acc.stat(&path, OpStat::default()).await;
                            (path, res)
                        };

                        self.task_queue.push_front(tokio::spawn(fut));
                        continue;
                    } else {
                        self.buf = Some(Entry::new(path, metadata));
                        return self.poll_next(cx);
                    };
                }
                Ok(None) => Poll::Ready(None),
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            };
        }
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
