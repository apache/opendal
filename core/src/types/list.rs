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
use std::future::Future;
use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use flagset::FlagSet;
use futures::Stream;
use futures::StreamExt;

use crate::raw::oio::List;
use crate::raw::*;
use crate::*;

/// Lister is designed to list entries at given path in an asynchronous
/// manner.
///
/// Users can construct Lister by [`Operator::lister`] or [`Operator::lister_with`], and can use `metakey` along with list.
/// For example, suppose you need to access `content_length`, you can bring the corresponding field in metakey when listing:
/// `op.list_with("dir/").metakey(Metakey::ContentLength).await?;`.
///
/// - Lister implements `Stream<Item = Result<Entry>>`.
/// - Lister will return `None` if there is no more entries or error has been returned.
pub struct Lister {
    acc: FusedAccessor,
    lister: Option<oio::Lister>,
    /// required_metakey is the metakey required by users.
    required_metakey: FlagSet<Metakey>,

    /// tasks is used to store tasks that are run in concurrent.
    tasks: ConcurrentFutures<StatTask>,
    errored: bool,
}

/// StatTask is used to store the task that is run in concurrent.
///
/// # Note for clippy
///
/// Clippy will raise error for this enum like the following:
///
/// ```shell
/// error: large size difference between variants
///   --> core/src/types/list.rs:64:1
///    |
/// 64 | / enum StatTask {
/// 65 | |     /// BoxFuture is used to store the join handle of spawned task.
/// 66 | |     Handle(BoxFuture<(String, Result<RpStat>)>),
///    | |     -------------------------------------------- the second-largest variant contains at least 0 bytes
/// 67 | |     /// KnownEntry is used to store the entry that already contains the required metakey.
/// 68 | |     KnownEntry(Option<Entry>),
///    | |     ------------------------- the largest variant contains at least 264 bytes
/// 69 | | }
///    | |_^ the entire enum is at least 0 bytes
///    |
///    = help: for further information visit https://rust-lang.github.io/rust-clippy/master/index.html#large_enum_variant
///    = note: `-D clippy::large-enum-variant` implied by `-D warnings`
///    = help: to override `-D warnings` add `#[allow(clippy::large_enum_variant)]`
/// help: consider boxing the large fields to reduce the total size of the enum
///    |
/// 68 |     KnownEntry(Box<Option<Entry>>),
///    |                ~~~~~~~~~~~~~~~~~~
/// ```
/// But this lint is wrong since it doesn't take the generic param JoinHandle into account. In fact, they have exactly
/// the same size:
///
/// ```rust
/// use std::mem::size_of;
///
/// use opendal::Entry;
/// use opendal::Result;
///
/// assert_eq!(264, size_of::<(String, Result<opendal::raw::RpStat>)>());
/// assert_eq!(264, size_of::<Option<Entry>>());
/// ```
///
/// So let's ignore this lint:
#[allow(clippy::large_enum_variant)]
enum StatTask {
    /// Stating is used to store the join handle of spawned task.
    ///
    /// TODO: Replace with static future type after rust supported.
    Stating(BoxedStaticFuture<(String, Result<Metadata>)>),
    /// Known is used to store the entry that already contains the required metakey.
    Known(Option<(String, Metadata)>),
}

impl Future for StatTask {
    type Output = (String, Result<Metadata>);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            StatTask::Stating(fut) => Pin::new(fut).poll(cx),
            StatTask::Known(entry) => {
                let (path, metadata) = entry.take().expect("entry should not be None");
                Poll::Ready((path, Ok(metadata)))
            }
        }
    }
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
            lister: Some(lister),
            required_metakey,

            tasks: ConcurrentFutures::new(concurrent),
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

        // Trying to pull more tasks if there are more space.
        if self.tasks.has_remaining() {
            if let Some(lister) = self.lister.as_mut() {
                match lister.poll_next(cx) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(Some(oe))) => {
                        let (path, metadata) = oe.into_entry().into_parts();
                        if metadata.contains_metakey(self.required_metakey) {
                            self.tasks
                                .push_back(StatTask::Known(Some((path, metadata))));
                        } else {
                            let acc = self.acc.clone();
                            let fut = async move {
                                let res = acc.stat(&path, OpStat::default()).await;
                                (path, res.map(|rp| rp.into_metadata()))
                            };
                            self.tasks.push_back(StatTask::Stating(Box::pin(fut)));
                        }
                    }
                    Poll::Ready(Ok(None)) => {
                        self.lister = None;
                    }
                    Poll::Ready(Err(err)) => {
                        self.errored = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                };
            }
        }

        // Try to poll tasks
        if let Some((path, rp)) = ready!(self.tasks.poll_next_unpin(cx)) {
            let metadata = rp?;
            return Poll::Ready(Some(Ok(Entry::new(path, metadata))));
        }

        if self.lister.is_some() {
            Poll::Pending
        } else {
            Poll::Ready(None)
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
