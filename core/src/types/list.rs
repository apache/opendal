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

use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use futures::Stream;

use crate::raw::*;
use crate::*;

/// Lister is designed to list entries at given path in an asynchronous
/// manner.
///
/// - Lister implements `Stream<Item = Result<Entry>>`.
/// - Lister will return `None` if there is no more entries or error has been returned.
pub struct Lister {
    lister: Option<oio::Lister>,

    fut: Option<BoxedStaticFuture<(oio::Lister, Result<Option<oio::Entry>>)>>,
    errored: bool,
}

/// # Safety
///
/// Lister will only be accessed by `&mut Self`
unsafe impl Sync for Lister {}

impl Lister {
    /// Create a new lister.
    pub(crate) async fn create(acc: Accessor, path: &str, args: OpList) -> Result<Self> {
        let (_, lister) = acc.list(path, args).await?;

        Ok(Self {
            lister: Some(lister),

            fut: None,
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

        if let Some(mut lister) = self.lister.take() {
            let fut = async move {
                let res = lister.next_dyn().await;
                (lister, res)
            };
            self.fut = Some(Box::pin(fut));
        }

        if let Some(fut) = self.fut.as_mut() {
            let (lister, entry) = ready!(fut.as_mut().poll(cx));
            self.lister = Some(lister);
            self.fut = None;

            return match entry {
                Ok(Some(oe)) => Poll::Ready(Some(Ok(oe.into_entry()))),
                Ok(None) => {
                    self.lister = None;
                    Poll::Ready(None)
                }
                Err(err) => {
                    self.errored = true;
                    Poll::Ready(Some(Err(err)))
                }
            };
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
    lister: oio::BlockingLister,
    errored: bool,
}

/// # Safety
///
/// BlockingLister will only be accessed by `&mut Self`
unsafe impl Sync for BlockingLister {}

impl BlockingLister {
    /// Create a new lister.
    pub(crate) fn create(acc: Accessor, path: &str, args: OpList) -> Result<Self> {
        let (_, lister) = acc.blocking_list(path, args)?;

        Ok(Self {
            lister,
            errored: false,
        })
    }
}

impl Iterator for BlockingLister {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        // Returns `None` if we have errored.
        if self.errored {
            return None;
        }

        match self.lister.next() {
            Ok(Some(entry)) => Some(Ok(entry.into_entry())),
            Ok(None) => None,
            Err(err) => {
                self.errored = true;
                Some(Err(err))
            }
        }
    }
}

#[cfg(test)]
#[cfg(feature = "services-azblob")]
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

        let builder = Azblob::default()
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
