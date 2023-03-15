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

use std::collections::VecDeque;
use std::mem;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::ready;
use futures::FutureExt;
use futures::Stream;

use crate::raw::*;
use crate::*;

/// Lister is designed to list entries at given path in an asynchronous
/// manner.
///
/// Users can construct Lister by `list` or `scan`.
///
/// User can use lister as `Stream<Item = Result<Entry>>` or
/// call `next_page` directly.
pub struct Lister {
    pager: Option<oio::Pager>,

    buf: VecDeque<oio::Entry>,
    /// We will move `pager` inside future and return it back while future is ready.
    /// Thus, we should not allow calling other function while we already have
    /// a future.
    #[allow(clippy::type_complexity)]
    fut: Option<BoxFuture<'static, (oio::Pager, Result<Option<Vec<oio::Entry>>>)>>,
}

impl Lister {
    /// Create a new lister.
    pub(crate) fn new(pager: oio::Pager) -> Self {
        Self {
            pager: Some(pager),
            buf: VecDeque::default(),
            fut: None,
        }
    }

    /// next_page can be used to fetch a new page.
    ///
    /// # Notes
    ///
    /// Don't mix the usage of `next_page` and `Stream<Item = Result<Entry>>`.
    /// Always using the same calling style.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        debug_assert!(
            self.fut.is_none(),
            "there are ongoing futures for next page"
        );

        let entries = if !self.buf.is_empty() {
            mem::take(&mut self.buf)
        } else {
            match self
                .pager
                .as_mut()
                .expect("pager must be valid")
                .next()
                .await?
            {
                // Ideally, the convert from `Vec` to `VecDeque` will not do reallocation.
                //
                // However, this could be changed as described in [impl<T, A> From<Vec<T, A>> for VecDeque<T, A>](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#impl-From%3CVec%3CT%2C%20A%3E%3E-for-VecDeque%3CT%2C%20A%3E)
                Some(entries) => entries.into(),
                None => return Ok(None),
            }
        };

        Ok(Some(entries.into_iter().map(|v| v.into_entry()).collect()))
    }
}

impl Stream for Lister {
    type Item = Result<Entry>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(oe) = self.buf.pop_front() {
            return Poll::Ready(Some(Ok(oe.into_entry())));
        }

        if let Some(fut) = self.fut.as_mut() {
            let (op, res) = ready!(fut.poll_unpin(cx));
            self.pager = Some(op);

            return match res? {
                Some(oes) => {
                    self.fut = None;
                    self.buf = oes.into();
                    self.poll_next(cx)
                }
                None => {
                    self.fut = None;
                    Poll::Ready(None)
                }
            };
        }

        let mut pager = self.pager.take().expect("pager must be valid");
        let fut = async move {
            let res = pager.next().await;

            (pager, res)
        };
        self.fut = Some(Box::pin(fut));
        self.poll_next(cx)
    }
}

/// BlockingLister is designed to list entries at given path in a blocking
/// manner.
///
/// Users can construct Lister by `blocking_list` or `blocking_scan`.
pub struct BlockingLister {
    pager: oio::BlockingPager,
    buf: VecDeque<oio::Entry>,
}

impl BlockingLister {
    /// Create a new lister.
    pub(crate) fn new(pager: oio::BlockingPager) -> Self {
        Self {
            pager,
            buf: VecDeque::default(),
        }
    }

    /// next_page can be used to fetch a new page.
    pub fn next_page(&mut self) -> Result<Option<Vec<Entry>>> {
        let entries = if !self.buf.is_empty() {
            mem::take(&mut self.buf)
        } else {
            match self.pager.next()? {
                // Ideally, the convert from `Vec` to `VecDeque` will not do reallocation.
                //
                // However, this could be changed as described in [impl<T, A> From<Vec<T, A>> for VecDeque<T, A>](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#impl-From%3CVec%3CT%2C%20A%3E%3E-for-VecDeque%3CT%2C%20A%3E)
                Some(entries) => entries.into(),
                None => return Ok(None),
            }
        };

        Ok(Some(entries.into_iter().map(|v| v.into_entry()).collect()))
    }
}

/// TODO: we can implement next_chunk.
impl Iterator for BlockingLister {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(oe) = self.buf.pop_front() {
            return Some(Ok(oe.into_entry()));
        }

        self.buf = match self.pager.next() {
            // Ideally, the convert from `Vec` to `VecDeque` will not do reallocation.
            //
            // However, this could be changed as described in [impl<T, A> From<Vec<T, A>> for VecDeque<T, A>](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#impl-From%3CVec%3CT%2C%20A%3E%3E-for-VecDeque%3CT%2C%20A%3E)
            Ok(Some(entries)) => entries.into(),
            Ok(None) => return None,
            Err(err) => return Some(Err(err)),
        };

        self.next()
    }
}
