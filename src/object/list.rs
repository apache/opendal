// Copyright 2022 Datafuse Labs
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

/// ObjectLister is returned by `Object::list` to list objects.
///
/// User can use object lister as `Stream<Item = Result<Object>>` or
/// call `next_page` directly.
pub struct ObjectLister {
    acc: FusedAccessor,
    pager: Option<output::Pager>,

    buf: VecDeque<output::Entry>,
    /// We will move `pager` inside future and return it back while future is ready.
    /// Thus, we should not allow calling other function while we already have
    /// a future.
    #[allow(clippy::type_complexity)]
    fut: Option<BoxFuture<'static, (output::Pager, Result<Option<Vec<output::Entry>>>)>>,
}

impl ObjectLister {
    /// Create a new object lister.
    pub(crate) fn new(acc: FusedAccessor, pager: output::Pager) -> Self {
        Self {
            acc,
            pager: Some(pager),
            buf: VecDeque::default(),
            fut: None,
        }
    }

    /// Fetch the operator that used by this object.
    pub(crate) fn operator(&self) -> Operator {
        self.acc.clone().into()
    }

    /// next_page can be used to fetch a new object page.
    ///
    /// # Notes
    ///
    /// Don't mix the usage of `next_page` and `Stream<Item = Result<Object>>`.
    /// Always using the same calling style.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Object>>> {
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
                .next_page()
                .await?
            {
                // Ideally, the convert from `Vec` to `VecDeque` will not do reallocation.
                //
                // However, this could be changed as described in [impl<T, A> From<Vec<T, A>> for VecDeque<T, A>](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#impl-From%3CVec%3CT%2C%20A%3E%3E-for-VecDeque%3CT%2C%20A%3E)
                Some(entries) => entries.into(),
                None => return Ok(None),
            }
        };

        Ok(Some(
            entries
                .into_iter()
                .map(|v| v.into_object(self.operator()))
                .collect(),
        ))
    }
}

impl Stream for ObjectLister {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(oe) = self.buf.pop_front() {
            return Poll::Ready(Some(Ok(oe.into_object(self.operator()))));
        }

        if let Some(fut) = self.fut.as_mut() {
            let (op, res) = ready!(fut.poll_unpin(cx));
            self.pager = Some(op);

            match res? {
                Some(oes) => {
                    self.fut = None;
                    self.buf = oes.into();
                    return self.poll_next(cx);
                }
                None => {
                    self.fut = None;
                    return Poll::Ready(None);
                }
            }
        }

        let mut pager = self.pager.take().expect("pager must be valid");
        let fut = async move {
            let res = pager.next_page().await;

            (pager, res)
        };
        self.fut = Some(Box::pin(fut));
        self.poll_next(cx)
    }
}

pub struct BlockingObjectLister {
    acc: FusedAccessor,
    pager: output::BlockingPager,
    buf: VecDeque<output::Entry>,
}

impl BlockingObjectLister {
    /// Create a new object lister.
    pub(crate) fn new(acc: FusedAccessor, pager: output::BlockingPager) -> Self {
        Self {
            acc,
            pager,
            buf: VecDeque::default(),
        }
    }

    /// Fetch the operator that used by this object.
    pub(crate) fn operator(&self) -> Operator {
        self.acc.clone().into()
    }

    /// next_page can be used to fetch a new object page.
    pub fn next_page(&mut self) -> Result<Option<Vec<Object>>> {
        let entries = if !self.buf.is_empty() {
            mem::take(&mut self.buf)
        } else {
            match self.pager.next_page()? {
                // Ideally, the convert from `Vec` to `VecDeque` will not do reallocation.
                //
                // However, this could be changed as described in [impl<T, A> From<Vec<T, A>> for VecDeque<T, A>](https://doc.rust-lang.org/std/collections/struct.VecDeque.html#impl-From%3CVec%3CT%2C%20A%3E%3E-for-VecDeque%3CT%2C%20A%3E)
                Some(entries) => entries.into(),
                None => return Ok(None),
            }
        };

        Ok(Some(
            entries
                .into_iter()
                .map(|v| v.into_object(self.operator()))
                .collect(),
        ))
    }
}

/// TODO: we can implement next_chunk.
impl Iterator for BlockingObjectLister {
    type Item = Result<Object>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(oe) = self.buf.pop_front() {
            return Some(Ok(oe.into_object(self.operator())));
        }

        self.buf = match self.pager.next_page() {
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
