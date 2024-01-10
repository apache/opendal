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

use std::fmt::Display;
use std::fmt::Formatter;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::raw::oio::Entry;
use crate::*;

/// PageOperation is the name for APIs of lister.
#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
#[non_exhaustive]
pub enum ListOperation {
    /// Operation for [`List::poll_next`]
    Next,
    /// Operation for [`BlockingList::next`]
    BlockingNext,
}

impl ListOperation {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for ListOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.into_static())
    }
}

impl From<ListOperation> for &'static str {
    fn from(v: ListOperation) -> &'static str {
        use ListOperation::*;

        match v {
            Next => "List::next",
            BlockingNext => "List::next",
        }
    }
}

/// Page trait is used by [`raw::Accessor`] to implement `list` operation.
pub trait List: Unpin + Send + Sync + 'static {
    /// Fetch a new page of [`Entry`]
    ///
    /// `Ok(None)` means all pages have been returned. Any following call
    /// to `next` will always get the same result.
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Entry>>>;
}

/// The boxed version of [`List`]
pub type Lister = Box<dyn List>;

impl<P: List + ?Sized> List for Box<P> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Entry>>> {
        (**self).poll_next(cx)
    }
}

impl List for () {
    fn poll_next(&mut self, _: &mut Context<'_>) -> Poll<Result<Option<Entry>>> {
        Poll::Ready(Ok(None))
    }
}

impl<P: List> List for Option<P> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Entry>>> {
        match self {
            Some(p) => p.poll_next(cx),
            None => Poll::Ready(Ok(None)),
        }
    }
}

/// Impl ListExt for all T: List
impl<T: List> ListExt for T {}

/// Extension of [`List`] to make it easier for use.
pub trait ListExt: List {
    /// Build a future for `poll_next`.
    fn next(&mut self) -> NextFuture<Self> {
        NextFuture { lister: self }
    }
}

pub struct NextFuture<'a, L: List + Unpin + ?Sized> {
    lister: &'a mut L,
}

impl<L> Future for NextFuture<'_, L>
where
    L: List + Unpin + ?Sized,
{
    type Output = Result<Option<Entry>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Option<Entry>>> {
        self.lister.poll_next(cx)
    }
}

/// BlockingList is the blocking version of [`List`].
pub trait BlockingList: Send + 'static {
    /// Fetch a new page of [`Entry`]
    ///
    /// `Ok(None)` means all pages have been returned. Any following call
    /// to `next` will always get the same result.
    fn next(&mut self) -> Result<Option<Entry>>;
}

/// BlockingLister is a boxed [`BlockingList`]
pub type BlockingLister = Box<dyn BlockingList>;

impl<P: BlockingList + ?Sized> BlockingList for Box<P> {
    fn next(&mut self) -> Result<Option<Entry>> {
        (**self).next()
    }
}

impl BlockingList for () {
    fn next(&mut self) -> Result<Option<Entry>> {
        Ok(None)
    }
}

impl<P: BlockingList> BlockingList for Option<P> {
    fn next(&mut self) -> Result<Option<Entry>> {
        match self {
            Some(p) => p.next(),
            None => Ok(None),
        }
    }
}
