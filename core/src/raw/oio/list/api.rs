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

use std::future::Future;
use std::ops::DerefMut;

use crate::raw::oio::Entry;
use crate::raw::*;
use crate::*;

/// The boxed version of [`List`]
pub type Lister = Box<dyn ListDyn>;

/// Page trait is used by [`raw::Accessor`] to implement `list` operation.
pub trait List: Unpin + Send + Sync {
    /// Fetch a new page of [`Entry`]
    ///
    /// `Ok(None)` means all pages have been returned. Any following call
    /// to `next` will always get the same result.
    fn next(&mut self) -> impl Future<Output = Result<Option<Entry>>> + MaybeSend;
}

impl List for () {
    async fn next(&mut self) -> Result<Option<Entry>> {
        Ok(None)
    }
}

impl<P: List> List for Option<P> {
    async fn next(&mut self) -> Result<Option<Entry>> {
        match self {
            Some(p) => p.next().await,
            None => Ok(None),
        }
    }
}

pub trait ListDyn: Unpin + Send + Sync {
    fn next_dyn(&mut self) -> BoxedFuture<Result<Option<Entry>>>;
}

impl<T: List + ?Sized> ListDyn for T {
    fn next_dyn(&mut self) -> BoxedFuture<Result<Option<Entry>>> {
        Box::pin(self.next())
    }
}

impl<T: ListDyn + ?Sized> List for Box<T> {
    async fn next(&mut self) -> Result<Option<Entry>> {
        self.deref_mut().next_dyn().await
    }
}

/// BlockingList is the blocking version of [`List`].
pub trait BlockingList: Send {
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
