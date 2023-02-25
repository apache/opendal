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

use std::fmt::Debug;
use std::fmt::Formatter;

use async_trait::async_trait;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// TypeEraseLayer will erase the types on internal accesoor.
///
/// # Notes
///
/// TypeEraseLayer is not a public accessible layer that can be used by
/// external users. We use this layer to erase any generice types.
pub struct TypeEraseLayer;

impl<A: Accessor> Layer<A> for TypeEraseLayer {
    type LayeredAccessor = TypeEraseAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccessor {
        TypeEraseAccessor { inner }
    }
}

/// Provide reader wrapper for backend.
pub struct TypeEraseAccessor<A: Accessor> {
    inner: A,
}

impl<A: Accessor> Debug for TypeEraseAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for TypeEraseAccessor<A> {
    type Inner = A;
    type Reader = output::Reader;
    type BlockingReader = output::BlockingReader;
    type Pager = output::Pager;
    type BlockingPager = output::BlockingPager;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, Box::new(r) as output::Reader))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, Box::new(r) as output::BlockingReader))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Pager)> {
        self.inner
            .list(path, args)
            .await
            .map(|(rp, p)| (rp, Box::new(p) as output::Pager))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingPager)> {
        self.inner
            .blocking_list(path, args)
            .map(|(rp, p)| (rp, Box::new(p) as output::BlockingPager))
    }

    async fn scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::Pager)> {
        self.inner
            .scan(path, args)
            .await
            .map(|(rp, p)| (rp, Box::new(p) as output::Pager))
    }

    fn blocking_scan(&self, path: &str, args: OpScan) -> Result<(RpScan, Self::BlockingPager)> {
        self.inner
            .blocking_scan(path, args)
            .map(|(rp, p)| (rp, Box::new(p) as output::BlockingPager))
    }
}
