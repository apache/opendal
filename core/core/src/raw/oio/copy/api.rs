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

use crate::raw::*;
use crate::*;

/// Copier is the type-erased [`Copy`].
pub type Copier = Box<dyn CopyDyn>;

/// Copy is the trait that OpenDAL returns for stateful copy operations.
pub trait Copy: Unpin + Send + Sync {
    /// Drive the copy operation forward.
    ///
    /// `Ok(Some(n))` means the copy operation made progress by `n` bytes.
    /// `Ok(None)` means the copy operation has completed.
    fn next(&mut self) -> impl Future<Output = Result<Option<usize>>> + MaybeSend;

    /// Close the copier and return metadata from the server-side completion response.
    fn close(&mut self) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// Abort the pending copy operation.
    fn abort(&mut self) -> impl Future<Output = Result<()>> + MaybeSend;
}

impl Copy for () {
    async fn next(&mut self) -> Result<Option<usize>> {
        Ok(None)
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(Metadata::default())
    }

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

/// OneShotCopier drives a single asynchronous copy step.
pub struct OneShotCopier {
    fut: Option<BoxedStaticFuture<Result<Metadata>>>,
    meta: Option<Metadata>,
}

/// # Safety
///
/// OneShotCopier is only accessed by `&mut self`.
unsafe impl Sync for OneShotCopier {}

/// # Safety
///
/// On wasm targets, futures are local but still only polled through `&mut self`.
unsafe impl Send for OneShotCopier {}

impl OneShotCopier {
    /// Create a new one-shot copier.
    pub fn new(fut: impl Future<Output = Result<Metadata>> + MaybeSend + 'static) -> Self {
        Self {
            fut: Some(Box::pin(fut)),
            meta: None,
        }
    }

    /// Create a one-shot copier that has already completed.
    pub fn completed() -> Self {
        Self {
            fut: None,
            meta: Some(Metadata::default()),
        }
    }
}

impl Copy for OneShotCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        if self.meta.is_none() {
            self.close().await?;
        }

        Ok(None)
    }

    async fn close(&mut self) -> Result<Metadata> {
        if let Some(fut) = self.fut.take() {
            self.meta = Some(fut.await?);
        }

        Ok(self.meta.clone().unwrap_or_default())
    }

    async fn abort(&mut self) -> Result<()> {
        self.fut = None;
        self.meta = None;
        Ok(())
    }
}

/// CopyDyn is the dyn version of [`Copy`].
pub trait CopyDyn: Unpin + Send + Sync {
    /// The dyn version of [`Copy::next`].
    fn next_dyn(&mut self) -> BoxedFuture<'_, Result<Option<usize>>>;

    /// The dyn version of [`Copy::close`].
    fn close_dyn(&mut self) -> BoxedFuture<'_, Result<Metadata>>;

    /// The dyn version of [`Copy::abort`].
    fn abort_dyn(&mut self) -> BoxedFuture<'_, Result<()>>;
}

impl<T: Copy + ?Sized> CopyDyn for T {
    fn next_dyn(&mut self) -> BoxedFuture<'_, Result<Option<usize>>> {
        Box::pin(self.next())
    }

    fn close_dyn(&mut self) -> BoxedFuture<'_, Result<Metadata>> {
        Box::pin(self.close())
    }

    fn abort_dyn(&mut self) -> BoxedFuture<'_, Result<()>> {
        Box::pin(self.abort())
    }
}

impl<T: CopyDyn + ?Sized> Copy for Box<T> {
    async fn next(&mut self) -> Result<Option<usize>> {
        self.deref_mut().next_dyn().await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.deref_mut().close_dyn().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.deref_mut().abort_dyn().await
    }
}
