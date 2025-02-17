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

use std::fmt::Debug;
use std::sync::Arc;

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;

use crate::raw::*;
use crate::*;

/// Add concurrent request limit.
///
/// # Notes
///
/// Users can control how many concurrent connections could be established
/// between OpenDAL and underlying storage services.
///
/// # Examples
///
/// ```no_run
/// # use opendal::layers::ConcurrentLimitLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(ConcurrentLimitLayer::new(1024))
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ConcurrentLimitLayer {
    permits: usize,
}

impl ConcurrentLimitLayer {
    /// Create a new ConcurrentLimitLayer will specify permits
    pub fn new(permits: usize) -> Self {
        Self { permits }
    }
}

impl<A: Access> Layer<A> for ConcurrentLimitLayer {
    type LayeredAccess = ConcurrentLimitAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        ConcurrentLimitAccessor {
            inner,
            semaphore: Arc::new(Semaphore::new(self.permits)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConcurrentLimitAccessor<A: Access> {
    inner: A,
    semaphore: Arc<Semaphore>,
}

impl<A: Access> LayeredAccess for ConcurrentLimitAccessor<A> {
    type Inner = A;
    type Reader = ConcurrentLimitWrapper<A::Reader>;
    type BlockingReader = ConcurrentLimitWrapper<A::BlockingReader>;
    type Writer = ConcurrentLimitWrapper<A::Writer>;
    type BlockingWriter = ConcurrentLimitWrapper<A::BlockingWriter>;
    type Lister = ConcurrentLimitWrapper<A::Lister>;
    type BlockingLister = ConcurrentLimitWrapper<A::BlockingLister>;
    type Deleter = ConcurrentLimitWrapper<A::Deleter>;
    type BlockingDeleter = ConcurrentLimitWrapper<A::BlockingDeleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .read(path, args)
            .await
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .write(path, args)
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore must be valid");

        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .delete()
            .await
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore must be valid");

        self.inner
            .list(path, args)
            .await
            .map(|(rp, s)| (rp, ConcurrentLimitWrapper::new(s, permit)))
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_create_dir(path, args)
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_read(path, args)
            .map(|(rp, r)| (rp, ConcurrentLimitWrapper::new(r, permit)))
    }

    fn blocking_write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::BlockingWriter)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_write(path, args)
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _permit = self
            .semaphore
            .try_acquire()
            .expect("semaphore must be valid");

        self.inner.blocking_stat(path, args)
    }

    fn blocking_delete(&self) -> Result<(RpDelete, Self::BlockingDeleter)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_delete()
            .map(|(rp, w)| (rp, ConcurrentLimitWrapper::new(w, permit)))
    }

    fn blocking_list(&self, path: &str, args: OpList) -> Result<(RpList, Self::BlockingLister)> {
        let permit = self
            .semaphore
            .clone()
            .try_acquire_owned()
            .expect("semaphore must be valid");

        self.inner
            .blocking_list(path, args)
            .map(|(rp, it)| (rp, ConcurrentLimitWrapper::new(it, permit)))
    }
}

pub struct ConcurrentLimitWrapper<R> {
    inner: R,

    // Hold on this permit until this reader has been dropped.
    _permit: OwnedSemaphorePermit,
}

impl<R> ConcurrentLimitWrapper<R> {
    fn new(inner: R, permit: OwnedSemaphorePermit) -> Self {
        Self {
            inner,
            _permit: permit,
        }
    }
}

impl<R: oio::Read> oio::Read for ConcurrentLimitWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        self.inner.read().await
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for ConcurrentLimitWrapper<R> {
    fn read(&mut self) -> Result<Buffer> {
        self.inner.read()
    }
}

impl<R: oio::Write> oio::Write for ConcurrentLimitWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for ConcurrentLimitWrapper<R> {
    fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs)
    }

    fn close(&mut self) -> Result<Metadata> {
        self.inner.close()
    }
}

impl<R: oio::List> oio::List for ConcurrentLimitWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next().await
    }
}

impl<R: oio::BlockingList> oio::BlockingList for ConcurrentLimitWrapper<R> {
    fn next(&mut self) -> Result<Option<oio::Entry>> {
        self.inner.next()
    }
}

impl<R: oio::Delete> oio::Delete for ConcurrentLimitWrapper<R> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        self.inner.flush().await
    }
}

impl<R: oio::BlockingDelete> oio::BlockingDelete for ConcurrentLimitWrapper<R> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.inner.delete(path, args)
    }

    fn flush(&mut self) -> Result<usize> {
        self.inner.flush()
    }
}
