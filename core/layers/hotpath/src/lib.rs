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

use hotpath::MeasurementGuard;
use opendal_core::raw::*;
use opendal_core::*;

const LABEL_CREATE_DIR: &str = "opendal.create_dir";
const LABEL_READ: &str = "opendal.read";
const LABEL_WRITE: &str = "opendal.write";
const LABEL_COPY: &str = "opendal.copy";
const LABEL_RENAME: &str = "opendal.rename";
const LABEL_STAT: &str = "opendal.stat";
const LABEL_DELETE: &str = "opendal.delete";
const LABEL_LIST: &str = "opendal.list";
const LABEL_PRESIGN: &str = "opendal.presign";

const LABEL_READER_READ: &str = "opendal.reader.read";
const LABEL_WRITER_WRITE: &str = "opendal.writer.write";
const LABEL_WRITER_CLOSE: &str = "opendal.writer.close";
const LABEL_WRITER_ABORT: &str = "opendal.writer.abort";
const LABEL_LISTER_NEXT: &str = "opendal.lister.next";
const LABEL_DELETER_DELETE: &str = "opendal.deleter.delete";
const LABEL_DELETER_CLOSE: &str = "opendal.deleter.close";

/// Add [hotpath](https://docs.rs/hotpath/) profiling for every operation.
///
/// # Notes
///
/// When `hotpath` profiling is enabled, initialize a guard via
/// [`hotpath::FunctionsGuardBuilder`] or `#[hotpath::main]` before running
/// operations. Otherwise, hotpath will panic on the first measurement.
///
/// # Examples
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_hotpath::HotpathLayer;
/// #
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let _guard = hotpath::FunctionsGuardBuilder::new("opendal").build();
/// let op = Operator::new(services::Memory::default())?
///     .layer(HotpathLayer)
///     .finish();
/// op.write("test", "hello").await?;
/// # Ok(())
/// # }
/// ```
pub struct HotpathLayer;

impl<A: Access> Layer<A> for HotpathLayer {
    type LayeredAccess = HotpathAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        HotpathAccessor { inner }
    }
}

#[derive(Debug)]
pub struct HotpathAccessor<A> {
    inner: A,
}

impl<A: Access> LayeredAccess for HotpathAccessor<A> {
    type Inner = A;
    type Reader = HotpathWrapper<A::Reader>;
    type Writer = HotpathWrapper<A::Writer>;
    type Lister = HotpathWrapper<A::Lister>;
    type Deleter = HotpathWrapper<A::Deleter>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> Result<RpCreateDir> {
        let _guard = MeasurementGuard::build(LABEL_CREATE_DIR, false, true);
        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let _guard = MeasurementGuard::build(LABEL_READ, false, true);
        let (rp, reader) = self.inner.read(path, args).await?;
        Ok((rp, HotpathWrapper::new(reader)))
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        let _guard = MeasurementGuard::build(LABEL_WRITE, false, true);
        let (rp, writer) = self.inner.write(path, args).await?;
        Ok((rp, HotpathWrapper::new(writer)))
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> Result<RpCopy> {
        let _guard = MeasurementGuard::build(LABEL_COPY, false, true);
        self.inner().copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> Result<RpRename> {
        let _guard = MeasurementGuard::build(LABEL_RENAME, false, true);
        self.inner().rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> Result<RpStat> {
        let _guard = MeasurementGuard::build(LABEL_STAT, false, true);
        self.inner.stat(path, args).await
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        let _guard = MeasurementGuard::build(LABEL_DELETE, false, true);
        let (rp, deleter) = self.inner.delete().await?;
        Ok((rp, HotpathWrapper::new(deleter)))
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        let _guard = MeasurementGuard::build(LABEL_LIST, false, true);
        let (rp, lister) = self.inner.list(path, args).await?;
        Ok((rp, HotpathWrapper::new(lister)))
    }

    async fn presign(&self, path: &str, args: OpPresign) -> Result<RpPresign> {
        let _guard = MeasurementGuard::build(LABEL_PRESIGN, false, true);
        self.inner.presign(path, args).await
    }
}

pub struct HotpathWrapper<R> {
    inner: R,
}

impl<R> HotpathWrapper<R> {
    fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R: oio::Read> oio::Read for HotpathWrapper<R> {
    async fn read(&mut self) -> Result<Buffer> {
        let _guard = MeasurementGuard::build(LABEL_READER_READ, false, true);
        self.inner.read().await
    }
}

impl<R: oio::Write> oio::Write for HotpathWrapper<R> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let _guard = MeasurementGuard::build(LABEL_WRITER_WRITE, false, true);
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        let _guard = MeasurementGuard::build(LABEL_WRITER_CLOSE, false, true);
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        let _guard = MeasurementGuard::build(LABEL_WRITER_ABORT, false, true);
        self.inner.abort().await
    }
}

impl<R: oio::List> oio::List for HotpathWrapper<R> {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let _guard = MeasurementGuard::build(LABEL_LISTER_NEXT, false, true);
        self.inner.next().await
    }
}

impl<R: oio::Delete> oio::Delete for HotpathWrapper<R> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let _guard = MeasurementGuard::build(LABEL_DELETER_DELETE, false, true);
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        let _guard = MeasurementGuard::build(LABEL_DELETER_CLOSE, false, true);
        self.inner.close().await
    }
}
