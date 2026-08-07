// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use opendal_core::raw::*;
use opendal_core::*;
use pin_project::pin_project;

#[derive(Clone, Debug)]
pub struct RuntimeLayer {
    handle: tokio::runtime::Handle,
}

impl RuntimeLayer {
    pub fn new(handle: tokio::runtime::Handle) -> Self {
        Self { handle }
    }
}

impl Layer for RuntimeLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(RuntimeService {
            inner,
            handle: self.handle.clone(),
        })
    }

    fn apply_context(&self, _service: Servicer, inner: OperationContext) -> OperationContext {
        inner.with_executor(Executor::with(RuntimeExecutor {
            handle: self.handle.clone(),
        }))
    }
}

#[derive(Clone)]
struct RuntimeExecutor {
    handle: tokio::runtime::Handle,
}

impl Execute for RuntimeExecutor {
    fn execute(&self, future: BoxedStaticFuture<()>) {
        drop(self.handle.spawn(future));
    }
}

#[derive(Debug)]
struct RuntimeService {
    inner: Servicer,
    handle: tokio::runtime::Handle,
}

impl Service for RuntimeService {
    type Reader = RuntimeReader;
    type Writer = RuntimeWriter;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        RuntimeFuture::new(self.inner.create_dir(ctx, path, args), self.handle.clone()).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        RuntimeFuture::new(self.inner.stat(ctx, path, args), self.handle.clone()).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let _guard = self.handle.enter();
        self.inner.read(ctx, path, args).map(|inner| RuntimeReader {
            inner,
            handle: self.handle.clone(),
        })
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let _guard = self.handle.enter();
        self.inner
            .write(ctx, path, args)
            .map(|inner| RuntimeWriter {
                inner,
                handle: self.handle.clone(),
            })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        let _guard = self.handle.enter();
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let _guard = self.handle.enter();
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        let _guard = self.handle.enter();
        self.inner.copy(ctx, from, to, args, opts)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        RuntimeFuture::new(self.inner.rename(ctx, from, to, args), self.handle.clone()).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        RuntimeFuture::new(self.inner.presign(ctx, path, args), self.handle.clone()).await
    }
}

struct RuntimeReader {
    inner: oio::Reader,
    handle: tokio::runtime::Handle,
}

impl oio::Read for RuntimeReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let (response, stream) =
            RuntimeFuture::new(self.inner.open(range), self.handle.clone()).await?;
        Ok((
            response,
            Box::new(RuntimeReadStream {
                inner: stream,
                handle: self.handle.clone(),
            }),
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        RuntimeFuture::new(self.inner.read(range), self.handle.clone()).await
    }
}

struct RuntimeReadStream {
    inner: Box<dyn oio::ReadStreamDyn>,
    handle: tokio::runtime::Handle,
}

impl oio::ReadStream for RuntimeReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        RuntimeFuture::new(self.inner.read(), self.handle.clone()).await
    }
}

struct RuntimeWriter {
    inner: oio::Writer,
    handle: tokio::runtime::Handle,
}

impl oio::Write for RuntimeWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        RuntimeFuture::new(self.inner.write(buffer), self.handle.clone()).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        RuntimeFuture::new(self.inner.close(), self.handle.clone()).await
    }

    async fn abort(&mut self) -> Result<()> {
        RuntimeFuture::new(self.inner.abort(), self.handle.clone()).await
    }
}

#[pin_project]
struct RuntimeFuture<F> {
    #[pin]
    inner: F,
    handle: tokio::runtime::Handle,
}

impl<F> RuntimeFuture<F> {
    fn new(inner: F, handle: tokio::runtime::Handle) -> Self {
        Self { inner, handle }
    }
}

impl<F: Future> Future for RuntimeFuture<F> {
    type Output = F::Output;

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = this.handle.enter();
        this.inner.poll(context)
    }
}
