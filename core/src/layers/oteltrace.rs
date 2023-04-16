use async_trait::async_trait;
use opentelemetry::global;
use opentelemetry::global::BoxedSpan;
use opentelemetry::trace::FutureExt;
use opentelemetry::trace::Span;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::Tracer;
use opentelemetry::Context;

use crate::ops::*;
use crate::raw::*;
use crate::*;

pub struct OtelTraceLayer;

#[derive(Debug)]
pub struct OtelTraceAccessor<A> {
    inner: A,
}

#[async_trait]
impl<A: Accessor> LayeredAccessor for OtelTraceAccessor<A> {
    type Inner = A;
    type Reader = OtelTraceWrapper<A::Reader>;
    type BlockingReader = OtelTraceWrapper<A::BlockingReader>;
    type Writer = OtelTraceWrapper<A::Writer>;
    type BlockingWriter = OtelTraceWrapper<A::BlockingWriter>;
    type Pager = OtelTraceWrapper<A::Pager>;
    type BlockingPager = OtelTraceWrapper<A::BlockingPager>;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn metadata(&self) -> AccessorInfo {
        let tracer = global::tracer("opendal");
        tracer.in_span("metadata", self.inner.info())
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let tracer = global::tracer("opendal");
        let span = tracer.start("create");
        let cx = Context::current_with_value(span);
        self.inner.create(path, args).with_context(cx).await
    }
}

pub struct OtelTraceWrapper<R> {
    span: BoxedSpan,
    inner: R,
}

impl<R> OtelTraceWrapper<R> {
    fn new(span: BoxedSpan, inner: R) -> Self {
        Self { span, inner }
    }
}

impl<R: oio::Read> oio::Read for OtelTraceWrapper<R> {
    fn poll_read(
        &mut self,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize>> {
        todo!()
    }

    fn poll_seek(
        &mut self,
        cx: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> std::task::Poll<Result<u64>> {
        todo!()
    }

    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<bytes::Bytes>>> {
        todo!()
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for OtelTraceWrapper<R> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        todo!()
    }

    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        todo!()
    }

    fn next(&mut self) -> Option<Result<bytes::Bytes>> {
        todo!()
    }
}

#[async_trait]
impl<R: oio::Write> oio::Write for OtelTraceWrapper<R> {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        todo!()
    }

    async fn append(&mut self, bs: Bytes) -> Result<()> {
        todo!()
    }

    async fn abort(&mut self) -> Result<()> {
        todo!()
    }

    async fn close(&mut self) -> Result<()> {
        todo!()
    }
}

impl<R: oio::BlockingWrite> oio::BlockingWrite for OtelTraceWrapper<R> {
    fn write(&mut self, bs: bytes::Bytes) -> Result<()> {
        todo!()
    }

    fn append(&mut self, bs: bytes::Bytes) -> Result<()> {
        todo!()
    }

    fn close(&mut self) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl<R: oio::Page> oio::Page for OtelTraceWrapper<R> {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        todo!()
    }
}

impl<R: oio::BlockingPage> oio::BlockingPage for OtelTraceWrapper<R> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_trace() {
        println!("hello")
    }
}
