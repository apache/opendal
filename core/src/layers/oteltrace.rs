use opentelemetry::global;
use opentelemetry::trace::FutureExt;
use opentelemetry::trace::Span;
use opentelemetry::trace::TraceContextExt;
use opentelemetry::trace::Tracer;
use opentelemetry::Context;

use crate::ops::*;
use crate::raw::*;

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
        let cx = Context::current_with_value(span);
        self.inner.create(path, args).with_context(cx).await
    }
}

pub struct OtelTraceWrapper<R> {
    span: Span,
    inner: R,
}

impl<R> OtelTraceWrapper<R> {
    fn new(span: Span, inner: R) -> Self {
        Self { span, inner }
    }
}
