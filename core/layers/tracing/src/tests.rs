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

use std::sync::Mutex;

use tracing::Subscriber;
use tracing::span::Attributes;
use tracing::span::Id;
use tracing_subscriber::layer::Context as LayerContext;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

use super::*;

#[derive(Debug, PartialEq, Eq)]
struct SpanLink {
    name: String,
    parent: Option<String>,
}

#[derive(Clone, Default)]
struct SpanRecorder {
    created_with_parent: Arc<Mutex<Vec<SpanLink>>>,
    closed: Arc<Mutex<Vec<String>>>,
}

impl<S> tracing_subscriber::Layer<S> for SpanRecorder
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, _attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        let parent = span.parent().map(|parent| parent.name().to_string());
        self.created_with_parent.lock().unwrap().push(SpanLink {
            name: span.name().to_string(),
            parent,
        });
    }

    fn on_close(&self, id: Id, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(&id) {
            self.closed.lock().unwrap().push(span.name().to_string());
        }
    }
}

#[derive(Debug)]
struct MockService {
    closed: Arc<Mutex<Vec<String>>>,
}

impl Service for MockService {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();
    type Copier = MockCopier;
    type Composer = MockComposer;

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new("mock", "/", "mock")
    }

    fn capability(&self) -> Capability {
        Capability {
            copy: true,
            compose: true,
            ..Default::default()
        }
    }

    async fn create_dir(
        &self,
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        unimplemented!()
    }

    async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
        unimplemented!()
    }

    fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
        unimplemented!()
    }

    fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
        unimplemented!()
    }

    fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
        unimplemented!()
    }

    fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
        unimplemented!()
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        unimplemented!()
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        unimplemented!()
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        let _span = span!(Level::DEBUG, "mock.copy.create");
        Ok(MockCopier {
            closed: self.closed.clone(),
            step: 0,
        })
    }

    fn compose(
        &self,
        _ctx: &OperationContext,
        _to: &str,
        _args: OpCompose,
    ) -> Result<Self::Composer> {
        let _span = span!(Level::DEBUG, "mock.compose.create");
        Ok(MockComposer {
            closed: self.closed.clone(),
            sources: Vec::new(),
        })
    }
}

struct MockCopier {
    closed: Arc<Mutex<Vec<String>>>,
    step: usize,
}

impl oio::Copy for MockCopier {
    async fn next(&mut self) -> Result<Option<usize>> {
        tokio::task::yield_now().await;
        assert!(
            !self
                .closed
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "copy"),
            "copy span must remain open during Copier::next"
        );
        let _span = span!(Level::DEBUG, "mock.copier.next");
        if self.step == 0 {
            self.step = 1;
            Ok(Some(7))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        tokio::task::yield_now().await;
        assert!(
            !self
                .closed
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "copy"),
            "copy span must remain open during Copier::close"
        );
        let _span = span!(Level::DEBUG, "mock.copier.close");
        Ok(MetadataBuilder::file(7).build())
    }

    async fn abort(&mut self) -> Result<()> {
        tokio::task::yield_now().await;
        assert!(
            !self
                .closed
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "copy"),
            "copy span must remain open during Copier::abort"
        );
        let _span = span!(Level::DEBUG, "mock.copier.abort");
        Ok(())
    }
}

struct MockComposer {
    closed: Arc<Mutex<Vec<String>>>,
    sources: Vec<String>,
}

impl oio::Compose for MockComposer {
    async fn compose(&mut self, path: &str, _args: OpRead) -> Result<()> {
        tokio::task::yield_now().await;
        assert!(
            !self
                .closed
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "compose"),
            "compose span must remain open during Composer::compose"
        );
        let _span = span!(Level::DEBUG, "mock.composer.compose");
        self.sources.push(path.to_string());
        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        tokio::task::yield_now().await;
        assert!(
            !self
                .closed
                .lock()
                .unwrap()
                .iter()
                .any(|name| name == "compose"),
            "compose span must remain open during Composer::close"
        );
        let _span = span!(Level::DEBUG, "mock.composer.close");
        Ok(MetadataBuilder::file(self.sources.len() as u64).build())
    }
}

#[tokio::test]
async fn copy_span_stays_active_across_copier_operations() {
    let recorder = SpanRecorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let service = TracingLayer::new().apply_service(Arc::new(MockService {
        closed: recorder.closed.clone(),
    }));
    let ctx = OperationContext::default();

    let mut copier = service.copy(&ctx, "src", "dst", OpCopy::default()).unwrap();
    assert!(!recorder.closed.lock().unwrap().iter().any(|s| s == "copy"));
    assert_eq!(oio::Copy::next(&mut copier).await.unwrap(), Some(7));
    assert_eq!(oio::Copy::next(&mut copier).await.unwrap(), None);
    let meta = oio::Copy::close(&mut copier).await.unwrap();
    assert_eq!(meta.content_length(), 7);
    assert!(!recorder.closed.lock().unwrap().iter().any(|s| s == "copy"));
    drop(copier);
    assert_eq!(
        recorder
            .closed
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == "copy")
            .count(),
        1
    );

    recorder.closed.lock().unwrap().clear();
    let mut aborted = service
        .copy(&ctx, "src", "dst2", OpCopy::default())
        .unwrap();
    oio::Copy::abort(&mut aborted).await.unwrap();
    drop(aborted);

    let parents = recorder.created_with_parent.lock().unwrap();
    for (name, parent) in [
        ("mock.copy.create", Some("copy")),
        ("mock.copier.next", Some("copy")),
        ("mock.copier.close", Some("copy")),
        ("mock.copier.abort", Some("copy")),
    ] {
        let expected = SpanLink {
            name: name.to_string(),
            parent: parent.map(ToString::to_string),
        };
        assert!(
            parents.contains(&expected),
            "missing expected span relationship {expected:?} in {parents:?}"
        );
    }
}

#[tokio::test]
async fn compose_span_stays_active_across_composer_operations() {
    let recorder = SpanRecorder::default();
    let subscriber = tracing_subscriber::registry().with(recorder.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let service = TracingLayer::new().apply_service(Arc::new(MockService {
        closed: recorder.closed.clone(),
    }));
    let ctx = OperationContext::default();

    let mut composer = service.compose(&ctx, "dst", OpCompose::default()).unwrap();
    assert!(
        !recorder
            .closed
            .lock()
            .unwrap()
            .iter()
            .any(|s| s == "compose")
    );
    oio::Compose::compose(&mut composer, "part1", OpRead::default())
        .await
        .unwrap();
    let meta = oio::Compose::close(&mut composer).await.unwrap();
    assert_eq!(meta.content_length(), 1);
    assert!(
        !recorder
            .closed
            .lock()
            .unwrap()
            .iter()
            .any(|s| s == "compose")
    );
    drop(composer);
    assert_eq!(
        recorder
            .closed
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.as_str() == "compose")
            .count(),
        1
    );

    let parents = recorder.created_with_parent.lock().unwrap();
    for (name, parent) in [
        ("mock.compose.create", Some("compose")),
        ("mock.composer.compose", Some("compose")),
        ("mock.composer.close", Some("compose")),
    ] {
        let expected = SpanLink {
            name: name.to_string(),
            parent: parent.map(ToString::to_string),
        };
        assert!(
            parents.contains(&expected),
            "missing expected span relationship {expected:?} in {parents:?}"
        );
    }
}
