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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]
use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::*;

/// Register the `mime_guess` layer with an operator registry.
///
/// The layer uses [`MimeGuessLayer::new`] and ignores all scoped options.
pub fn register_mime_guess_layer(registry: &OperatorRegistry) {
    registry.register_layer("mime_guess", |_| Ok(MimeGuessLayer::new()));
}

/// `MimeGuessLayer` sets `Content-Type` from a path's file extension.
///
/// # MimeGuess
///
/// This layer uses [mime_guess](https://crates.io/crates/mime_guess) to automatically
/// set `Content-Type` based on the file extension in the operation path.
///
/// The layer preserves any `content_type` that callers or services set.
///
/// On `write`, the layer only injects a guessed `content_type` when the
/// underlying service reports [`Capability::write_with_content_type`].
///
/// For example, object storage services often return `content_type` from `stat`.
/// In that case, the layer keeps the service's value and skips MIME guessing.
///
/// The [Fs](https://docs.rs/opendal/latest/opendal/services/struct.Fs.html)
/// service might omit `content_type` from `stat`, so the layer derives a value
/// from the path's extension.
///
/// The layer cannot infer every custom or uncommon extension. It leaves
/// `content_type` empty when
/// [mime_guess::from_path::first_raw](https://docs.rs/mime_guess/latest/mime_guess/struct.MimeGuess.html#method.first_raw)
/// returns `None`.
///
/// # Examples
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_mime_guess::MimeGuessLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(MimeGuessLayer::new());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct MimeGuessLayer {}

impl MimeGuessLayer {
    /// Create a new [`MimeGuessLayer`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Layer for MimeGuessLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(MimeGuessService(inner))
    }
}

#[derive(Debug)]
struct MimeGuessService(Servicer);

fn mime_from_path(path: &str) -> Option<&str> {
    mime_guess::from_path(path).first_raw()
}

fn opwrite_with_mime(path: &str, op: OpWrite) -> OpWrite {
    if op.content_type().is_some() {
        return op;
    }

    if let Some(mime) = mime_from_path(path) {
        return op.into_content_type(mime);
    }

    op
}

fn opcompose_with_mime(path: &str, op: OpCompose) -> OpCompose {
    if op.content_type().is_some() {
        return op;
    }

    if let Some(mime) = mime_from_path(path) {
        return op.into_content_type(mime);
    }

    op
}

fn rpstat_with_mime(path: &str, rp: RpStat) -> RpStat {
    rp.map_metadata(|metadata| {
        if metadata.content_type().is_some() {
            return metadata;
        }

        if let Some(mime) = mime_from_path(path) {
            let mut metadata = metadata.into_builder();
            metadata.content_type(mime);
            return metadata.build();
        }

        metadata
    })
}

impl Service for MimeGuessService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.0.info()
    }

    fn capability(&self) -> Capability {
        self.0.capability()
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.0.create_dir(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.0.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let args = if self.0.capability().write_with_content_type {
            opwrite_with_mime(path, args)
        } else {
            args
        };
        self.0.write(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.0.copy(ctx, from, to, args)
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.0.compose(ctx, to, opcompose_with_mime(to, args))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.0
            .stat(ctx, path, args)
            .await
            .map(|rp| rpstat_with_mime(path, rp))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.0.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.0.restore(ctx, path, args).await
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.0.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.0.list(ctx, path, args)
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.0.presign(ctx, path, args).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::TryStreamExt;

    use super::*;

    const DATA: &str = "<html>test</html>";
    const CUSTOM: &str = "text/custom";
    const HTML: &str = "text/html";

    #[derive(Debug, Default)]
    struct MockWriter {
        written: u64,
    }

    impl oio::Write for MockWriter {
        async fn write(&mut self, bs: Buffer) -> Result<()> {
            self.written += bs.len() as u64;
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::file(self.written).build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct MockComposer;

    impl oio::Compose for MockComposer {
        async fn compose(&mut self, _path: &str, _args: OpRead) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::file(0).build())
        }
    }

    #[derive(Debug, Default)]
    struct MockService {
        capability: Capability,
        last_write_content_type: Arc<Mutex<Option<String>>>,
        last_compose_content_type: Arc<Mutex<Option<String>>>,
    }

    impl Service for MockService {
        type Reader = ();
        type Writer = MockWriter;
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = MockComposer;

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock")
        }

        fn capability(&self) -> Capability {
            self.capability
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpCreateDir,
        ) -> Result<RpCreateDir> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn write(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            args: OpWrite,
        ) -> Result<Self::Writer> {
            *self.last_write_content_type.lock().unwrap() =
                args.content_type().map(ToOwned::to_owned);
            Ok(MockWriter::default())
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn compose(
            &self,
            _ctx: &OperationContext,
            _to: &str,
            args: OpCompose,
        ) -> Result<Self::Composer> {
            *self.last_compose_content_type.lock().unwrap() =
                args.content_type().map(ToOwned::to_owned);
            Ok(MockComposer)
        }

        async fn stat(
            &self,
            _ctx: &OperationContext,
            _path: &str,
            _args: OpStat,
        ) -> Result<RpStat> {
            Ok(RpStat::new(MetadataBuilder::file(0).build()))
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }
    }

    #[derive(Default)]
    struct MockBuilder {
        service: MockService,
    }

    impl Builder for MockBuilder {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            Ok(self.service)
        }
    }

    #[tokio::test]
    async fn registered_mime_guess_applies_to_memory() -> Result<()> {
        let registry = OperatorRegistry::get();
        register_mime_guess_layer(registry);
        let op = registry.load("memory://?layers=mime_guess&layer.mime_guess.future_option=")?;

        op.write("test0.html", DATA).await?;
        assert_eq!(op.stat("test0.html").await?.content_type(), Some(HTML));

        op.write("test1.asdfghjkl", DATA).await?;
        assert_eq!(op.stat("test1.asdfghjkl").await?.content_type(), None);

        op.write_with("test2.html", DATA)
            .content_type(CUSTOM)
            .await?;
        assert_eq!(op.stat("test2.html").await?.content_type(), Some(CUSTOM));

        let entries = op
            .lister_with("")
            .await?
            .and_then(|entry| {
                let op = op.clone();
                async move { op.stat(entry.path()).await }
            })
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(entries[0].content_type(), Some(HTML));
        assert_eq!(entries[1].content_type(), None);
        assert_eq!(entries[2].content_type(), Some(CUSTOM));

        Ok(())
    }

    #[tokio::test]
    async fn test_write_respects_capability() -> Result<()> {
        let recorded = Arc::new(Mutex::new(None));
        let op = Operator::new(MockBuilder {
            service: MockService {
                capability: Capability {
                    write: true,
                    write_with_content_type: false,
                    ..Default::default()
                },
                last_write_content_type: recorded.clone(),
                ..Default::default()
            },
        })?
        .layer(MimeGuessLayer::new());

        op.write("index.html", "hi").await?;
        assert_eq!(recorded.lock().unwrap().as_deref(), None);

        // Preserve caller-supplied content_type unchanged even when capability is false.
        op.write_with("index.html", "hi")
            .content_type(CUSTOM)
            .await?;
        assert_eq!(recorded.lock().unwrap().as_deref(), Some(CUSTOM));

        let recorded = Arc::new(Mutex::new(None));
        let op = Operator::new(MockBuilder {
            service: MockService {
                capability: Capability {
                    write: true,
                    write_with_content_type: true,
                    ..Default::default()
                },
                last_write_content_type: recorded.clone(),
                ..Default::default()
            },
        })?
        .layer(MimeGuessLayer::new());

        op.write("index.html", "hi").await?;
        assert_eq!(recorded.lock().unwrap().as_deref(), Some(HTML));

        op.write_with("index.html", "hi")
            .content_type(CUSTOM)
            .await?;
        assert_eq!(recorded.lock().unwrap().as_deref(), Some(CUSTOM));

        Ok(())
    }

    #[tokio::test]
    async fn test_compose_guesses_mime_and_preserves_explicit_content_type() -> Result<()> {
        let recorded = Arc::new(Mutex::new(None));
        let op = Operator::new(MockBuilder {
            service: MockService {
                capability: Capability {
                    compose: true,
                    compose_with_content_type: true,
                    ..Default::default()
                },
                last_compose_content_type: recorded.clone(),
                ..Default::default()
            },
        })?
        .layer(MimeGuessLayer::new());

        op.compose(["a", "b"], "out.html").await?;
        assert_eq!(recorded.lock().unwrap().as_deref(), Some(HTML));

        op.compose_with(["a", "b"], "out.html")
            .content_type(CUSTOM)
            .await?;
        assert_eq!(recorded.lock().unwrap().as_deref(), Some(CUSTOM));

        Ok(())
    }
}
