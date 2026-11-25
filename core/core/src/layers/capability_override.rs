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

use std::fmt;
use std::sync::Arc;

use serde_json::Map;
use serde_json::Number;
use serde_json::Value;

use crate::raw::*;
use crate::*;

/// Layer for overriding an operator's effective capability.
///
/// This layer updates [`Capability`] exposed by
/// [`OperatorInfo::capability`][crate::OperatorInfo::capability]
/// without changing the service's native capability. It is useful when the
/// backend implementation supports a capability, but the specific endpoint or
/// test setup needs to disable or tune it.
///
/// # Examples
///
/// ```no_run
/// use opendal_core::layers::CapabilityOverrideLayer;
/// use opendal_core::services;
/// use opendal_core::Operator;
/// use opendal_core::Result;
///
/// # fn main() -> Result<()> {
/// let op = Operator::new(services::Memory::default())?
///     .layer(CapabilityOverrideLayer::new(|mut cap| {
///         cap.write_with_if_match = false;
///         cap.delete_max_size = Some(700);
///         cap
///     }));
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CapabilityOverrideLayer {
    apply: Arc<dyn Fn(Capability) -> Capability + Send + Sync>,
}

impl fmt::Debug for CapabilityOverrideLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilityOverrideLayer")
            .finish_non_exhaustive()
    }
}

impl CapabilityOverrideLayer {
    /// Create a new [`CapabilityOverrideLayer`].
    pub fn new(apply: impl Fn(Capability) -> Capability + Send + Sync + 'static) -> Self {
        Self {
            apply: Arc::new(apply),
        }
    }

    /// Create a new [`CapabilityOverrideLayer`] from capability override entries.
    ///
    /// The input is a comma-separated list of capability assignments:
    ///
    /// - `read=true` sets a boolean capability to `true`
    /// - `read=false` sets a boolean capability to `false`
    /// - `delete_max_size=1000` sets a numeric capability
    /// - `delete_max_size=none` unsets an optional capability
    pub fn from_overrides(input: &str) -> Result<Self> {
        let overrides = CapabilityOverrides::parse(input)?;
        Ok(Self::new(move |cap| overrides.apply(cap)))
    }
}

impl Layer for CapabilityOverrideLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(CapabilityOverrideService {
            inner,
            apply: self.apply.clone(),
        })
    }
}

struct CapabilityOverrideService {
    inner: Servicer,
    apply: Arc<dyn Fn(Capability) -> Capability + Send + Sync>,
}

impl fmt::Debug for CapabilityOverrideService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilityOverrideService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl Service for CapabilityOverrideService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        (self.apply)(self.inner.capability())
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args)
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

#[derive(Clone, Debug, Default)]
struct CapabilityOverrides {
    values: Map<String, Value>,
}

impl CapabilityOverrides {
    fn parse(input: &str) -> Result<Self> {
        let mut overrides = Self::default();

        for token in input.split(',').map(str::trim).filter(|v| !v.is_empty()) {
            let (name, value) = parse_capability_override(token)?;
            overrides.values.insert(name.to_string(), value);
            overrides.try_apply(Capability::default()).map_err(|err| {
                invalid_capability_override(token, &format!("failed to apply override: {err}"))
            })?;
        }

        Ok(overrides)
    }

    fn apply(&self, cap: Capability) -> Capability {
        self.try_apply(cap)
            .expect("capability overrides must be validated before applying")
    }

    fn try_apply(&self, cap: Capability) -> Result<Capability> {
        let mut value = serde_json::to_value(cap).map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                format!("failed to serialize capability: {err}"),
            )
        })?;
        let object = value.as_object_mut().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "serialized capability must be a JSON object",
            )
        })?;
        object.extend(self.values.clone());

        serde_json::from_value(value).map_err(|err| {
            Error::new(
                ErrorKind::ConfigInvalid,
                format!("failed to deserialize capability overrides: {err}"),
            )
        })
    }
}

fn parse_capability_override(token: &str) -> Result<(&str, Value)> {
    let Some((name, value)) = token.split_once('=') else {
        return Err(invalid_capability_override(
            token,
            "expected `capability=value`",
        ));
    };

    Ok((
        name.trim(),
        parse_capability_value(value.trim())
            .map_err(|err| invalid_capability_override(token, &err.to_string()))?,
    ))
}

fn invalid_capability_override(token: &str, reason: &str) -> Error {
    Error::new(
        ErrorKind::ConfigInvalid,
        format!("invalid capability override entry `{token}`: {reason}"),
    )
}

fn parse_capability_value(value: &str) -> Result<Value> {
    match value {
        "true" | "on" | "yes" => Ok(Value::Bool(true)),
        "false" | "off" | "no" => Ok(Value::Bool(false)),
        "none" | "null" | "unset" => Ok(Value::Null),
        _ => value
            .parse::<usize>()
            .map(|v| Value::Number(Number::from(v)))
            .map_err(|_| {
                Error::new(
                    ErrorKind::ConfigInvalid,
                    "expected a boolean, non-negative integer, or `none`",
                )
            }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::Operator;
    use crate::services;

    /// Counts copy requests; copy metadata support is set by `metadata`.
    #[derive(Debug, Default)]
    struct CopyCounter {
        metadata: bool,
        copies: Arc<AtomicUsize>,
    }

    impl Builder for CopyCounter {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            Ok(self)
        }
    }

    impl Service for CopyCounter {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type Copier = CopyDone;
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("copy-counter")
        }

        fn capability(&self) -> Capability {
            Capability {
                copy: true,
                copy_with_user_metadata: self.metadata,
                copy_with_content_type: self.metadata,
                copy_with_content_language: self.metadata,
                ..Default::default()
            }
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            ().create_dir(ctx, path, args).await
        }

        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            ().stat(ctx, path, args).await
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<()> {
            ().read(ctx, path, args)
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<()> {
            ().write(ctx, path, args)
        }

        fn delete(&self, ctx: &OperationContext) -> Result<()> {
            ().delete(ctx)
        }

        fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<()> {
            ().list(ctx, path, args)
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<CopyDone> {
            self.copies.fetch_add(1, Ordering::SeqCst);
            Ok(CopyDone)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            ().rename(ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            ().presign(ctx, path, args).await
        }
    }

    struct CopyDone;

    impl oio::Copy for CopyDone {
        async fn next(&mut self) -> Result<Option<usize>> {
            Ok(None)
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::file(0).build())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    async fn copy_with_metadata(op: &Operator) -> Vec<Result<Metadata>> {
        vec![
            op.copy_with("from", "to")
                .user_metadata(HashMap::from([("mtime".to_owned(), "42".to_owned())]))
                .await,
            op.copy_with("from", "to").content_type("text/plain").await,
            op.copy_with("from", "to").content_language("en").await,
        ]
    }

    #[tokio::test]
    async fn capability_override_gates_copy_metadata() -> Result<()> {
        let copies = Arc::new(AtomicUsize::new(0));
        let op = Operator::new(CopyCounter {
            metadata: true,
            copies: copies.clone(),
        })?
        .layer(CapabilityOverrideLayer::new(|mut cap| {
            cap.copy_with_user_metadata = false;
            cap.copy_with_content_type = false;
            cap.copy_with_content_language = false;
            cap
        }));

        for res in copy_with_metadata(&op).await {
            assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);
        }
        assert_eq!(copies.load(Ordering::SeqCst), 0);

        let op = Operator::new(CopyCounter {
            metadata: false,
            copies: copies.clone(),
        })?
        .layer(CapabilityOverrideLayer::from_overrides(
            "copy_with_user_metadata=true,copy_with_content_type=true,copy_with_content_language=true",
        )?);

        for res in copy_with_metadata(&op).await {
            res?;
        }
        assert_eq!(copies.load(Ordering::SeqCst), 3);

        Ok(())
    }

    #[test]
    fn capability_override_updates_capability() -> Result<()> {
        let op = Operator::new(services::Memory::default())?.layer(CapabilityOverrideLayer::new(
            |mut cap| {
                cap.read = false;
                cap.delete_max_size = Some(7);
                cap
            },
        ));

        assert!(!op.info().capability().read);
        assert_eq!(op.info().capability().delete_max_size, Some(7));

        Ok(())
    }

    #[test]
    fn parse_capability_overrides() -> Result<()> {
        let layer = CapabilityOverrideLayer::from_overrides(
            "read=false,write_can_append=true,delete_max_size=7",
        )?;
        let op = Operator::new(services::Memory::default())?.layer(layer);

        assert!(!op.info().capability().read);
        assert!(op.info().capability().write_can_append);
        assert_eq!(op.info().capability().delete_max_size, Some(7));

        Ok(())
    }

    #[test]
    fn parse_bool_assignments_and_unset_sizes() -> Result<()> {
        let layer =
            CapabilityOverrideLayer::from_overrides("read=false,write=true,delete_max_size=none")?;
        let op = Operator::new(services::Memory::default())?.layer(layer);

        assert!(!op.info().capability().read);
        assert!(op.info().capability().write);
        assert_eq!(op.info().capability().delete_max_size, None);

        Ok(())
    }

    #[test]
    fn reject_unknown_capability() {
        let err = CapabilityOverrideLayer::from_overrides("not_a_capability=false").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn reject_capability_shorthand() {
        let err = CapabilityOverrideLayer::from_overrides("-read").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[test]
    fn reject_invalid_capability_type() {
        let err = CapabilityOverrideLayer::from_overrides("read=1").unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
    }

    #[derive(Debug, Default)]
    struct CountingService {
        capability: Capability,
        calls: Arc<AtomicUsize>,
    }

    impl Builder for CountingService {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            Ok(self)
        }
    }

    impl Service for CountingService {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type Deleter = CountingDeleter;
        type Copier = CopyDone;
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("counting")
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
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RpCreateDir::default())
        }

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RpStat::new(MetadataBuilder::file(0).build()))
        }

        fn read(&self, _: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn write(&self, _: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
            Ok(CountingDeleter {
                calls: self.calls.clone(),
            })
        }

        fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(CopyDone)
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RpRename::default())
        }

        async fn restore(&self, _: &OperationContext, _: &str, _: OpRestore) -> Result<RpRestore> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RpRestore::default())
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RpPresign::new(PresignedRequest::new(
                http::Method::GET,
                "https://example.com/object".parse().unwrap(),
                http::HeaderMap::new(),
            )))
        }
    }

    struct CountingDeleter {
        calls: Arc<AtomicUsize>,
    }

    impl oio::Delete for CountingDeleter {
        async fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn capability_override_enables_options_on_default_operator_stack() -> Result<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        // Underlying service reports no conditional/versioned capabilities.
        let base_cap = Capability {
            stat: true,
            read: true,
            write: true,
            delete: true,
            list: true,
            copy: true,
            rename: true,
            presign_stat: true,
            presign_read: true,
            presign_write: true,
            presign_delete: true,
            ..Default::default()
        };

        // Without override, every unsupported option fails before reaching the service.
        let base = Operator::new(CountingService {
            capability: base_cap,
            calls: calls.clone(),
        })?;
        assert_eq!(
            base.stat_with("a")
                .if_match("etag")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.reader_with("a")
                .version("v1")
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.writer_with("a")
                .if_match("etag")
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.delete_with("a")
                .version("v1")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.list_with("a/")
                .start_after("a/0")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.copy_with("a", "b")
                .if_not_exists(true)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.rename_with("a", "b")
                .if_not_exists(true)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.restore_with("a")
                .version("v1")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.presign_stat_with("a", Duration::from_secs(60))
                .if_match("etag")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            base.presign_read_options(
                "a",
                Duration::from_secs(60),
                options::ReadOptions {
                    version: Some("v1".to_string()),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err()
            .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        // Overriding capabilities to `=true` lets all those operations reach the service
        // because no bottom `CorrectnessCheckLayer` rejects them against native capability.
        let enabled = base.layer(CapabilityOverrideLayer::from_overrides(
            "stat_with_if_match=true,\
             read_with_version=true,\
             write_with_if_match=true,\
             delete_with_version=true,\
             list_with_start_after=true,\
             copy_with_if_not_exists=true,\
             rename_with_if_not_exists=true,\
             restore=true,\
             restore_with_version=true",
        )?);

        enabled.stat_with("a").if_match("etag").await?;
        let _ = enabled.reader_with("a").version("v1").await?;
        let _ = enabled.writer_with("a").if_match("etag").await?;
        enabled.delete_with("a").version("v1").await?;
        let _ = enabled.lister_with("a/").start_after("a/0").await?;
        enabled.copy_with("a", "b").if_not_exists(true).await?;
        enabled.rename_with("a", "b").if_not_exists(true).await?;
        enabled.restore_with("a").version("v1").await?;
        let _ = enabled
            .presign_stat_with("a", Duration::from_secs(60))
            .if_match("etag")
            .await?;
        let _ = enabled
            .presign_read_options(
                "a",
                Duration::from_secs(60),
                options::ReadOptions {
                    version: Some("v1".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(calls.load(Ordering::SeqCst), 10);

        // Re-restricting capabilities to `=false` blocks them again before reaching the service.
        let restricted = enabled.layer(CapabilityOverrideLayer::from_overrides(
            "stat_with_if_match=false,\
             read_with_version=false,\
             write_with_if_match=false,\
             delete_with_version=false,\
             list_with_start_after=false,\
             copy_with_if_not_exists=false,\
             rename_with_if_not_exists=false,\
             restore_with_version=false",
        )?);
        let before = calls.load(Ordering::SeqCst);
        assert_eq!(
            restricted
                .stat_with("a")
                .if_match("etag")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .reader_with("a")
                .version("v1")
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .writer_with("a")
                .if_match("etag")
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .delete_with("a")
                .version("v1")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .lister_with("a/")
                .start_after("a/0")
                .await
                .err()
                .unwrap()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .copy_with("a", "b")
                .if_not_exists(true)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .rename_with("a", "b")
                .if_not_exists(true)
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .restore_with("a")
                .version("v1")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .presign_stat_with("a", Duration::from_secs(60))
                .if_match("etag")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(
            restricted
                .presign_read_options(
                    "a",
                    Duration::from_secs(60),
                    options::ReadOptions {
                        version: Some("v1".to_string()),
                        ..Default::default()
                    },
                )
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::Unsupported
        );
        assert_eq!(calls.load(Ordering::SeqCst), before);

        Ok(())
    }
}
