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

use std::fmt::Formatter;
use std::sync::Arc;

use crate::raw::*;
use crate::*;

/// Add a correctness capability check layer for every operation
///
/// Before performing any operations, we will first verify the operation and its critical arguments
/// against the capability of the underlying service. If the operation or arguments is not supported,
/// an error will be returned directly.
///
/// # Notes
///
/// OpenDAL applies this checker to every service by default, so users don't need to invoke it manually.
/// this checker ensures the operation and its critical arguments, which might affect the correctness of
/// the call, are supported by the underlying service.
///
/// for example, when calling `write_with_append`, but `append` is not supported by the underlying
/// service, an `Unsupported` error is returned. without this check, undesired data may be written.
#[derive(Default)]
pub struct CorrectnessCheckLayer;

impl std::fmt::Debug for CorrectnessCheckLayer {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrectnessCheckLayer").finish()
    }
}

impl Layer for CorrectnessCheckLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(self.layer(inner))
    }
}

impl CorrectnessCheckLayer {
    fn layer(&self, inner: Servicer) -> CorrectnessService {
        CorrectnessService { inner }
    }
}

pub struct CorrectnessService {
    inner: Servicer,
}

pub(crate) fn new_unsupported_error(scheme: &'static str, op: Operation, args: &str) -> Error {
    let op = op.into_static();

    Error::new(
        ErrorKind::Unsupported,
        format!("The service {scheme} does not support the operation {op} with the arguments {args}. Please verify if the relevant flags have been enabled, or submit an issue if you believe this is incorrect."),
    )
    .with_operation(op)
}

enum ResolvedIfNotChanged {
    Version(String),
    Etag(String),
}

fn resolve_if_not_changed(
    scheme: &'static str,
    operation: Operation,
    identity: IfNotChanged,
    supports_version: bool,
    supports_etag: bool,
    explicit_version: Option<&str>,
    explicit_etag: Option<&str>,
) -> Result<ResolvedIfNotChanged> {
    if supports_version && let Some(version) = identity.version() {
        if let Some(explicit) = explicit_version
            && explicit != version
        {
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "if_not_changed conflicts with if_version_match",
            )
            .with_operation(operation.into_static()));
        }
        return Ok(ResolvedIfNotChanged::Version(version.to_string()));
    }

    if supports_etag && let Some(etag) = identity.etag() {
        if let Some(explicit) = explicit_etag
            && explicit != etag
        {
            return Err(Error::new(
                ErrorKind::ConditionNotMatch,
                "if_not_changed conflicts with if_match",
            )
            .with_operation(operation.into_static()));
        }
        return Ok(ResolvedIfNotChanged::Etag(etag.to_string()));
    }

    if !supports_version && !supports_etag {
        return Err(new_unsupported_error(scheme, operation, "if_not_changed"));
    }

    Err(Error::new(
        ErrorKind::ConfigInvalid,
        format!("if_not_changed metadata has no identity supported by {operation}"),
    )
    .with_operation(operation.into_static()))
}

fn check_delete_args(
    scheme: &'static str,
    capability: Capability,
    args: &mut OpDelete,
) -> Result<()> {
    if let Some(identity) = args.take_if_not_changed() {
        match resolve_if_not_changed(
            scheme,
            Operation::Delete,
            identity,
            capability.delete_with_if_version_match,
            capability.delete_with_if_match,
            args.if_version_match(),
            args.if_match(),
        )? {
            ResolvedIfNotChanged::Version(version) => {
                *args = std::mem::take(args).with_if_version_match(version);
            }
            ResolvedIfNotChanged::Etag(etag) => {
                *args = std::mem::take(args).with_if_match(etag);
            }
        }
    }

    if args.version().is_some() && !capability.delete_with_version {
        return Err(new_unsupported_error(scheme, Operation::Delete, "version"));
    }
    if args.recursive() && !capability.delete_with_recursive {
        return Err(new_unsupported_error(
            scheme,
            Operation::Delete,
            "recursive",
        ));
    }
    if args.if_match().is_some() && !capability.delete_with_if_match {
        return Err(new_unsupported_error(scheme, Operation::Delete, "if_match"));
    }
    if args.if_none_match().is_some() && !capability.delete_with_if_none_match {
        return Err(new_unsupported_error(
            scheme,
            Operation::Delete,
            "if_none_match",
        ));
    }
    if args.if_version_match().is_some() && !capability.delete_with_if_version_match {
        return Err(new_unsupported_error(
            scheme,
            Operation::Delete,
            "if_version_match",
        ));
    }
    if args.if_version_not_match().is_some() && !capability.delete_with_if_version_not_match {
        return Err(new_unsupported_error(
            scheme,
            Operation::Delete,
            "if_version_not_match",
        ));
    }

    Ok(())
}

impl std::fmt::Debug for CorrectnessService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrectnessCheckService")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl CorrectnessService {
    fn check_write_args(&self, mut args: OpWrite) -> Result<OpWrite> {
        let capability = self.capability();
        let scheme = self.info().scheme();
        if let Some(identity) = args.take_if_not_changed() {
            match resolve_if_not_changed(
                scheme,
                Operation::Write,
                identity,
                capability.write_with_if_version_match,
                capability.write_with_if_match,
                args.if_version_match(),
                args.if_match(),
            )? {
                ResolvedIfNotChanged::Version(version) => {
                    args = args.with_if_version_match(&version);
                }
                ResolvedIfNotChanged::Etag(etag) => {
                    args = args.with_if_match(&etag);
                }
            }
        }
        if args.append() && !capability.write_can_append {
            return Err(new_unsupported_error(scheme, Operation::Write, "append"));
        }
        if args.if_not_exists() && !capability.write_with_if_not_exists {
            return Err(new_unsupported_error(
                scheme,
                Operation::Write,
                "if_not_exists",
            ));
        }
        if args.if_match().is_some() && !capability.write_with_if_match {
            return Err(new_unsupported_error(scheme, Operation::Write, "if_match"));
        }
        if let Some(if_none_match) = args.if_none_match()
            && !capability.write_with_if_none_match
        {
            let mut err = new_unsupported_error(scheme, Operation::Write, "if_none_match");
            if if_none_match == "*" && capability.write_with_if_not_exists {
                err = err.with_context("hint", "use if_not_exists instead");
            }
            return Err(err);
        }
        if args.if_version_match().is_some() && !capability.write_with_if_version_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Write,
                "if_version_match",
            ));
        }
        if args.if_version_not_match().is_some() && !capability.write_with_if_version_not_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Write,
                "if_version_not_match",
            ));
        }

        Ok(args)
    }

    fn check_copy_args(&self, mut args: OpCopy) -> Result<OpCopy> {
        let capability = self.capability();
        let scheme = self.info().scheme();
        if let Some(identity) = args.take_if_not_changed() {
            match resolve_if_not_changed(
                scheme,
                Operation::Copy,
                identity,
                capability.copy_with_if_version_match,
                capability.copy_with_if_match,
                args.if_version_match(),
                args.if_match(),
            )? {
                ResolvedIfNotChanged::Version(version) => {
                    args = args.with_if_version_match(version);
                }
                ResolvedIfNotChanged::Etag(etag) => {
                    args = args.with_if_match(etag);
                }
            }
        }
        if args.if_not_exists() && !capability.copy_with_if_not_exists {
            return Err(new_unsupported_error(
                scheme,
                Operation::Copy,
                "if_not_exists",
            ));
        }
        if args.if_match().is_some() && !capability.copy_with_if_match {
            return Err(new_unsupported_error(scheme, Operation::Copy, "if_match"));
        }
        if args.if_none_match().is_some() && !capability.copy_with_if_none_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Copy,
                "if_none_match",
            ));
        }
        if args.if_version_match().is_some() && !capability.copy_with_if_version_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Copy,
                "if_version_match",
            ));
        }
        if args.if_version_not_match().is_some() && !capability.copy_with_if_version_not_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Copy,
                "if_version_not_match",
            ));
        }
        if args.source_version().is_some() && !capability.copy_with_source_version {
            return Err(new_unsupported_error(
                scheme,
                Operation::Copy,
                "source_version",
            ));
        }

        Ok(args)
    }
}

impl Service for CorrectnessService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = CheckWrapper<oio::Deleter>;
    type Copier = oio::Copier;
    type Composer = CheckWrapper<oio::Composer>;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        self.inner.capability()
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let capability = self.capability();
        let scheme = self.info().scheme();
        if !capability.read_with_version && args.version().is_some() {
            return Err(new_unsupported_error(scheme, Operation::Read, "version"));
        }
        if !capability.read_with_if_match && args.if_match().is_some() {
            return Err(new_unsupported_error(scheme, Operation::Read, "if_match"));
        }
        if !capability.read_with_if_none_match && args.if_none_match().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Read,
                "if_none_match",
            ));
        }
        if !capability.read_with_if_version_match && args.if_version_match().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Read,
                "if_version_match",
            ));
        }
        if !capability.read_with_if_version_not_match && args.if_version_not_match().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Read,
                "if_version_not_match",
            ));
        }
        if !capability.read_with_if_modified_since && args.if_modified_since().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Read,
                "if_modified_since",
            ));
        }
        if !capability.read_with_if_unmodified_since && args.if_unmodified_since().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Read,
                "if_unmodified_since",
            ));
        }

        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let args = self.check_write_args(args)?;
        self.inner.write(ctx, path, args)
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        let capability = self.capability();
        let scheme = self.info().scheme();
        if !capability.stat_with_version && args.version().is_some() {
            return Err(new_unsupported_error(scheme, Operation::Stat, "version"));
        }
        if !capability.stat_with_if_match && args.if_match().is_some() {
            return Err(new_unsupported_error(scheme, Operation::Stat, "if_match"));
        }
        if !capability.stat_with_if_none_match && args.if_none_match().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Stat,
                "if_none_match",
            ));
        }
        if !capability.stat_with_if_version_match && args.if_version_match().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Stat,
                "if_version_match",
            ));
        }
        if !capability.stat_with_if_version_not_match && args.if_version_not_match().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Stat,
                "if_version_not_match",
            ));
        }
        if !capability.stat_with_if_modified_since && args.if_modified_since().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Stat,
                "if_modified_since",
            ));
        }
        if !capability.stat_with_if_unmodified_since && args.if_unmodified_since().is_some() {
            return Err(new_unsupported_error(
                scheme,
                Operation::Stat,
                "if_unmodified_since",
            ));
        }

        self.inner.stat(ctx, path, args).await
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner
            .delete(ctx)
            .map(|deleter| CheckWrapper::new(deleter, self.info().scheme(), self.capability()))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        let args = self.check_copy_args(args)?;
        self.inner.copy(ctx, from, to, args)
    }

    fn compose(
        &self,
        ctx: &OperationContext,
        to: &str,
        mut args: OpCompose,
    ) -> Result<Self::Composer> {
        let capability = self.capability();
        let scheme = self.info().scheme();
        if !capability.compose {
            return Err(new_unsupported_error(scheme, Operation::Compose, ""));
        }
        if let Some(identity) = args.take_if_not_changed() {
            match resolve_if_not_changed(
                scheme,
                Operation::Compose,
                identity,
                capability.compose_with_if_version_match,
                capability.compose_with_if_match,
                args.if_version_match(),
                args.if_match(),
            )? {
                ResolvedIfNotChanged::Version(version) => {
                    args = args.with_if_version_match(version);
                }
                ResolvedIfNotChanged::Etag(etag) => {
                    args = args.with_if_match(etag);
                }
            }
        }
        if args.if_not_exists() && !capability.compose_with_if_not_exists {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "if_not_exists",
            ));
        }
        if args.if_match().is_some() && !capability.compose_with_if_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "if_match",
            ));
        }
        if args.if_none_match().is_some() && !capability.compose_with_if_none_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "if_none_match",
            ));
        }
        if args.if_version_match().is_some() && !capability.compose_with_if_version_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "if_version_match",
            ));
        }
        if args.if_version_not_match().is_some() && !capability.compose_with_if_version_not_match {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "if_version_not_match",
            ));
        }
        if args.content_type().is_some() && !capability.compose_with_content_type {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "content_type",
            ));
        }
        if args.content_disposition().is_some() && !capability.compose_with_content_disposition {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "content_disposition",
            ));
        }
        if args.content_encoding().is_some() && !capability.compose_with_content_encoding {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "content_encoding",
            ));
        }
        if args.cache_control().is_some() && !capability.compose_with_cache_control {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "cache_control",
            ));
        }
        if args.user_metadata().is_some() && !capability.compose_with_user_metadata {
            return Err(new_unsupported_error(
                scheme,
                Operation::Compose,
                "user_metadata",
            ));
        }

        self.inner
            .compose(ctx, to, args)
            .map(|composer| CheckWrapper::new(composer, scheme, capability))
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        let capability = self.capability();
        let scheme = self.info().scheme();
        if args.if_not_exists() && !capability.rename_with_if_not_exists {
            return Err(new_unsupported_error(
                scheme,
                Operation::Rename,
                "if_not_exists",
            ));
        }

        self.inner.rename(ctx, from, to, args).await
    }

    async fn restore(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRestore,
    ) -> Result<RpRestore> {
        let capability = self.capability();
        let scheme = self.info().scheme();
        if !capability.restore {
            return Err(new_unsupported_error(scheme, Operation::Restore, ""));
        }
        if args.version().is_some() && !capability.restore_with_version {
            return Err(new_unsupported_error(scheme, Operation::Restore, "version"));
        }
        if args.if_not_exists() && !capability.restore_with_if_not_exists {
            return Err(new_unsupported_error(
                scheme,
                Operation::Restore,
                "if_not_exists",
            ));
        }
        if args.if_not_exists() && args.version().is_none() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "if_not_exists requires a restore version",
            )
            .with_operation(Operation::Restore));
        }

        self.inner.restore(ctx, path, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        let (expire, operation) = args.into_parts();
        let operation = match operation {
            PresignOperation::Write(args) => PresignOperation::Write(self.check_write_args(args)?),
            PresignOperation::Delete(mut args) => {
                check_delete_args(self.info().scheme(), self.capability(), &mut args)?;
                PresignOperation::Delete(args)
            }
            operation => operation,
        };

        self.inner
            .presign(ctx, path, OpPresign::new(operation, expire))
            .await
    }
}

pub struct CheckWrapper<T> {
    scheme: &'static str,
    capability: Capability,
    inner: T,
}

impl<T> CheckWrapper<T> {
    fn new(inner: T, scheme: &'static str, capability: Capability) -> Self {
        Self {
            inner,
            scheme,
            capability,
        }
    }

    fn check_delete(&self, args: &mut OpDelete) -> Result<()> {
        check_delete_args(self.scheme, self.capability, args)
    }

    fn check_compose_source(&self, args: &mut OpRead) -> Result<()> {
        if let Some(metadata) = args.take_if_not_changed() {
            if self.capability.compose_with_source_version
                && let Some(version) = metadata.version()
            {
                if let Some(explicit) = args.version() {
                    if explicit != version {
                        return Err(Error::new(
                            ErrorKind::ConditionNotMatch,
                            "if_not_changed conflicts with source version",
                        )
                        .with_operation(Operation::Compose.into_static()));
                    }
                } else {
                    args.set_version(version);
                }
            } else if self.capability.compose_with_source_if_match
                && let Some(etag) = metadata.etag()
            {
                if let Some(explicit) = args.if_match() {
                    if explicit != etag {
                        return Err(Error::new(
                            ErrorKind::ConditionNotMatch,
                            "if_not_changed conflicts with source if_match",
                        )
                        .with_operation(Operation::Compose.into_static()));
                    }
                } else {
                    args.set_if_match(etag);
                }
            } else if !self.capability.compose_with_source_version
                && !self.capability.compose_with_source_if_match
            {
                return Err(new_unsupported_error(
                    self.scheme,
                    Operation::Compose,
                    "source_if_not_changed",
                ));
            } else {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "if_not_changed metadata has no identity supported by compose",
                )
                .with_operation(Operation::Compose.into_static()));
            }
        }

        if args.version().is_some() && !self.capability.compose_with_source_version {
            return Err(new_unsupported_error(
                self.scheme,
                Operation::Compose,
                "source_version",
            ));
        }

        if args.if_match().is_some() && !self.capability.compose_with_source_if_match {
            return Err(new_unsupported_error(
                self.scheme,
                Operation::Compose,
                "source_if_match",
            ));
        }

        if args.if_none_match().is_some() || args.if_version_not_match().is_some() {
            return Err(new_unsupported_error(
                self.scheme,
                Operation::Compose,
                "source_identity",
            ));
        }

        Ok(())
    }
}

impl<T: oio::Delete> oio::Delete for CheckWrapper<T> {
    async fn delete(&mut self, path: &str, mut args: OpDelete) -> Result<()> {
        self.check_delete(&mut args)?;
        self.inner.delete(path, args).await
    }

    async fn close(&mut self) -> Result<()> {
        self.inner.close().await
    }
}

impl<T: oio::Compose> oio::Compose for CheckWrapper<T> {
    async fn compose(&mut self, path: &str, mut args: OpRead) -> Result<()> {
        self.check_compose_source(&mut args)?;
        self.inner.compose(path, args).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Capability;
    use crate::ComposeInput;
    use crate::EntryMode;
    use crate::Metadata;
    use crate::Operator;
    use crate::raw::oio;

    #[derive(Debug)]
    struct MockService {
        capability: Capability,
        expected_write_if_match: Option<String>,
        expected_copy_if_match: Option<String>,
        expected_delete_if_match: Option<String>,
        expected_presign_write_if_match: Option<String>,
        expected_presign_delete_if_match: Option<String>,
        expected_compose_if_match: Option<String>,
        expected_compose_source_version: Option<String>,
    }

    impl Service for MockService {
        type Reader = MockReader;
        type Writer = MockWriter;
        type Lister = ();
        type Deleter = MockDeleter;
        type Copier = ();
        type Composer = MockComposer;

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("memory")
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

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            Ok(RpStat::new(Metadata::new(EntryMode::Unknown)))
        }

        fn read(&self, _ctx: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            Ok(MockReader)
        }

        fn write(&self, _ctx: &OperationContext, _: &str, args: OpWrite) -> Result<Self::Writer> {
            if let Some(expected) = self.expected_write_if_match.as_deref() {
                assert_eq!(args.if_match(), Some(expected));
            }
            Ok(MockWriter)
        }

        fn list(&self, _ctx: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Ok(())
        }

        fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
            Ok(MockDeleter {
                expected_if_match: self.expected_delete_if_match.clone(),
            })
        }

        fn copy(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            if let Some(expected) = self.expected_copy_if_match.as_deref() {
                assert_eq!(args.if_match(), Some(expected));
            }
            Ok(())
        }

        fn compose(
            &self,
            _: &OperationContext,
            _: &str,
            args: OpCompose,
        ) -> Result<Self::Composer> {
            if let Some(expected) = self.expected_compose_if_match.as_deref() {
                assert_eq!(args.if_match(), Some(expected));
            }
            Ok(MockComposer {
                expected_source_version: self.expected_compose_source_version.clone(),
            })
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            Ok(RpRename::default())
        }

        async fn presign(
            &self,
            _: &OperationContext,
            _: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            match args.into_parts().1 {
                PresignOperation::Write(args) => {
                    if let Some(expected) = self.expected_presign_write_if_match.as_deref() {
                        assert_eq!(args.if_match(), Some(expected));
                    }
                }
                PresignOperation::Delete(args) => {
                    if let Some(expected) = self.expected_presign_delete_if_match.as_deref() {
                        assert_eq!(args.if_match(), Some(expected));
                    }
                }
                _ => {}
            }
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }
    }

    struct MockReader;

    impl oio::Read for MockReader {
        async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            Ok((
                RpRead::new(Metadata::new(EntryMode::FILE).with_content_length(0)),
                Box::new(Buffer::new()) as Box<dyn oio::ReadStreamDyn>,
            ))
        }

        async fn read(&self, _: BytesRange) -> Result<(RpRead, Buffer)> {
            Ok((
                RpRead::new(Metadata::new(EntryMode::FILE).with_content_length(0)),
                Buffer::new(),
            ))
        }
    }

    struct MockWriter;

    impl oio::Write for MockWriter {
        async fn write(&mut self, _: Buffer) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(Metadata::default())
        }

        async fn abort(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct MockDeleter {
        expected_if_match: Option<String>,
    }

    impl oio::Delete for MockDeleter {
        async fn delete(&mut self, _: &str, args: OpDelete) -> Result<()> {
            if let Some(expected) = self.expected_if_match.as_deref() {
                assert_eq!(args.if_match(), Some(expected));
            }
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct MockComposer {
        expected_source_version: Option<String>,
    }

    impl oio::Compose for MockComposer {
        async fn compose(&mut self, _: &str, args: OpRead) -> Result<()> {
            if let Some(expected) = self.expected_source_version.as_deref() {
                assert_eq!(args.version(), Some(expected));
            }
            Ok(())
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(Metadata::new(EntryMode::FILE).with_content_length(0))
        }
    }

    fn new_test_operator(capability: Capability) -> Operator {
        let srv = MockService {
            capability,
            expected_write_if_match: None,
            expected_copy_if_match: None,
            expected_delete_if_match: None,
            expected_presign_write_if_match: None,
            expected_presign_delete_if_match: None,
            expected_compose_if_match: None,
            expected_compose_source_version: None,
        };

        Operator::from_parts(OperationContext::default(), Arc::new(srv))
            .layer(CorrectnessCheckLayer)
    }

    fn new_compose_test_operator(
        capability: Capability,
        expected_if_match: &str,
        expected_source_version: &str,
    ) -> Operator {
        let srv = MockService {
            capability,
            expected_write_if_match: None,
            expected_copy_if_match: None,
            expected_delete_if_match: None,
            expected_presign_write_if_match: None,
            expected_presign_delete_if_match: None,
            expected_compose_if_match: Some(expected_if_match.to_string()),
            expected_compose_source_version: Some(expected_source_version.to_string()),
        };

        Operator::from_parts(OperationContext::default(), Arc::new(srv))
            .layer(CorrectnessCheckLayer)
    }

    fn new_if_not_changed_test_operator(
        capability: Capability,
        expected_if_match: &str,
    ) -> Operator {
        let expected = || Some(expected_if_match.to_string());
        let srv = MockService {
            capability,
            expected_write_if_match: expected(),
            expected_copy_if_match: expected(),
            expected_delete_if_match: expected(),
            expected_presign_write_if_match: expected(),
            expected_presign_delete_if_match: expected(),
            expected_compose_if_match: None,
            expected_compose_source_version: None,
        };

        Operator::from_parts(OperationContext::default(), Arc::new(srv))
            .layer(CorrectnessCheckLayer)
    }

    #[tokio::test]
    async fn test_read() {
        let op = new_test_operator(Capability {
            read: true,
            ..Default::default()
        });
        let res = op.read_with("path").version("version").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            read: true,
            read_with_version: true,
            ..Default::default()
        });
        let res = op.read_with("path").version("version").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_stat() {
        let op = new_test_operator(Capability {
            stat: true,
            ..Default::default()
        });
        let res = op.stat_with("path").version("version").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            stat: true,
            stat_with_version: true,
            ..Default::default()
        });
        let res = op.stat_with("path").version("version").await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_write_with() {
        let op = new_test_operator(Capability {
            write: true,
            write_with_if_not_exists: true,
            ..Default::default()
        });
        let res = op.write_with("path", "".as_bytes()).append(true).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let res = op
            .write_with("path", "".as_bytes())
            .if_none_match("etag")
            .await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Unsupported (permanent) at write => The service memory does not support the operation write with the arguments if_none_match. Please verify if the relevant flags have been enabled, or submit an issue if you believe this is incorrect."
        );

        // Now try a wildcard if-none-match
        let res = op
            .write_with("path", "".as_bytes())
            .if_none_match("*")
            .await;
        assert!(res.is_err());
        assert_eq!(
            res.unwrap_err().to_string(),
            "Unsupported (permanent) at write, context: { hint: use if_not_exists instead } => The service memory does not support the operation write with the arguments if_none_match. Please verify if the relevant flags have been enabled, or submit an issue if you believe this is incorrect."
        );

        let res = op
            .write_with("path", "".as_bytes())
            .if_not_exists(true)
            .await;
        assert!(res.is_ok());

        let op = new_test_operator(Capability {
            write: true,
            write_can_append: true,
            write_with_if_not_exists: true,
            write_with_if_none_match: true,
            ..Default::default()
        });
        let res = op.writer_with("path").append(true).await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_delete() {
        let op = new_test_operator(Capability {
            delete: true,
            ..Default::default()
        });
        let res = op.delete_with("path").version("version").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            delete: true,
            delete_with_version: true,
            ..Default::default()
        });
        let res = op.delete_with("path").version("version").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_compose() {
        let op = new_test_operator(Capability {
            compose: true,
            ..Default::default()
        });

        let err = op
            .compose([ComposeInput::new("from").with_version("version")], "to")
            .await
            .expect_err("source version must require a capability");
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let err = op
            .compose_with(["from"], "to")
            .content_type("text/plain")
            .await
            .expect_err("destination content type must require a capability");
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let err = op
            .compose(Vec::<String>::new(), "to")
            .await
            .expect_err("empty composition must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let err = op
            .compose(["same"], "same")
            .await
            .expect_err("a destination cannot be its own source");
        assert_eq!(err.kind(), ErrorKind::IsSameFile);

        let op = new_test_operator(Capability {
            compose: true,
            compose_with_source_version: true,
            compose_with_content_type: true,
            ..Default::default()
        });
        op.compose_with([ComposeInput::new("from").with_version("version")], "to")
            .content_type("text/plain")
            .await
            .expect("supported compose options must be forwarded");
    }

    #[tokio::test]
    async fn test_version_preconditions_are_forwarded() -> Result<()> {
        let op = new_test_operator(Capability {
            stat: true,
            stat_with_version: true,
            stat_with_if_version_match: true,
            read: true,
            read_with_if_version_match: true,
            write: true,
            write_with_if_match: true,
            write_with_if_version_match: true,
            delete: true,
            delete_with_if_version_match: true,
            delete_with_if_version_not_match: true,
            copy: true,
            copy_with_if_version_match: true,
            ..Default::default()
        });

        op.stat_with("path").if_version_match("version").await?;
        op.read_with("path").if_version_match("version").await?;
        op.write_with("path", "")
            .if_version_match("version")
            .await?;
        op.delete_with("path").if_version_match("version").await?;
        op.copy_with("from", "to")
            .if_version_match("version")
            .await?;

        op.write_with("path", "")
            .if_match("etag")
            .if_version_match("version")
            .await?;

        op.stat_with("path")
            .version("selected")
            .if_version_match("current")
            .await?;

        op.delete_with("path")
            .if_version_match("matched")
            .if_version_not_match("not-matched")
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_if_not_changed_merges_at_dispatch() -> Result<()> {
        let op = new_test_operator(Capability {
            write: true,
            write_with_if_match: true,
            write_with_if_version_match: true,
            delete: true,
            delete_with_if_match: true,
            copy: true,
            copy_with_if_match: true,
            compose: true,
            compose_with_if_match: true,
            compose_with_source_version: true,
            ..Default::default()
        });
        let metadata = Metadata::default()
            .with_etag("etag".to_string())
            .with_version("version".to_string());

        op.write_with("path", "")
            .if_match("other-etag")
            .if_version_match("version")
            .if_not_changed(&metadata)
            .await?;

        let err = op
            .write_with("path", "")
            .if_version_match("other-version")
            .if_not_changed(&metadata)
            .await
            .expect_err("different selected version must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        op.copy_with("from", "to")
            .if_match("etag")
            .if_not_changed(&metadata)
            .await?;
        let err = op
            .copy_with("from", "to")
            .if_match("other-etag")
            .if_not_changed(&metadata)
            .await
            .expect_err("different selected etag must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        op.delete_with("path")
            .if_match("etag")
            .if_not_changed(&metadata)
            .await?;
        let err = op
            .delete_with("path")
            .if_match("other-etag")
            .if_not_changed(&metadata)
            .await
            .expect_err("different selected etag must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        op.compose_with(["from"], "to")
            .if_match("etag")
            .if_not_changed(&metadata)
            .await?;
        let err = op
            .compose_with(["from"], "to")
            .if_match("other-etag")
            .if_not_changed(&metadata)
            .await
            .expect_err("different destination etag must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        op.compose(
            [ComposeInput::new("from")
                .with_version("version")
                .with_if_not_changed(&metadata)],
            "to",
        )
        .await?;
        let err = op
            .compose(
                [ComposeInput::new("from")
                    .with_version("other-version")
                    .with_if_not_changed(&metadata)],
                "to",
            )
            .await
            .expect_err("different source version must fail");
        assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

        let op = new_if_not_changed_test_operator(
            Capability {
                write: true,
                write_with_if_match: true,
                copy: true,
                copy_with_if_match: true,
                delete: true,
                delete_with_if_match: true,
                ..Default::default()
            },
            "etag",
        );
        op.write_with("path", "").if_not_changed(&metadata).await?;
        op.copy_with("from", "to").if_not_changed(&metadata).await?;
        op.delete_with("path").if_not_changed(&metadata).await?;
        op.presign_write_options(
            "path",
            std::time::Duration::from_secs(60),
            options::WriteOptions {
                if_not_changed: Some(metadata.clone()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign after checking write args");
        op.presign_delete_options(
            "path",
            std::time::Duration::from_secs(60),
            options::DeleteOptions {
                if_not_changed: Some(metadata.clone()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mock service rejects presign after checking delete args");

        let op = new_compose_test_operator(
            Capability {
                compose: true,
                compose_with_if_match: true,
                compose_with_source_version: true,
                ..Default::default()
            },
            "etag",
            "version",
        );
        op.compose_with(
            [ComposeInput::new("from").with_if_not_changed(&metadata)],
            "to",
        )
        .if_not_changed(&metadata)
        .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_presign_conditions_reach_service() {
        let op = new_test_operator(Capability::default());

        let err = op
            .presign_stat_options(
                "path",
                std::time::Duration::from_secs(60),
                options::StatOptions {
                    if_version_match: Some("version".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("mock service rejects presign");
        assert!(err.to_string().contains("operation is not supported"));

        let err = op
            .presign_delete_options(
                "path",
                std::time::Duration::from_secs(60),
                options::DeleteOptions {
                    if_none_match: Some("etag".to_string()),
                    if_version_not_match: Some("version".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("unsupported delete conditions must fail before presign");
        assert_eq!(err.kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            write_with_if_match: true,
            ..Default::default()
        });
        let err = op
            .presign_write_options(
                "path",
                std::time::Duration::from_secs(60),
                options::WriteOptions {
                    if_not_changed: Some(Metadata::default().with_etag("etag".to_string())),
                    ..Default::default()
                },
            )
            .await
            .expect_err("mock service rejects presign");
        assert!(err.to_string().contains("operation is not supported"));
    }

    #[tokio::test]
    async fn test_rename_with_if_not_exists() {
        let op = new_test_operator(Capability {
            rename: true,
            ..Default::default()
        });
        let res = op.rename_with("from", "to").if_not_exists(true).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            rename: true,
            rename_with_if_not_exists: true,
            ..Default::default()
        });
        let res = op.rename_with("from", "to").if_not_exists(true).await;
        assert!(res.is_ok());
    }
}
