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

//! Capability check layer implementation for Apache OpenDAL.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::*;

/// `CapabilityCheckLayer` validates optional operation arguments against service capabilities.
///
/// Similar to `CorrectnessChecker`, this layer verifies selected optional arguments for write,
/// copy, and list operations against the capabilities of the underlying service. If an argument is
/// not supported, an error is returned directly.
///
/// # Notes
///
/// There are two main differences between this checker with the `CorrectnessChecker`:
/// 1. This checker provides additional checks for capabilities like write_with_content_type and
///    list_with_versions, among others. These capabilities do not affect data integrity, even if
///    the underlying storage services do not support them.
///
/// 2. OpenDAL doesn't apply this checker by default. Users can enable this layer if they want to
///    enforce stricter requirements.
///
/// # Examples
///
/// ```no_run
/// # use opendal_core::services;
/// # use opendal_core::Operator;
/// # use opendal_core::Result;
/// # use opendal_layer_capability_check::CapabilityCheckLayer;
/// #
/// # fn main() -> Result<()> {
/// let _ = Operator::new(services::Memory::default())?
///     .layer(CapabilityCheckLayer::new());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct CapabilityCheckLayer {}

impl CapabilityCheckLayer {
    /// Create a new [`CapabilityCheckLayer`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Layer for CapabilityCheckLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(self.layer(inner))
    }
}

impl CapabilityCheckLayer {
    fn layer(&self, inner: Servicer) -> CapabilityCheckService {
        CapabilityCheckService { inner }
    }
}

#[doc(hidden)]
#[derive(Debug)]
pub struct CapabilityCheckService {
    inner: Servicer,
}

fn new_unsupported_error(info: &ServiceInfo, op: Operation, args: &str) -> Error {
    let scheme = info.scheme();
    let op = op.into_static();

    Error::new(
        ErrorKind::Unsupported,
        format!("The service {scheme} does not support the operation {op} with the arguments {args}. Please verify if the relevant flags have been enabled, or submit an issue if you believe this is incorrect."),
    )
    .with_operation(op)
}

impl Service for CapabilityCheckService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
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
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let capability = self.capability();
        let info = self.info();
        if !capability.write_with_content_type && args.content_type().is_some() {
            return Err(new_unsupported_error(
                &info,
                Operation::Write,
                "content_type",
            ));
        }
        if !capability.write_with_cache_control && args.cache_control().is_some() {
            return Err(new_unsupported_error(
                &info,
                Operation::Write,
                "cache_control",
            ));
        }
        if !capability.write_with_content_disposition && args.content_disposition().is_some() {
            return Err(new_unsupported_error(
                &info,
                Operation::Write,
                "content_disposition",
            ));
        }

        self.inner.write(ctx, path, args)
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> Result<Self::Copier> {
        let capability = self.capability();
        let info = self.info();
        if args.if_not_exists() && !capability.copy_with_if_not_exists {
            return Err(new_unsupported_error(
                &info,
                Operation::Copy,
                "if_not_exists",
            ));
        }
        if args.if_match().is_some() && !capability.copy_with_if_match {
            return Err(new_unsupported_error(&info, Operation::Copy, "if_match"));
        }
        if args.source_version().is_some() && !capability.copy_with_source_version {
            return Err(new_unsupported_error(
                &info,
                Operation::Copy,
                "source_version",
            ));
        }

        self.inner.copy(ctx, from, to, args, opts)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let capability = self.capability();
        if !capability.list_with_versions && args.versions() {
            let info = self.info();
            return Err(new_unsupported_error(&info, Operation::List, "version"));
        }
        if !capability.list_with_glob && args.glob().is_some() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::List,
                "glob",
            ));
        }

        self.inner.list(ctx, path, args)
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

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MockService {
        capability: Capability,
    }

    impl Service for MockService {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type Copier = ();

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

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn read(&self, _ctx: &OperationContext, _: &str, _: OpRead) -> Result<Self::Reader> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn write(&self, _ctx: &OperationContext, _: &str, _: OpWrite) -> Result<Self::Writer> {
            Ok(())
        }

        fn list(&self, _ctx: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Ok(())
        }

        fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }

        fn copy(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpCopy,
            _: OpCopier,
        ) -> Result<Self::Copier> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
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

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            Err(Error::new(
                ErrorKind::Unsupported,
                "operation is not supported",
            ))
        }
    }

    fn new_test_operator(capability: Capability) -> Operator {
        let srv = MockService { capability };

        Operator::from_parts(OperationContext::default(), Arc::new(srv))
            .layer(CapabilityCheckLayer::new())
    }

    #[tokio::test]
    async fn test_writer_with() {
        let op = new_test_operator(Capability {
            write: true,
            ..Default::default()
        });
        let res = op.writer_with("path").content_type("type").await;
        assert!(res.is_err());

        let res = op.writer_with("path").cache_control("cache").await;
        assert!(res.is_err());

        let res = op
            .writer_with("path")
            .content_disposition("disposition")
            .await;
        assert!(res.is_err());

        let op = new_test_operator(Capability {
            write: true,
            write_with_content_type: true,
            write_with_cache_control: true,
            write_with_content_disposition: true,
            ..Default::default()
        });
        let res = op.writer_with("path").content_type("type").await;
        assert!(res.is_ok());

        let res = op.writer_with("path").cache_control("cache").await;
        assert!(res.is_ok());

        let res = op
            .writer_with("path")
            .content_disposition("disposition")
            .await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_list_with() {
        let op = new_test_operator(Capability {
            list: true,
            ..Default::default()
        });
        let res = op.list_with("path/").versions(true).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            list: true,
            list_with_versions: true,
            ..Default::default()
        });
        let res = op.lister_with("path/").versions(true).await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_list_with_glob() {
        let op = new_test_operator(Capability {
            list: true,
            ..Default::default()
        });
        let res = op.list_with("path/").glob("*.jpg").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            list: true,
            list_with_glob: true,
            ..Default::default()
        });
        let res = op.lister_with("path/").glob("*.jpg").await;
        assert!(res.is_ok())
    }
}
