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

use crate::layers::correctness_check::new_unsupported_error;
use crate::raw::*;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Add an extra capability check layer for every operation
///
/// Similar to `CorrectnessChecker`, Before performing any operations, this layer will first verify
/// its arguments against the capability of the underlying service. If the arguments is not supported,
/// an error will be returned directly.
///
/// Notes
///
/// There are two main differences between this checker with the `CorrectnessChecker`:
/// 1. This checker provides additional checks for capabilities like write_with_content_type and
///     list_with_versions, among others. These capabilities do not affect data integrity, even if
///     the underlying storage services do not support them.
///
/// 2. OpenDAL doesn't apply this checker by default. Users can enable this layer if they want to
///     enforce stricter requirements.
///
/// # examples
///
/// ```no_run
/// # use opendal::layers::CapabilityCheckLayer;
/// # use opendal::services;
/// # use opendal::Operator;
/// # use opendal::Result;
/// # use opendal::Scheme;
///
/// # fn main() -> Result<()> {
/// use opendal::layers::CapabilityCheckLayer;
/// let _ = Operator::new(services::Memory::default())?
///     .layer(CapabilityCheckLayer)
///     .finish();
/// Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct CapabilityCheckLayer;

impl<A: Access> Layer<A> for CapabilityCheckLayer {
    type LayeredAccess = CapabilityAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        let info = inner.info();

        CapabilityAccessor { info, inner }
    }
}
pub struct CapabilityAccessor<A: Access> {
    info: Arc<AccessorInfo>,
    inner: A,
}

impl<A: Access> Debug for CapabilityAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CapabilityCheckAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access> LayeredAccess for CapabilityAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type Writer = A::Writer;
    type Lister = A::Lister;
    type Deleter = A::Deleter;
    type BlockingReader = A::BlockingReader;
    type BlockingWriter = A::BlockingWriter;
    type BlockingLister = A::BlockingLister;
    type BlockingDeleter = A::BlockingDeleter;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
        let capability = self.info.full_capability();
        if !capability.write_with_content_type && args.content_type().is_some() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::Write,
                "content_type",
            ));
        }
        if !capability.write_with_cache_control && args.cache_control().is_some() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::Write,
                "cache_control",
            ));
        }
        if !capability.write_with_content_disposition && args.content_disposition().is_some() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::Write,
                "content_disposition",
            ));
        }

        self.inner.write(path, args).await
    }

    async fn delete(&self) -> crate::Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> crate::Result<(RpList, Self::Lister)> {
        let capability = self.info.full_capability();
        if !capability.list_with_versions && args.versions() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::List,
                "version",
            ));
        }

        self.inner.list(path, args).await
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> crate::Result<(RpRead, Self::BlockingReader)> {
        self.inner().blocking_read(path, args)
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> crate::Result<(RpWrite, Self::BlockingWriter)> {
        let capability = self.info.full_capability();
        if !capability.write_with_content_type && args.content_type().is_some() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::Write,
                "content_type",
            ));
        }
        if !capability.write_with_cache_control && args.cache_control().is_some() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::Write,
                "cache_control",
            ));
        }
        if !capability.write_with_content_disposition && args.content_disposition().is_some() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::Write,
                "content_disposition",
            ));
        }

        self.inner.blocking_write(path, args)
    }

    fn blocking_delete(&self) -> crate::Result<(RpDelete, Self::BlockingDeleter)> {
        self.inner.blocking_delete()
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> crate::Result<(RpList, Self::BlockingLister)> {
        let capability = self.info.full_capability();
        if !capability.list_with_versions && args.versions() {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::List,
                "version",
            ));
        }

        self.inner.blocking_list(path, args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Capability, ErrorKind, Operator};

    #[derive(Debug)]
    struct MockService {
        capability: Capability,
    }

    impl Access for MockService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type Deleter = oio::Deleter;
        type BlockingReader = oio::BlockingReader;
        type BlockingWriter = oio::BlockingWriter;
        type BlockingLister = oio::BlockingLister;
        type BlockingDeleter = oio::BlockingDeleter;

        fn info(&self) -> Arc<AccessorInfo> {
            let info = AccessorInfo::default();
            info.set_native_capability(self.capability);

            info.into()
        }

        async fn write(&self, _: &str, _: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
            Ok((RpWrite::new(), Box::new(())))
        }

        async fn list(&self, _: &str, _: OpList) -> crate::Result<(RpList, Self::Lister)> {
            Ok((RpList {}, Box::new(())))
        }
    }

    fn new_test_operator(capability: Capability) -> Operator {
        let srv = MockService { capability };

        Operator::from_inner(Arc::new(srv)).layer(CapabilityCheckLayer)
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
}
