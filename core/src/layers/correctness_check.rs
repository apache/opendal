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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use crate::raw::*;
use crate::{Error, ErrorKind};

/// Add a correctness capability check layer for every operation
///
/// Before performing any operations, we will first verify the operation and its critical arguments
/// against the capability of the underlying service. If the operation or arguments is not supported,
/// an error will be returned directly.
///
/// # Notes
///
/// OpenDAL applies this checker to every accessor by default, so users don't need to invoke it manually.
/// this checker ensures the operation and its critical arguments, which might affect the correctness of
/// the call, are supported by the underlying service.
///
/// for example, when calling `write_with_append`, but `append` is not supported by the underlying
/// service, an `Unsupported` error is returned. without this check, undesired data may be written.
#[derive(Default)]
pub struct CorrectnessCheckLayer;

impl<A: Access> Layer<A> for CorrectnessCheckLayer {
    type LayeredAccess = CorrectnessAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        CorrectnessAccessor {
            info: inner.info(),
            inner,
        }
    }
}

pub(crate) fn new_unsupported_error(info: &AccessorInfo, op: impl Into<&'static str>) -> Error {
    let scheme = info.scheme();
    let op = op.into();

    Error::new(
        ErrorKind::Unsupported,
        format!("service {scheme} doesn't support operation {op}"),
    )
    .with_operation(op)
}

pub(crate) fn new_unsupported_args_error(
    info: &AccessorInfo,
    op: impl Into<&'static str>,
    args: &str,
) -> Error {
    let scheme = info.scheme();
    let op = op.into();

    Error::new(
        ErrorKind::Unsupported,
        format!("service {scheme} doesn't support operation {op} with args {args}"),
    )
    .with_operation(op)
}

pub struct CorrectnessAccessor<A: Access> {
    info: Arc<AccessorInfo>,
    inner: A,
}

impl<A: Access> Debug for CorrectnessAccessor<A> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CorrectnessCheckAccessor")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<A: Access> LayeredAccess for CorrectnessAccessor<A> {
    type Inner = A;
    type Reader = A::Reader;
    type BlockingReader = A::BlockingReader;
    type Writer = A::Writer;
    type BlockingWriter = A::BlockingWriter;
    type Lister = A::Lister;
    type BlockingLister = A::BlockingLister;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    async fn create_dir(&self, path: &str, args: OpCreateDir) -> crate::Result<RpCreateDir> {
        let capability = self.info.full_capability();
        if !capability.create_dir {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::CreateDir,
            ));
        }

        self.inner.create_dir(path, args).await
    }

    async fn read(&self, path: &str, args: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
        let capability = self.info.full_capability();
        if !capability.read {
            return Err(new_unsupported_error(self.info.as_ref(), Operation::Read));
        }

        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
        let capability = self.info.full_capability();
        if !capability.write {
            return Err(new_unsupported_error(&self.info, Operation::Write));
        }
        if args.append() && !capability.write_can_append {
            return Err(new_unsupported_args_error(
                &self.info,
                Operation::Write,
                "append",
            ));
        }
        if args.if_not_exists() && !capability.write_with_if_not_exists {
            return Err(new_unsupported_args_error(
                &self.info,
                Operation::Write,
                "if_not_exists",
            ));
        }
        if args.if_none_match().is_some() && !capability.write_with_if_none_match {
            return Err(new_unsupported_args_error(
                self.info.as_ref(),
                Operation::Write,
                "if_none_match",
            ));
        }

        self.inner.write(path, args).await
    }

    async fn copy(&self, from: &str, to: &str, args: OpCopy) -> crate::Result<RpCopy> {
        let capability = self.info.full_capability();
        if !capability.copy {
            return Err(new_unsupported_error(self.info.as_ref(), Operation::Copy));
        }

        self.inner.copy(from, to, args).await
    }

    async fn rename(&self, from: &str, to: &str, args: OpRename) -> crate::Result<RpRename> {
        let capability = self.info.full_capability();
        if !capability.rename {
            return Err(new_unsupported_error(self.info.as_ref(), Operation::Rename));
        }

        self.inner.rename(from, to, args).await
    }

    async fn stat(&self, path: &str, args: OpStat) -> crate::Result<RpStat> {
        let capability = self.info.full_capability();
        if !capability.stat {
            return Err(new_unsupported_error(self.info.as_ref(), Operation::Stat));
        }

        self.inner.stat(path, args).await
    }

    async fn delete(&self, path: &str, args: OpDelete) -> crate::Result<RpDelete> {
        let capability = self.info.full_capability();
        if !capability.delete {
            return Err(new_unsupported_error(self.info.as_ref(), Operation::Delete));
        }

        self.inner.delete(path, args).await
    }

    async fn list(&self, path: &str, args: OpList) -> crate::Result<(RpList, Self::Lister)> {
        let capability = self.info.full_capability();
        if !capability.list {
            return Err(new_unsupported_error(self.info.as_ref(), Operation::List));
        }

        self.inner.list(path, args).await
    }

    async fn batch(&self, args: OpBatch) -> crate::Result<RpBatch> {
        let capability = self.info.full_capability();
        if !capability.batch {
            return Err(new_unsupported_error(self.info.as_ref(), Operation::Batch));
        }

        self.inner.batch(args).await
    }

    async fn presign(&self, path: &str, args: OpPresign) -> crate::Result<RpPresign> {
        let capability = self.info.full_capability();
        if !capability.presign {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::Presign,
            ));
        }

        self.inner.presign(path, args).await
    }

    fn blocking_create_dir(&self, path: &str, args: OpCreateDir) -> crate::Result<RpCreateDir> {
        let capability = self.info.full_capability();
        if !capability.create_dir || !capability.blocking {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::BlockingCreateDir,
            ));
        }

        self.inner.blocking_create_dir(path, args)
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> crate::Result<(RpRead, Self::BlockingReader)> {
        let capability = self.info.full_capability();
        if !capability.read || !capability.blocking {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::BlockingRead,
            ));
        }

        self.inner.blocking_read(path, args)
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
    ) -> crate::Result<(RpWrite, Self::BlockingWriter)> {
        let capability = self.info.full_capability();
        if !capability.write || !capability.blocking {
            return Err(new_unsupported_error(&self.info, Operation::BlockingWrite));
        }
        if args.append() && !capability.write_can_append {
            return Err(new_unsupported_args_error(
                &self.info,
                Operation::BlockingWrite,
                "append",
            ));
        }
        if args.if_not_exists() && !capability.write_with_if_not_exists {
            return Err(new_unsupported_args_error(
                &self.info,
                Operation::BlockingWrite,
                "if_not_exists",
            ));
        }
        if args.if_none_match().is_some() && !capability.write_with_if_none_match {
            return Err(new_unsupported_args_error(
                self.info.as_ref(),
                Operation::BlockingWrite,
                "if_none_match",
            ));
        }

        self.inner.blocking_write(path, args)
    }

    fn blocking_copy(&self, from: &str, to: &str, args: OpCopy) -> crate::Result<RpCopy> {
        let capability = self.info.full_capability();
        if !capability.copy || !capability.blocking {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::BlockingCopy,
            ));
        }

        self.inner().blocking_copy(from, to, args)
    }

    fn blocking_rename(&self, from: &str, to: &str, args: OpRename) -> crate::Result<RpRename> {
        let capability = self.info.full_capability();
        if !capability.rename || !capability.blocking {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::BlockingRename,
            ));
        }

        self.inner().blocking_rename(from, to, args)
    }

    fn blocking_stat(&self, path: &str, args: OpStat) -> crate::Result<RpStat> {
        let capability = self.info.full_capability();
        if !capability.stat || !capability.blocking {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::BlockingStat,
            ));
        }

        self.inner.blocking_stat(path, args)
    }

    fn blocking_delete(&self, path: &str, args: OpDelete) -> crate::Result<RpDelete> {
        let capability = self.info.full_capability();
        if !capability.delete || !capability.blocking {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::BlockingDelete,
            ));
        }

        self.inner().blocking_delete(path, args)
    }

    fn blocking_list(
        &self,
        path: &str,
        args: OpList,
    ) -> crate::Result<(RpList, Self::BlockingLister)> {
        let capability = self.info.full_capability();
        if !capability.list || !capability.blocking {
            return Err(new_unsupported_error(
                self.info.as_ref(),
                Operation::BlockingList,
            ));
        }

        self.inner.blocking_list(path, args)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::raw::{oio, PresignedRequest};
    use crate::{Capability, EntryMode, Metadata, Operator};
    use http::HeaderMap;
    use http::Method as HttpMethod;

    #[derive(Debug)]
    struct MockService {
        capability: Capability,
    }

    impl Access for MockService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = oio::Lister;
        type BlockingReader = oio::BlockingReader;
        type BlockingWriter = oio::BlockingWriter;
        type BlockingLister = oio::BlockingLister;

        fn info(&self) -> Arc<AccessorInfo> {
            let mut info = AccessorInfo::default();
            info.set_native_capability(self.capability);

            info.into()
        }

        async fn create_dir(&self, _: &str, _: OpCreateDir) -> crate::Result<RpCreateDir> {
            Ok(RpCreateDir {})
        }

        async fn stat(&self, _: &str, _: OpStat) -> crate::Result<RpStat> {
            Ok(RpStat::new(Metadata::new(EntryMode::Unknown)))
        }

        async fn read(&self, _: &str, _: OpRead) -> crate::Result<(RpRead, Self::Reader)> {
            Ok((RpRead::new(), Box::new(bytes::Bytes::new())))
        }

        async fn write(&self, _: &str, _: OpWrite) -> crate::Result<(RpWrite, Self::Writer)> {
            Ok((RpWrite::new(), Box::new(())))
        }

        async fn delete(&self, _: &str, _: OpDelete) -> crate::Result<RpDelete> {
            Ok(RpDelete {})
        }

        async fn list(&self, _: &str, _: OpList) -> crate::Result<(RpList, Self::Lister)> {
            Ok((RpList {}, Box::new(())))
        }

        async fn copy(&self, _: &str, _: &str, _: OpCopy) -> crate::Result<RpCopy> {
            Ok(RpCopy {})
        }

        async fn rename(&self, _: &str, _: &str, _: OpRename) -> crate::Result<RpRename> {
            Ok(RpRename {})
        }

        async fn presign(&self, _: &str, _: OpPresign) -> crate::Result<RpPresign> {
            Ok(RpPresign::new(PresignedRequest::new(
                HttpMethod::POST,
                "https://example.com/presign".parse().expect("should parse"),
                HeaderMap::new(),
            )))
        }
    }

    fn new_test_operator(capability: Capability) -> Operator {
        let srv = MockService { capability };

        Operator::from_inner(Arc::new(srv)).layer(CorrectnessCheckLayer)
    }

    #[tokio::test]
    async fn test_read() {
        let op = new_test_operator(Capability::default());
        let res = op.read("path").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            read: true,
            stat: true,
            ..Default::default()
        });
        let res = op.read("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_stat() {
        let op = new_test_operator(Capability::default());
        let res = op.stat("path").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            stat: true,
            ..Default::default()
        });
        let res = op.stat("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_writer() {
        let op = new_test_operator(Capability::default());
        let bs: Vec<u8> = vec![];
        let res = op.write("path", bs).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            write: true,
            ..Default::default()
        });
        let res = op.writer("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_write_with() {
        let op = new_test_operator(Capability {
            write: true,
            ..Default::default()
        });
        let res = op.write_with("path", "".as_bytes()).append(true).await;
        assert!(res.is_err());

        let res = op
            .write_with("path", "".as_bytes())
            .if_not_exists(true)
            .await;
        assert!(res.is_err());

        let res = op
            .write_with("path", "".as_bytes())
            .if_none_match("etag")
            .await;
        assert!(res.is_err());

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
    async fn test_create_dir() {
        let op = new_test_operator(Capability::default());
        let res = op.create_dir("path/").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            create_dir: true,
            ..Default::default()
        });
        let res = op.create_dir("path/").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_delete() {
        let op = new_test_operator(Capability::default());
        let res = op.delete("path").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            delete: true,
            ..Default::default()
        });
        let res = op.delete("path").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_copy() {
        let op = new_test_operator(Capability::default());
        let res = op.copy("path_a", "path_b").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            copy: true,
            ..Default::default()
        });
        let res = op.copy("path_a", "path_b").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_rename() {
        let op = new_test_operator(Capability::default());
        let res = op.rename("path_a", "path_b").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            rename: true,
            ..Default::default()
        });
        let res = op.rename("path_a", "path_b").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_list() {
        let op = new_test_operator(Capability::default());
        let res = op.list("path/").await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            list: true,
            list_with_recursive: true,
            ..Default::default()
        });
        let res = op.list("path/").await;
        assert!(res.is_ok())
    }

    #[tokio::test]
    async fn test_presign() {
        let op = new_test_operator(Capability::default());
        let res = op.presign_read("path", Duration::from_secs(1)).await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);

        let op = new_test_operator(Capability {
            presign: true,
            ..Default::default()
        });
        let res = op.presign_read("path", Duration::from_secs(1)).await;
        assert!(res.is_ok())
    }
}
