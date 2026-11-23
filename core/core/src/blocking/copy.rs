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

use crate::Copier as AsyncCopier;
use crate::*;

/// BlockingCopier is designed to drive long-running copy operations in a
/// blocking manner.
pub struct Copier {
    handle: tokio::runtime::Handle,
    copier: Option<AsyncCopier>,
}

/// # Safety
///
/// BlockingCopier will only be accessed by `&mut Self`.
unsafe impl Sync for Copier {}

impl Copier {
    /// Create a new blocking copier.
    pub(crate) fn new(handle: tokio::runtime::Handle, copier: AsyncCopier) -> Self {
        Self {
            handle,
            copier: Some(copier),
        }
    }

    /// Abort the pending copy operation.
    pub fn abort(&mut self) -> Result<()> {
        let Some(copier) = self.copier.as_mut() else {
            return Ok(());
        };

        self.handle.block_on(copier.abort())
    }
}

impl Iterator for Copier {
    type Item = Result<usize>;

    fn next(&mut self) -> Option<Self::Item> {
        let copier = self.copier.as_mut()?;

        match self.handle.block_on(copier.next()) {
            Ok(Some(n)) => Some(Ok(n)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        }
    }
}

impl Drop for Copier {
    fn drop(&mut self) {
        if let Some(v) = self.copier.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fmt;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::raw::*;

    #[derive(Default)]
    struct MockCalls {
        next: AtomicUsize,
        abort: AtomicUsize,
        dropped: AtomicUsize,
        dropped_in_runtime: AtomicBool,
    }

    impl MockCalls {
        fn next(&self) -> usize {
            self.next.load(Ordering::Relaxed)
        }

        fn abort(&self) -> usize {
            self.abort.load(Ordering::Relaxed)
        }

        fn dropped(&self) -> usize {
            self.dropped.load(Ordering::Relaxed)
        }

        fn dropped_in_runtime(&self) -> bool {
            self.dropped_in_runtime.load(Ordering::Relaxed)
        }
    }

    struct MockCopy {
        steps: VecDeque<Result<Option<usize>>>,
        calls: Arc<MockCalls>,
    }

    impl MockCopy {
        fn new(
            calls: &Arc<MockCalls>,
            steps: impl IntoIterator<Item = Result<Option<usize>>>,
        ) -> Self {
            Self {
                steps: steps.into_iter().collect(),
                calls: calls.clone(),
            }
        }
    }

    impl oio::Copy for MockCopy {
        async fn next(&mut self) -> Result<Option<usize>> {
            self.calls.next.fetch_add(1, Ordering::Relaxed);
            self.steps.pop_front().unwrap_or(Ok(None))
        }

        async fn close(&mut self) -> Result<Metadata> {
            Ok(MetadataBuilder::unknown().build())
        }

        async fn abort(&mut self) -> Result<()> {
            self.calls.abort.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    impl Drop for MockCopy {
        fn drop(&mut self) {
            self.calls.dropped.fetch_add(1, Ordering::Relaxed);
            self.calls.dropped_in_runtime.store(
                tokio::runtime::Handle::try_current().is_ok(),
                Ordering::Relaxed,
            );
        }
    }

    #[derive(Default)]
    struct MockCopyService {
        copier: Mutex<Option<MockCopy>>,
    }

    impl MockCopyService {
        fn new(copier: MockCopy) -> Self {
            Self {
                copier: Mutex::new(Some(copier)),
            }
        }
    }

    impl fmt::Debug for MockCopyService {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("MockCopyService").finish()
        }
    }

    impl Builder for MockCopyService {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            Ok(self)
        }
    }

    impl Service for MockCopyService {
        type Reader = ();
        type Writer = ();
        type Lister = ();
        type Deleter = ();
        type Copier = MockCopy;
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("mock-copy")
        }

        fn capability(&self) -> Capability {
            Capability {
                copy: true,
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

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<MockCopy> {
            Ok(self
                .copier
                .lock()
                .unwrap()
                .take()
                .expect("mock copier already taken"))
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

    fn new_blocking_copier(mock: MockCopy) -> (tokio::runtime::Runtime, Copier) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        let blocking_op = {
            // The guard must not outlive construction, or calls on the copier
            // would run inside an entered runtime instead of exercising
            // `Handle::block_on` from a synchronous thread.
            let _guard = rt.enter();
            let op = crate::Operator::new(MockCopyService::new(mock)).unwrap();
            crate::blocking::Operator::new(op).unwrap()
        };
        let copier = blocking_op.copier("from", "to").unwrap();
        (rt, copier)
    }

    #[test]
    fn test_copier_abort_after_failed_iteration_reaches_none() {
        let calls = Arc::new(MockCalls::default());
        let (_rt, mut copier) = new_blocking_copier(MockCopy::new(
            &calls,
            [
                Ok(Some(4)),
                Err(Error::new(ErrorKind::Unexpected, "mock step failed")),
            ],
        ));

        assert_eq!(copier.next().unwrap().unwrap(), 4);
        assert_eq!(
            copier.next().unwrap().unwrap_err().kind(),
            ErrorKind::Unexpected
        );
        assert!(copier.next().is_none());
        assert!(copier.next().is_none());
        assert_eq!(calls.next(), 2);
        assert_eq!(calls.abort(), 0);
        assert_eq!(calls.dropped(), 0);

        copier.abort().unwrap();
        assert_eq!(calls.abort(), 1);
        assert_eq!(calls.dropped(), 1);
        assert!(calls.dropped_in_runtime());

        assert!(copier.next().is_none());
        copier.abort().unwrap();
        assert_eq!(calls.abort(), 1);
    }

    #[test]
    fn test_copier_drop_after_failed_iteration_runs_in_runtime() {
        let calls = Arc::new(MockCalls::default());
        let (_rt, mut copier) = new_blocking_copier(MockCopy::new(
            &calls,
            [
                Ok(Some(4)),
                Err(Error::new(ErrorKind::Unexpected, "mock step failed")),
            ],
        ));

        assert_eq!(copier.next().unwrap().unwrap(), 4);
        assert_eq!(
            copier.next().unwrap().unwrap_err().kind(),
            ErrorKind::Unexpected
        );
        assert!(copier.next().is_none());
        assert_eq!(calls.dropped(), 0);

        drop(copier);
        assert_eq!(calls.dropped(), 1);
        assert!(calls.dropped_in_runtime());
    }
}
