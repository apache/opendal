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

use crate::Deleter as AsyncDeleter;
use crate::*;

/// BlockingDeleter is designed to continuously remove content from storage.
///
/// It leverages batch deletion capabilities provided by storage services for efficient removal.
pub struct Deleter {
    handle: tokio::runtime::Handle,
    inner: Option<AsyncDeleter>,
}

impl Deleter {
    pub(crate) fn create(handle: tokio::runtime::Handle, inner: AsyncDeleter) -> Result<Self> {
        Ok(Self {
            handle,
            inner: Some(inner),
        })
    }

    /// Delete a path with default or per-entry options.
    ///
    /// Pass a path directly to use [`options::DeleteOptions::default`]. Pass
    /// `(String, DeleteOptions)` when this entry needs version or condition
    /// options.
    pub fn delete(&mut self, input: impl IntoDeleteInput) -> Result<()> {
        let Some(inner) = self.inner.as_mut() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "deleter has been dropped",
            ));
        };

        self.handle.block_on(inner.delete(input))
    }

    /// Delete an infallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Deleter::delete_try_iter`]: delete a fallible iterator of paths.
    pub fn delete_iter<I, D>(&mut self, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = D>,
        D: IntoDeleteInput,
    {
        for entry in iter {
            self.delete(entry)?;
        }
        Ok(())
    }

    /// Delete an fallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Deleter::delete_iter`]: delete an infallible iterator of paths.
    pub fn delete_try_iter<I, D>(&mut self, try_iter: I) -> Result<()>
    where
        I: IntoIterator<Item = Result<D>>,
        D: IntoDeleteInput,
    {
        for entry in try_iter {
            self.delete(entry?)?;
        }
        Ok(())
    }

    /// Close the deleter, this will flush the deleter and wait until all paths are deleted.
    pub fn close(&mut self) -> Result<()> {
        let Some(inner) = self.inner.as_mut() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "deleter has been dropped",
            ));
        };

        self.handle.block_on(inner.close())
    }
}

/// Make sure the inner deleter is dropped in async context.
impl Drop for Deleter {
    fn drop(&mut self) {
        if let Some(v) = self.inner.take() {
            self.handle.block_on(async move { drop(v) });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Entry;
    use crate::blocking::Operator;
    use crate::services;

    fn new_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn new_blocking_operator(rt: &tokio::runtime::Runtime) -> Operator {
        // The guard must not outlive construction, or calls on the operator
        // would nest a `block_on` inside the entered runtime.
        let _guard = rt.enter();
        let op = crate::Operator::new(services::Memory::default()).unwrap();
        Operator::new(op).unwrap()
    }

    #[test]
    fn test_delete_try_iter_accepts_blocking_lister() {
        let rt = new_runtime();
        let op = new_blocking_operator(&rt);
        op.write("dir/a", "a").unwrap();
        op.write("dir/b", "b").unwrap();

        let mut deleter = op.deleter().unwrap();
        deleter.delete_try_iter(op.lister("dir/").unwrap()).unwrap();
        deleter.close().unwrap();

        assert!(op.list("dir/").unwrap().is_empty());
    }

    #[test]
    fn test_delete_try_iter_error_drops_blocking_lister() {
        let rt = new_runtime();
        let op = new_blocking_operator(&rt);
        op.write("dir/a", "a").unwrap();
        op.write("dir/b", "b").unwrap();

        let err = Error::new(ErrorKind::Unexpected, "injected iterator failure");
        let iter = std::iter::once(Err::<Entry, _>(err)).chain(op.lister("dir/").unwrap());

        let mut deleter = op.deleter().unwrap();
        let err = deleter.delete_try_iter(iter).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        deleter.close().unwrap();

        assert_eq!(op.list("dir/").unwrap().len(), 2);
    }

    #[test]
    fn test_deleter_drop_runs_in_runtime() {
        use std::fmt;
        use std::sync::Arc;
        use std::sync::Mutex;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use crate::raw::*;

        #[derive(Default)]
        struct MockCalls {
            deleted: AtomicUsize,
            dropped: AtomicUsize,
            dropped_in_runtime: AtomicBool,
        }

        struct MockDeleter {
            calls: Arc<MockCalls>,
        }

        impl oio::Delete for MockDeleter {
            async fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
                self.calls.deleted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }

            async fn close(&mut self) -> Result<()> {
                Ok(())
            }
        }

        impl Drop for MockDeleter {
            fn drop(&mut self) {
                self.calls.dropped.fetch_add(1, Ordering::Relaxed);
                self.calls.dropped_in_runtime.store(
                    tokio::runtime::Handle::try_current().is_ok(),
                    Ordering::Relaxed,
                );
            }
        }

        #[derive(Default)]
        struct MockDeleteService {
            deleter: Mutex<Option<MockDeleter>>,
        }

        impl fmt::Debug for MockDeleteService {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("MockDeleteService").finish()
            }
        }

        impl Builder for MockDeleteService {
            type Config = ();

            fn build(self) -> Result<impl Service> {
                Ok(self)
            }
        }

        impl Service for MockDeleteService {
            type Reader = ();
            type Writer = ();
            type Lister = ();
            type Deleter = MockDeleter;
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                ServiceInfo::with_scheme("mock-delete")
            }

            fn capability(&self) -> Capability {
                Capability {
                    delete: true,
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

            async fn stat(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpStat,
            ) -> Result<RpStat> {
                ().stat(ctx, path, args).await
            }

            fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<()> {
                ().read(ctx, path, args)
            }

            fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<()> {
                ().write(ctx, path, args)
            }

            fn delete(&self, _: &OperationContext) -> Result<MockDeleter> {
                Ok(self
                    .deleter
                    .lock()
                    .unwrap()
                    .take()
                    .expect("mock deleter already taken"))
            }

            fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<()> {
                ().list(ctx, path, args)
            }

            fn copy(
                &self,
                ctx: &OperationContext,
                from: &str,
                to: &str,
                args: OpCopy,
            ) -> Result<()> {
                ().copy(ctx, from, to, args)
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

        let calls = Arc::new(MockCalls::default());
        let rt = new_runtime();
        let blocking_op = {
            let _guard = rt.enter();
            let op = crate::Operator::new(MockDeleteService {
                deleter: Mutex::new(Some(MockDeleter {
                    calls: calls.clone(),
                })),
            })
            .unwrap();
            Operator::new(op).unwrap()
        };

        let mut deleter = blocking_op.deleter().unwrap();
        deleter.delete("test").unwrap();
        deleter.close().unwrap();
        assert_eq!(calls.deleted.load(Ordering::Relaxed), 1);
        assert_eq!(calls.dropped.load(Ordering::Relaxed), 0);

        drop(deleter);
        assert_eq!(calls.dropped.load(Ordering::Relaxed), 1);
        assert!(calls.dropped_in_runtime.load(Ordering::Relaxed));
    }
}
