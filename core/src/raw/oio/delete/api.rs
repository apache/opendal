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

use crate::raw::{BoxedFuture, MaybeSend, OpDelete};
use crate::*;
use std::future::Future;

/// Deleter is a type erased [`Delete`]
pub type Deleter = Box<dyn DeleteDyn>;

/// Delete is the trait to perform delete operations.
pub trait Delete: Unpin + Send + Sync {
    /// Delete given path with optional arguments.
    ///
    /// # Behavior
    ///
    /// - `Ok(())` means the path has been queued for deletion.
    /// - `Err(err)` means error happens and no deletion has been queued.
    fn delete(
        &mut self,
        path: String,
        args: OpDelete,
    ) -> impl Future<Output = Result<()>> + MaybeSend;

    /// Flush the deletion queue to make sure all enqueued paths are deleted.
    ///
    /// # Behavior
    ///
    /// - `Ok(())` means all enqueued paths are deleted successfully.
    /// - `Err(err)` means error happens while performing deletion.
    fn flush(&mut self) -> impl Future<Output = Result<()>> + MaybeSend;
}

impl Delete for () {
    async fn delete(&mut self, _: String, _: OpDelete) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output deleter doesn't support delete",
        ))
    }

    async fn flush(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output deleter doesn't support flush",
        ))
    }
}

pub trait DeleteDyn: Unpin + Send + Sync {
    fn delete_dyn(&mut self, path: String, args: OpDelete) -> BoxedFuture<Result<()>>;

    fn flush_dyn(&mut self) -> BoxedFuture<Result<()>>;
}

impl<T: Delete + ?Sized> DeleteDyn for T {
    fn delete_dyn(&mut self, path: String, args: OpDelete) -> BoxedFuture<Result<()>> {
        Box::pin(self.delete(path, args))
    }

    fn flush_dyn(&mut self) -> BoxedFuture<Result<()>> {
        Box::pin(self.flush())
    }
}

impl<T: DeleteDyn + ?Sized> Delete for Box<T> {
    async fn delete(&mut self, path: String, args: OpDelete) -> Result<()> {
        self.delete_dyn(path, args).await
    }

    async fn flush(&mut self) -> Result<()> {
        self.flush_dyn().await
    }
}

/// BlockingDeleter is a type erased [`BlockingDelete`]
pub type BlockingDeleter = Box<dyn BlockingDelete>;

/// BlockingDelete is the trait to perform delete operations.
pub trait BlockingDelete: Send + Sync + 'static {
    /// Delete given path with optional arguments.
    ///
    /// # Behavior
    ///
    /// - `Ok(())` means the path has been queued for deletion.
    /// - `Err(err)` means error happens and no deletion has been queued.
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()>;

    /// Flush the deletion queue to make sure all enqueued paths are deleted.
    ///
    /// # Behavior
    ///
    /// - `Ok(())` means all enqueued paths are deleted successfully.
    /// - `Err(err)` means error happens while performing deletion.
    fn flush(&mut self) -> Result<()>;
}

impl BlockingDelete for () {
    fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output deleter doesn't support delete",
        ))
    }

    fn flush(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output deleter doesn't support flush",
        ))
    }
}

/// `Box<dyn BlockingDelete>` won't implement `BlockingDelete` automatically.
///
/// To make BlockingWriter work as expected, we must add this impl.
impl<T: BlockingDelete + ?Sized> BlockingDelete for Box<T> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        (**self).delete(path, args)
    }

    fn flush(&mut self) -> Result<()> {
        (**self).flush()
    }
}
