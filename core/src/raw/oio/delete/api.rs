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
use std::ops::DerefMut;

/// Deleter is a type erased [`Delete`]
pub type Deleter = Box<dyn DeleteDyn>;

/// The Delete trait defines interfaces for performing deletion operations.
pub trait Delete: Unpin + Send + Sync {
    /// Requests deletion of a resource at the specified path with optional arguments
    ///
    /// # Parameters
    /// - `path`: The path of the resource to delete
    /// - `args`: Additional arguments for the delete operation
    ///
    /// # Returns
    /// - `Ok(())`: The deletion request has been successfully queued (does not guarantee actual deletion)
    /// - `Err(err)`: An error occurred and the deletion request was not queued
    ///
    /// # Notes
    /// This method just queue the delete request. The actual deletion will be
    /// performed when `flush` is called.
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()>;

    /// Flushes the deletion queue to ensure queued deletions are executed
    ///
    /// # Returns
    /// - `Ok(0)`: All queued deletions have been processed or the queue is empty.
    /// - `Ok(count)`: The number of resources successfully deleted. Implementations should
    ///   return an error if the queue is non-empty but no resources were deleted
    /// - `Err(err)`: An error occurred while performing the deletions
    ///
    /// # Notes
    /// - This method is asynchronous and will wait for queued deletions to complete
    fn flush(&mut self) -> impl Future<Output = Result<usize>> + MaybeSend;
}

impl Delete for () {
    fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output deleter doesn't support delete",
        ))
    }

    async fn flush(&mut self) -> Result<usize> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output deleter doesn't support flush",
        ))
    }
}

/// The dyn version of [`Delete`]
pub trait DeleteDyn: Unpin + Send + Sync {
    /// The dyn version of [`Delete::delete`]
    fn delete_dyn(&mut self, path: &str, args: OpDelete) -> Result<()>;

    /// The dyn version of [`Delete::flush`]
    fn flush_dyn(&mut self) -> BoxedFuture<Result<usize>>;
}

impl<T: Delete + ?Sized> DeleteDyn for T {
    fn delete_dyn(&mut self, path: &str, args: OpDelete) -> Result<()> {
        Delete::delete(self, path, args)
    }

    fn flush_dyn(&mut self) -> BoxedFuture<Result<usize>> {
        Box::pin(self.flush())
    }
}

impl<T: DeleteDyn + ?Sized> Delete for Box<T> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.deref_mut().delete_dyn(path, args)
    }

    async fn flush(&mut self) -> Result<usize> {
        self.deref_mut().flush_dyn().await
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

    /// Flushes the deletion queue to ensure queued deletions are executed
    ///
    /// # Returns
    /// - `Ok(0)`: All queued deletions have been processed or the queue is empty.
    /// - `Ok(count)`: The number of resources successfully deleted. Implementations should
    ///   return an error if the queue is non-empty but no resources were deleted
    /// - `Err(err)`: An error occurred while performing the deletions
    fn flush(&mut self) -> Result<usize>;
}

impl BlockingDelete for () {
    fn delete(&mut self, _: &str, _: OpDelete) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output deleter doesn't support delete",
        ))
    }

    fn flush(&mut self) -> Result<usize> {
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

    fn flush(&mut self) -> Result<usize> {
        (**self).flush()
    }
}
