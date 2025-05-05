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

use crate::raw::*;
use crate::*;
use std::future::Future;

/// OneShotDelete is used to implement [`oio::Delete`] based on one shot operation.
///
/// OneShotDeleter will perform delete operation while calling `flush`.
pub trait OneShotDelete: Send + Sync + Unpin + 'static {
    /// delete_once delete one path at once.
    ///
    /// Implementations should make sure that the data is deleted correctly at once.
    fn delete_once(
        &self,
        path: String,
        args: OpDelete,
    ) -> impl Future<Output = Result<()>> + MaybeSend;
}

/// BlockingOneShotDelete is used to implement [`oio::BlockingDelete`] based on one shot operation.
///
/// BlockingOneShotDeleter will perform delete operation while calling `flush`.
pub trait BlockingOneShotDelete: Send + Sync + 'static {
    /// delete_once delete one path at once.
    ///
    /// Implementations should make sure that the data is deleted correctly at once.
    fn blocking_delete_once(&self, path: String, args: OpDelete) -> Result<()>;
}

/// OneShotDelete is used to implement [`oio::Delete`] based on one shot.
pub struct OneShotDeleter<D> {
    inner: D,
    delete: Option<(String, OpDelete)>,
}

impl<D> OneShotDeleter<D> {
    /// Create a new one shot deleter.
    pub fn new(inner: D) -> Self {
        Self {
            inner,
            delete: None,
        }
    }

    fn delete_inner(&mut self, path: String, args: OpDelete) -> Result<()> {
        if self.delete.is_some() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "OneShotDeleter doesn't support batch delete",
            ));
        }

        self.delete = Some((path, args));
        Ok(())
    }
}

impl<D: OneShotDelete> oio::Delete for OneShotDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.delete_inner(path.to_string(), args)
    }

    async fn flush(&mut self) -> Result<usize> {
        let Some((path, args)) = self.delete.clone() else {
            return Ok(0);
        };

        self.inner.delete_once(path, args).await?;
        self.delete = None;
        Ok(1)
    }
}

impl<D: BlockingOneShotDelete> oio::BlockingDelete for OneShotDeleter<D> {
    fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.delete_inner(path.to_string(), args)
    }

    fn flush(&mut self) -> Result<usize> {
        let Some((path, args)) = self.delete.clone() else {
            return Ok(0);
        };

        self.inner.blocking_delete_once(path, args)?;
        self.delete = None;
        Ok(1)
    }
}
