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

use std::future::Future;
use std::ops::DerefMut;

use crate::raw::*;
use crate::*;

/// Writer is a type erased [`Write`]
pub type Writer = Box<dyn WriteDyn>;

/// Write is the trait that OpenDAL returns to callers.
pub trait Write: Unpin + Send + Sync {
    /// Write given bytes into writer.
    ///
    /// # Behavior
    ///
    /// - `Ok(())` means all bytes has been written successfully.
    /// - `Err(err)` means error happens and no bytes has been written.
    fn write(&mut self, bs: Buffer) -> impl Future<Output = Result<()>> + MaybeSend;

    /// Close the writer and make sure all data has been flushed.
    fn close(&mut self) -> impl Future<Output = Result<Metadata>> + MaybeSend;

    /// Abort the pending writer.
    fn abort(&mut self) -> impl Future<Output = Result<()>> + MaybeSend;
}

impl Write for () {
    async fn write(&mut self, _: Buffer) -> Result<()> {
        unimplemented!("write is required to be implemented for oio::Write")
    }

    async fn close(&mut self) -> Result<Metadata> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support close",
        ))
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "output writer doesn't support abort",
        ))
    }
}

pub trait WriteDyn: Unpin + Send + Sync {
    fn write_dyn(&mut self, bs: Buffer) -> BoxedFuture<'_, Result<()>>;

    fn close_dyn(&mut self) -> BoxedFuture<'_, Result<Metadata>>;

    fn abort_dyn(&mut self) -> BoxedFuture<'_, Result<()>>;
}

impl<T: Write + ?Sized> WriteDyn for T {
    fn write_dyn(&mut self, bs: Buffer) -> BoxedFuture<'_, Result<()>> {
        Box::pin(self.write(bs))
    }

    fn close_dyn(&mut self) -> BoxedFuture<'_, Result<Metadata>> {
        Box::pin(self.close())
    }

    fn abort_dyn(&mut self) -> BoxedFuture<'_, Result<()>> {
        Box::pin(self.abort())
    }
}

impl<T: WriteDyn + ?Sized> Write for Box<T> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.deref_mut().write_dyn(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.deref_mut().close_dyn().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.deref_mut().abort_dyn().await
    }
}
