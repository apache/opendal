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

use std::io::Write;

use futures::AsyncWriteExt;

use crate::raw::*;
use crate::*;

/// StdWriter is the adapter of [`std::io::Write`] for [`BlockingWriter`].
///
/// Users can use this adapter in cases where they need to use [`std::io::Write`] related trait.
///
/// # Notes
///
/// Files are automatically closed when they go out of scope. Errors detected on closing are ignored
/// by the implementation of Drop. Use the method `close` if these errors must be manually handled.
pub struct StdWriter {
    handle: tokio::runtime::Handle,
    w: FuturesAsyncWriter,
}

impl StdWriter {
    /// NOTE: don't allow users to create directly.
    #[inline]
    pub(crate) fn new(handle: tokio::runtime::Handle, w: Writer) -> Self {
        StdWriter {
            handle,
            w: w.into_futures_async_write(),
        }
    }

    /// Close the internal writer and make sure all data have been stored.
    pub fn close(&mut self) -> std::io::Result<()> {
        self.handle.block_on(self.w.close())
    }
}

impl Write for StdWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.handle.block_on(self.w.write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.handle.block_on(self.w.flush())
    }
}
