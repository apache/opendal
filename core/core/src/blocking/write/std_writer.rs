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

use crate::*;

/// `StdWriter` adapts a [`crate::blocking::Writer`] to [`std::io::Write`].
///
/// Users can use this adapter in cases where they need to use [`std::io::Write`] related trait.
///
/// # Notes
///
/// Files are automatically closed when they go out of scope. Errors detected on closing are ignored
/// by the implementation of Drop. Use the method `close` if these errors must be manually handled.
pub struct StdWriter {
    handle: tokio::runtime::Handle,
    w: Option<FuturesAsyncWriter>,
}

impl StdWriter {
    /// NOTE: don't allow users to create directly.
    #[inline]
    pub(crate) fn new(handle: tokio::runtime::Handle, w: Writer) -> Self {
        StdWriter {
            handle,
            w: Some(w.into_futures_async_write()),
        }
    }

    /// Close the internal writer and make sure all data have been stored.
    pub fn close(&mut self) -> std::io::Result<()> {
        let Some(w) = self.w.as_mut() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been dropped").into());
        };

        self.handle.block_on(w.close())
    }
}

impl Write for StdWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(w) = self.w.as_mut() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been dropped").into());
        };

        self.handle.block_on(w.write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let Some(w) = self.w.as_mut() else {
            return Err(Error::new(ErrorKind::Unexpected, "writer has been dropped").into());
        };

        self.handle.block_on(w.flush())
    }
}

/// Make sure the inner writer is dropped in async context.
impl Drop for StdWriter {
    fn drop(&mut self) {
        let Some(mut v) = self.w.take() else {
            return;
        };

        self.handle.block_on(async move {
            // Data that has been written but not sent yet lives in the writer's
            // internal buffer, so dropping without closing would lose it. Close
            // errors are ignored here, as documented for this adapter.
            //
            // A writer that `close` already closed is skipped: closing a closed
            // writer returns an error instead of closing it again.
            if !v.is_closed() {
                let _ = v.close().await;
            }

            drop(v);
        });
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::Write;

    use super::*;
    use crate::blocking;
    use crate::services;

    const PAYLOAD: &[u8] = b"hello, world";

    fn new_operator() -> crate::Result<blocking::Operator> {
        let op = Operator::new(services::Memory::default())?;
        blocking::Operator::new(op)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_without_close_persists_data() {
        let op = new_operator().expect("memory operator must be created");

        let written = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
            let path = "test_drop_without_close_persists_data";
            let mut w = op.writer(path).map_err(io::Error::from)?.into_std_write();
            w.write_all(PAYLOAD)?;

            drop(w);

            Ok(op.read(path).map_err(io::Error::from)?.to_vec())
        })
        .await
        .expect("blocking write task must complete")
        .expect("blocking write must succeed");

        assert_eq!(written, PAYLOAD);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_after_flush_persists_data() {
        let op = new_operator().expect("memory operator must be created");

        let written = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<u8>> {
            let path = "test_drop_after_flush_persists_data";
            let mut w = op.writer(path).map_err(io::Error::from)?.into_std_write();
            w.write_all(PAYLOAD)?;
            w.flush()?;

            drop(w);

            Ok(op.read(path).map_err(io::Error::from)?.to_vec())
        })
        .await
        .expect("blocking write task must complete")
        .expect("blocking write must succeed");

        assert_eq!(written, PAYLOAD);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_drop_after_close_does_not_close_again() {
        let op = new_operator().expect("memory operator must be created");

        let (closed, written) =
            tokio::task::spawn_blocking(move || -> std::io::Result<(bool, Vec<u8>)> {
                let path = "test_drop_after_close_does_not_close_again";
                let mut w = op.writer(path).map_err(io::Error::from)?.into_std_write();
                w.write_all(PAYLOAD)?;
                w.close()?;

                // Drop must skip the second close, so the inner writer has to be
                // observed as closed before the writer goes out of scope.
                let closed = w.w.as_ref().expect("writer must be present").is_closed();

                drop(w);

                Ok((closed, op.read(path).map_err(io::Error::from)?.to_vec()))
            })
            .await
            .expect("blocking write task must complete")
            .expect("blocking write must succeed");

        assert!(closed, "explicit close must leave the sink closed");
        assert_eq!(written, PAYLOAD);
    }
}
