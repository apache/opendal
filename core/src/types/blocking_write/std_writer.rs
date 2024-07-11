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
    w: Option<WriteGenerator<oio::BlockingWriter>>,
    buf: oio::FlexBuf,
}

impl StdWriter {
    /// NOTE: don't allow users to create directly.
    #[inline]
    pub(crate) fn new(w: WriteGenerator<oio::BlockingWriter>) -> Self {
        StdWriter {
            w: Some(w),
            buf: oio::FlexBuf::new(256 * 1024),
        }
    }

    /// Close the internal writer and make sure all data have been stored.
    pub fn close(&mut self) -> std::io::Result<()> {
        // Make sure all cache has been flushed.
        self.flush()?;

        let Some(w) = &mut self.w else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "writer has been closed",
            ));
        };

        w.close()
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;

        // Drop writer after close succeed;
        self.w = None;

        Ok(())
    }
}

impl Write for StdWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let Some(w) = &mut self.w else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "writer has been closed",
            ));
        };

        loop {
            let n = self.buf.put(buf);
            if n > 0 {
                return Ok(n);
            }

            let bs = self.buf.get().expect("frozen buffer must be valid");
            let n = w
                .write(Buffer::from(bs))
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
            self.buf.advance(n);
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let Some(w) = &mut self.w else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "writer has been closed",
            ));
        };

        loop {
            // Make sure buf has been frozen.
            self.buf.freeze();
            let Some(bs) = self.buf.get() else {
                return Ok(());
            };

            w.write(Buffer::from(bs))
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
            self.buf.clean();
        }
    }
}

impl Drop for StdWriter {
    fn drop(&mut self) {
        if let Some(mut w) = self.w.take() {
            // Ignore error happens in close.
            let _ = w.close();
        }
    }
}
