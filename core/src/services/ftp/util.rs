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

use std::io;

use bb8::PooledConnection;
use bytes::Bytes;
use futures::AsyncRead;
use futures::AsyncReadExt;
use suppaftp::Status;
use tokio::io::ReadBuf;

use super::backend::Manager;
use crate::raw::*;
use crate::services::ftp::err::parse_error;
use crate::*;

/// Wrapper for ftp data stream and command stream.
pub struct FtpReader {
    reader: Box<dyn AsyncRead + Send + Unpin>,
    conn: Option<PooledConnection<'static, Manager>>,
    buf: Vec<u8>,
}

unsafe impl Sync for FtpReader {}

impl FtpReader {
    /// Create an instance of FtpReader.
    pub fn new(
        reader: Box<dyn AsyncRead + Send + Unpin>,
        conn: PooledConnection<'static, Manager>,
    ) -> Self {
        Self {
            reader,
            conn: Some(conn),
            buf: Vec::with_capacity(64 * 1024),
        }
    }
}

impl oio::Read for FtpReader {
    async fn next_v2(&mut self, size: usize) -> Result<Bytes> {
        if self.conn.is_none() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "ftp reader is already closed",
            ));
        }

        // Make sure buf has enough space.
        if self.buf.capacity() < size {
            self.buf.reserve(size - self.buf.capacity());
        }
        let buf = self.buf.spare_capacity_mut();
        let mut read_buf: ReadBuf = ReadBuf::uninit(buf);

        // SAFETY: Read at most `size` bytes into `read_buf`.
        unsafe {
            read_buf.assume_init(size);
        }

        let data = self.reader.read(read_buf.initialize_unfilled()).await;

        // Data read with success, copy and return it.
        if let Ok(n) = data {
            if n > 0 {
                read_buf.set_filled(n);
                return Ok(Bytes::copy_from_slice(&self.buf[..n]));
            }
        }

        // While hitting Error or EOF, we should end this ftp stream.
        let _ = self
            .conn
            .take()
            .expect("connection must be valid during read")
            .read_response_in(&[Status::ClosingDataConnection, Status::RequestedFileActionOk])
            .await
            .map_err(parse_error)?;
        Ok(Bytes::new())
    }

    async fn seek(&mut self, pos: io::SeekFrom) -> Result<u64> {
        let _ = pos;

        Err(Error::new(
            ErrorKind::Unsupported,
            "ftp reader doesn't support seeking",
        ))
    }
}
