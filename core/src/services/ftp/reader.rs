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

use bb8::PooledConnection;
use bytes::BytesMut;
use futures::AsyncRead;
use futures::AsyncReadExt;

use super::backend::Manager;
use super::err::parse_error;
use crate::raw::*;
use crate::*;

pub struct FtpReader {
    /// Keep the connection alive while data stream is alive.
    _ftp_stream: PooledConnection<'static, Manager>,

    data_stream: Box<dyn AsyncRead + Sync + Send + Unpin + 'static>,
    chunk: usize,
    buf: BytesMut,
}

/// # Safety
///
/// We only have `&mut self` for FtpReader.
unsafe impl Sync for FtpReader {}

impl FtpReader {
    pub async fn new(
        mut ftp_stream: PooledConnection<'static, Manager>,
        path: String,
        args: OpRead,
    ) -> Result<Self> {
        let (offset, size) = (
            args.range().offset(),
            args.range().size().unwrap_or(u64::MAX),
        );
        if offset != 0 {
            ftp_stream
                .resume_transfer(offset as usize)
                .await
                .map_err(parse_error)?;
        }
        let ds = ftp_stream
            .retr_as_stream(path)
            .await
            .map_err(parse_error)?
            .take(size as _);

        Ok(Self {
            _ftp_stream: ftp_stream,

            data_stream: Box::new(ds),
            chunk: 1024 * 1024,
            buf: BytesMut::new(),
        })
    }
}

impl oio::Read for FtpReader {
    async fn read(&mut self) -> Result<Buffer> {
        self.buf.resize(self.chunk, 0);
        let n = self
            .data_stream
            .read(&mut self.buf)
            .await
            .map_err(new_std_io_error)?;
        Ok(Buffer::from(self.buf.split_to(n).freeze()))
    }
}
