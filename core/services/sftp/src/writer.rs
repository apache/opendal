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

use bytes::Buf;

use super::core::PIPELINE_DEPTH;
use super::core::SftpSessionRef;
use super::core::close_handle_detached;
use super::core::parse_sftp_error;
use opendal_core::raw::*;
use opendal_core::*;

/// Writes a remote file by streaming `Buffer`s into an open SFTP handle.
///
/// Writes are split into packets no larger than the size the server advertises
/// and several packets are kept in flight, so throughput does not collapse to
/// one round trip per packet.
pub struct SftpWriter {
    /// Keeps the session alive while the remote handle is open.
    conn: SftpSessionRef,
    handle: String,
    offset: u64,
    closed: bool,
}

impl Drop for SftpWriter {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        // An aborted write never calls `close`, so release the handle here.
        close_handle_detached(self.conn.session.clone(), std::mem::take(&mut self.handle));
    }
}

impl SftpWriter {
    pub fn new(conn: SftpSessionRef, handle: String, offset: u64) -> Self {
        SftpWriter {
            conn,
            handle,
            offset,
            closed: false,
        }
    }
}

impl oio::Write for SftpWriter {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        let chunk = self.conn.write_len as usize;
        let mut inflight = Vec::with_capacity(PIPELINE_DEPTH);

        while bs.has_remaining() {
            let piece = bs.chunk();
            let take = piece.len().min(chunk);
            let data = piece[..take].to_vec();
            bs.advance(take);

            let offset = self.offset;
            self.offset += take as u64;

            let session = self.conn.session.clone();
            let handle = self.handle.clone();
            inflight.push(async move { session.write(handle, offset, data).await });

            if inflight.len() >= PIPELINE_DEPTH {
                futures::future::try_join_all(std::mem::take(&mut inflight))
                    .await
                    .map_err(parse_sftp_error)?;
            }
        }

        if !inflight.is_empty() {
            futures::future::try_join_all(inflight)
                .await
                .map_err(parse_sftp_error)?;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        if !self.closed {
            self.conn
                .session
                .close(self.handle.as_str())
                .await
                .map_err(parse_sftp_error)?;
            self.closed = true;
        }

        Ok(Metadata::default())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "SftpWriter doesn't support abort",
        ))
    }
}
