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

use asyncband::pool::bounded;
use bytes::Buf;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;

use super::core::Manager;
use super::core::format_ftp_error;
use opendal_core::raw::*;
use opendal_core::*;

/// Tracks how far `FtpWriter::close` progressed so that a retry after a
/// temporary error never re-opens the data stream or re-commits a file that
/// already holds the final content.
#[derive(Debug, PartialEq, Eq)]
enum CloseState {
    /// The data stream may still be written to; nothing was finalized yet.
    Open,
    /// The data stream was flushed and finalized; the staged tmp file may
    /// still need the rename onto the target path.
    Finalized,
    /// The target path holds the final content; only metadata reporting
    /// remains.
    Committed,
}

pub struct FtpWriter {
    target_path: String,
    tmp_path: Option<String>,
    append: bool,
    /// Declared before `ftp_stream` so the data connection is always closed
    /// before the control connection returns to the pool.
    data_stream: Option<Box<dyn AsyncWrite + Sync + Send + Unpin + 'static>>,
    ftp_stream: bounded::Object<Manager>,
    /// Bytes actually sent over the data stream.
    written: u64,
    state: CloseState,
}

/// # Safety
///
/// We only have `&mut self` for FtpWrite.
unsafe impl Sync for FtpWriter {}

/// # TODO
///
/// Writer is not implemented correctly.
///
/// After we can use data stream, we should return it directly.
impl FtpWriter {
    pub fn new(ftp_stream: bounded::Object<Manager>, target_path: String, append: bool) -> Self {
        let tmp_path = (!append).then(|| build_ftp_tmp_path(&target_path));
        FtpWriter {
            target_path,
            tmp_path,
            append,
            data_stream: None,
            ftp_stream,
            written: 0,
            state: CloseState::Open,
        }
    }

    /// Open the data stream on first use so that empty writes and appends still
    /// create the remote file once the stream is finalized.
    async fn data_stream(
        &mut self,
    ) -> Result<&mut Box<dyn AsyncWrite + Sync + Send + Unpin + 'static>> {
        if self.data_stream.is_none() {
            let path = self.tmp_path.as_ref().unwrap_or(&self.target_path);
            self.data_stream = Some(Box::new(
                self.ftp_stream
                    .append_with_stream(path)
                    .await
                    .map_err(format_ftp_error)?,
            ));
        }

        Ok(self
            .data_stream
            .as_mut()
            .expect("ftp data stream must be initialized"))
    }
}

impl oio::Write for FtpWriter {
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        debug_assert_eq!(self.state, CloseState::Open);

        while bs.has_remaining() {
            let n = self
                .data_stream()
                .await?
                .write(bs.chunk())
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "copy from ftp stream").set_source(err)
                })?;
            self.written += n as u64;
            bs.advance(n);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        // Each step runs at most once and only when the previous steps
        // completed, so retrying close after a temporary error resumes
        // where it left off instead of re-opening the transfer.
        if self.state == CloseState::Open {
            // Open the stream even when nothing was written so that empty
            // writes and appends still create or touch the remote file.
            self.data_stream().await?;
            // Take the stream before flushing so a failed flush drops the
            // data connection instead of leaving it open.
            let mut data_stream = self
                .data_stream
                .take()
                .expect("ftp data stream must be initialized");
            data_stream.flush().await.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "flush data stream failed").set_source(err)
            })?;

            self.ftp_stream
                .finalize_put_stream(data_stream)
                .await
                .map_err(format_ftp_error)?;
            self.state = CloseState::Finalized;
        }

        if self.state == CloseState::Finalized {
            if let Some(tmp_path) = &self.tmp_path {
                self.ftp_stream
                    .rename(tmp_path, &self.target_path)
                    .await
                    .map_err(format_ftp_error)?;
                self.tmp_path = None;
            }
            self.state = CloseState::Committed;
        }

        debug_assert_eq!(self.state, CloseState::Committed);

        // An append's final size includes pre-existing content, so query it
        // from the server; a regular write reports the bytes written through
        // this stream, which also works on servers without RFC 3659 SIZE.
        if self.append {
            let size = self
                .ftp_stream
                .size(&self.target_path)
                .await
                .map_err(format_ftp_error)?;
            Ok(MetadataBuilder::file(size as u64).build())
        } else {
            Ok(MetadataBuilder::file(self.written).build())
        }
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "FtpWriter doesn't support abort",
        ))
    }
}

fn build_ftp_tmp_path(path: &str) -> String {
    let parent = get_parent(path);
    let tmp_name = build_tmp_path_of(path);

    if parent == "/" {
        tmp_name
    } else {
        format!("{parent}{tmp_name}")
    }
}

#[cfg(test)]
mod tests {
    use super::build_ftp_tmp_path;

    #[test]
    fn test_build_ftp_tmp_path() {
        for path in [
            "example.txt",
            "folder/example.txt",
            "folder/subfolder/deep/example.txt",
            ".hidden dir/.résumé #%.txt",
        ] {
            let tmp_path = build_ftp_tmp_path(path);
            let (stem, suffix) = tmp_path.rsplit_once('.').unwrap();
            assert_eq!(stem, path);
            assert!(!suffix.is_empty() && !suffix.contains('/'), "{tmp_path}");
        }
    }
}
