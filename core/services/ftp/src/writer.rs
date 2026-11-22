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

/// How far `close` progressed on this writer.
///
/// Retry safety: `CompleteWriter` keeps the inner writer on error and
/// `RetryLayer` may call `close` again after a temporary failure. FTP cannot
/// tell apart partial and complete staging content afterwards, so every
/// remote-mutating step must run at most once:
///
/// - `Transfer` is the only state that may open or consume the data stream.
/// - Consuming the stream to finalize it leaves `Transfer` for good. If the
///   transfer then fails or its outcome is unknown, the state becomes
///   `Failed`, which fails `close` permanently instead of re-opening `APPE`
///   or renaming a possibly partial staging file over the target.
/// - `Rename` and `Report` no longer touch the data stream and are safe to
///   retry: the staging file already holds the final content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CloseState {
    /// The data stream may still be opened and written to.
    Transfer,
    /// The stream was consumed and finalized; a staged tmp file may still
    /// need the rename onto the target path.
    Rename,
    /// The remote file is committed; only metadata reporting remains.
    Report,
    /// The transfer was consumed but its outcome failed or is unknown, so
    /// the remote file may hold partial data. Terminal: `close` fails
    /// permanently without mutating the remote file again.
    Failed,
    /// All steps completed; `close` returns the cached metadata.
    Done,
}

impl CloseState {
    /// The data stream may only be opened while the writer is still in
    /// `Transfer`. Once the stream was consumed the remote content is out of
    /// our control and the stream must never be re-opened.
    fn may_open_data_stream(self) -> bool {
        self == CloseState::Transfer
    }
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
    /// Metadata reported once the writer reached `CloseState::Done`.
    result: Option<Metadata>,
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
            result: None,
            state: CloseState::Transfer,
        }
    }

    /// Open the data stream on first use so that empty writes and appends
    /// still create the remote file once the stream is finalized.
    async fn data_stream(
        &mut self,
    ) -> Result<&mut Box<dyn AsyncWrite + Sync + Send + Unpin + 'static>> {
        if self.data_stream.is_none() {
            if !self.state.may_open_data_stream() {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "ftp data stream is already finalized",
                )
                .with_operation("FtpWriter::data_stream"));
            }
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
        // The transfer was already consumed and its outcome failed or is
        // unknown; the remote file may hold partial data. Fail permanently
        // so a retried close can never commit it.
        if self.state == CloseState::Failed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "ftp write transfer already failed; the remote file may hold partial data",
            )
            .with_operation("FtpWriter::close"));
        }

        if self.state == CloseState::Transfer {
            // Opening the stream covers closing without any write. A failed
            // open is the only step a retry may repeat, since the stream was
            // never consumed at this point.
            self.data_stream().await?;
            // Consuming the stream is the point of no return: whatever
            // happens next, close must never re-open it.
            self.state = CloseState::Rename;
            // Take the stream before flushing so a failed flush drops the
            // data connection instead of leaving it open.
            let mut data_stream = self
                .data_stream
                .take()
                .expect("ftp data stream must be initialized");
            data_stream.flush().await.map_err(|err| {
                self.state = CloseState::Failed;
                Error::new(ErrorKind::Unexpected, "flush data stream failed").set_source(err)
            })?;
            self.ftp_stream
                .finalize_put_stream(data_stream)
                .await
                .map_err(|err| {
                    self.state = CloseState::Failed;
                    format_ftp_error(err)
                })?;
        }

        if self.state == CloseState::Rename {
            if let Some(tmp_path) = &self.tmp_path {
                self.ftp_stream
                    .rename(tmp_path, &self.target_path)
                    .await
                    .map_err(format_ftp_error)?;
                self.tmp_path = None;
            }
            self.state = CloseState::Report;
        }

        if self.state == CloseState::Report {
            // An append's final size includes pre-existing content, so query
            // it from the server; a regular write reports the bytes sent
            // through this stream, which also works on servers without
            // RFC 3659 SIZE.
            let metadata = if self.append {
                let size = self
                    .ftp_stream
                    .size(&self.target_path)
                    .await
                    .map_err(format_ftp_error)?;
                MetadataBuilder::file(size as u64).build()
            } else {
                MetadataBuilder::file(self.written).build()
            };
            self.result = Some(metadata);
            self.state = CloseState::Done;
        }

        debug_assert_eq!(self.state, CloseState::Done);
        Ok(self
            .result
            .clone()
            .expect("ftp writer must have reported metadata"))
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
    use super::*;

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

    /// Once the data stream was consumed, the remote content is out of our
    /// control: the stream must never be re-opened, and a failed or finished
    /// close must not be able to mutate the remote file again.
    #[test]
    fn only_transfer_may_open_data_stream() {
        assert!(CloseState::Transfer.may_open_data_stream());
        assert!(!CloseState::Rename.may_open_data_stream());
        assert!(!CloseState::Report.may_open_data_stream());
        assert!(!CloseState::Failed.may_open_data_stream());
        assert!(!CloseState::Done.may_open_data_stream());
    }
}
