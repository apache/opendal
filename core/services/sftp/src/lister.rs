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

use std::collections::VecDeque;

use russh_sftp::protocol::{FileAttributes, FileMode};

use super::core::SftpSessionRef;
use super::core::close_handle_detached;
use super::core::is_eof;
use super::core::parse_sftp_error;
use super::core::to_metadata;
use opendal_core::Result;
use opendal_core::raw::oio;
use opendal_core::raw::oio::Entry;

/// Lists a remote directory one `SSH_FXP_READDIR` batch at a time.
///
/// Entries are yielded as they arrive rather than buffering the whole
/// directory, which keeps memory flat for large directories.
pub struct SftpLister {
    /// Keeps the session alive while the directory handle is open.
    conn: SftpSessionRef,
    handle: String,
    prefix: String,
    buffer: VecDeque<(String, FileAttributes)>,
    /// Whether the directory's own entry still has to be emitted.
    emit_self: bool,
    finished: bool,
}

impl Drop for SftpLister {
    fn drop(&mut self) {
        if self.finished {
            return;
        }

        // `list_with_limit` callers routinely stop before the directory ends,
        // so the handle has to be released here too.
        close_handle_detached(self.conn.session.clone(), std::mem::take(&mut self.handle));
    }
}

impl SftpLister {
    pub fn new(conn: SftpSessionRef, handle: String, path: String) -> Self {
        let prefix = if path == "/" { "".to_owned() } else { path };

        SftpLister {
            conn,
            handle,
            prefix,
            buffer: VecDeque::new(),
            emit_self: true,
            finished: false,
        }
    }

    /// Builds the entry for the directory being listed.
    ///
    /// The metadata comes from the open handle rather than a `.` entry:
    /// OpenSSH on Windows omits `.` and `..` from `readdir` altogether, so a
    /// listing that relied on them would silently drop the directory itself.
    async fn self_entry(&self) -> Entry {
        let attrs = match self.conn.session.fstat(self.handle.as_str()).await {
            Ok(attrs) => attrs.attrs,
            Err(_) => {
                let mut attrs = FileAttributes::default();
                attrs.set_type(FileMode::DIR);
                attrs
            }
        };

        let path = if self.prefix.is_empty() {
            "/"
        } else {
            self.prefix.as_str()
        };

        Entry::new(path, to_metadata(&attrs))
    }

    /// Pulls the next batch of entries, returning `false` once the server
    /// reports the end of the directory.
    async fn fill(&mut self) -> Result<bool> {
        if self.finished {
            return Ok(false);
        }

        match self.conn.session.readdir(self.handle.as_str()).await {
            Ok(name) => {
                self.buffer
                    .extend(name.files.into_iter().map(|f| (f.filename, f.attrs)));
                Ok(true)
            }
            Err(e) if is_eof(&e) => {
                self.finished = true;
                // Release the directory handle as soon as the listing ends.
                let _ = self
                    .conn
                    .session
                    .close(std::mem::take(&mut self.handle))
                    .await;
                Ok(false)
            }
            Err(e) => {
                self.finished = true;
                Err(parse_sftp_error(e))
            }
        }
    }
}

impl oio::List for SftpLister {
    async fn next(&mut self) -> Result<Option<Entry>> {
        if self.emit_self {
            self.emit_self = false;
            return Ok(Some(self.self_entry().await));
        }

        loop {
            let Some((filename, attrs)) = self.buffer.pop_front() else {
                if self.fill().await? {
                    continue;
                }
                return Ok(None);
            };

            // Emitted up front from the handle, so both are dropped here.
            if filename == "." || filename == ".." {
                continue;
            }

            let path = format!(
                "{}{}{}",
                self.prefix,
                filename,
                if attrs.file_type().is_dir() { "/" } else { "" }
            );

            return Ok(Some(Entry::new(path.as_str(), to_metadata(&attrs))));
        }
    }
}
