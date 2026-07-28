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

use russh_sftp::protocol::FileAttributes;

use super::core::SftpSessionRef;
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
    finished: bool,
}

impl SftpLister {
    pub fn new(conn: SftpSessionRef, handle: String, path: String) -> Self {
        let prefix = if path == "/" { "".to_owned() } else { path };

        SftpLister {
            conn,
            handle,
            prefix,
            buffer: VecDeque::new(),
            finished: false,
        }
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
                let _ = self.conn.session.close(self.handle.as_str()).await;
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
        loop {
            let Some((filename, attrs)) = self.buffer.pop_front() else {
                if self.fill().await? {
                    continue;
                }
                return Ok(None);
            };

            if filename == ".." {
                continue;
            }

            if filename == "." {
                let path = if self.prefix.is_empty() {
                    "/"
                } else {
                    self.prefix.as_str()
                };
                return Ok(Some(Entry::new(path, to_metadata(&attrs))));
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
