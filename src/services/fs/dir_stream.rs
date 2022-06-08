// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use log::debug;
use log::error;

use super::error::parse_io_error;
use super::Backend;
use crate::error::other;
use crate::error::ObjectError;
use crate::Object;
use crate::ObjectMode;
use crate::{Accessor, DirEntry};

pub struct DirStream {
    backend: Arc<Backend>,
    path: String,

    rd: std::fs::ReadDir,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, path: &str, rd: std::fs::ReadDir) -> Self {
        Self {
            backend,
            path: path.to_string(),
            rd,
        }
    }
}

impl futures::Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rd.next() {
            None => {
                debug!("dir object {} list done", &self.path);
                Poll::Ready(None)
            }
            Some(Err(e)) => {
                error!("dir object {} list: {:?}", &self.path, e);
                Poll::Ready(Some(Err(parse_io_error(e, "list", &self.path))))
            }
            Some(Ok(de)) => {
                let path = self.backend.get_rel_path(&de.path().to_string_lossy());

                // On Windows and most Unix platforms this function is free
                // (no extra system calls needed), but some Unix platforms may
                // require the equivalent call to symlink_metadata to learn about
                // the target file type.
                let file_type = de.file_type()?;

                let d = if file_type.is_file() {
                    DirEntry::new(self.backend.clone(), ObjectMode::FILE, &path)
                } else if file_type.is_dir() {
                    // Make sure we are returning the correct path.
                    DirEntry::new(
                        self.backend.clone(),
                        ObjectMode::DIR,
                        &format!("{}/", &path),
                    )
                } else {
                    DirEntry::new(self.backend.clone(), ObjectMode::Unknown, &path)
                };

                debug!(
                    "dir object {} got entry, mode: {}, path: {}",
                    &self.path,
                    d.mode(),
                    d.path()
                );
                Poll::Ready(Some(Ok(d)))
            }
        }
    }
}
