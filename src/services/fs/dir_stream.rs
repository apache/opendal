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

use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;

use super::backend::Backend;
use super::error::parse_io_error;
use crate::object::ObjectPage;
use crate::path::build_rel_path;
use crate::ObjectEntry;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Result;

pub struct DirStream {
    backend: Arc<Backend>,
    root: String,

    rd: std::fs::ReadDir,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, root: &str, rd: std::fs::ReadDir) -> Self {
        Self {
            backend,
            root: root.to_string(),
            rd,
        }
    }
}

#[async_trait]
impl ObjectPage for DirStream {
    async fn next_page(&mut self) -> Result<Option<Vec<ObjectEntry>>> {
        todo!()
    }
}

impl futures::Stream for DirStream {
    type Item = Result<ObjectEntry>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rd.next() {
            None => Poll::Ready(None),
            Some(Err(e)) => Poll::Ready(Some(Err(parse_io_error(e)))),
            Some(Ok(de)) => {
                let path = build_rel_path(&self.root, &de.path().to_string_lossy());

                // On Windows and most Unix platforms this function is free
                // (no extra system calls needed), but some Unix platforms may
                // require the equivalent call to symlink_metadata to learn about
                // the target file type.
                let file_type = de.file_type().map_err(parse_io_error)?;

                let d = if file_type.is_file() {
                    ObjectEntry::new(&path, ObjectMetadata::new(ObjectMode::FILE))
                } else if file_type.is_dir() {
                    // Make sure we are returning the correct path.
                    ObjectEntry::new(
                        &format!("{}/", &path),
                        ObjectMetadata::new(ObjectMode::DIR).with_complete(),
                    )
                } else {
                    ObjectEntry::new(&path, ObjectMetadata::new(ObjectMode::Unknown))
                };

                Poll::Ready(Some(Ok(d)))
            }
        }
    }
}
