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

use super::Backend;
use crate::path::build_rel_path;
use crate::DirEntry;
use crate::ObjectMode;

pub struct DirStream {
    backend: Arc<Backend>,
    root: String,
    rd: hdrs::Readdir,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, root: &str, rd: hdrs::Readdir) -> Self {
        Self {
            backend,
            root: root.to_string(),
            rd,
        }
    }
}

impl futures::Stream for DirStream {
    type Item = Result<DirEntry>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rd.next() {
            None => Poll::Ready(None),
            Some(de) => {
                let path = build_rel_path(&self.root, de.path());

                let mut d = if de.is_file() {
                    DirEntry::new(self.backend.clone(), ObjectMode::FILE, &path)
                } else if de.is_dir() {
                    // Make sure we are returning the correct path.
                    DirEntry::new(self.backend.clone(), ObjectMode::DIR, &format!("{}/", path))
                } else {
                    DirEntry::new(self.backend.clone(), ObjectMode::Unknown, &path)
                };

                // set metadata fields of `DirEntry`
                d.set_content_length(de.len());
                d.set_last_modified(time::OffsetDateTime::from(de.modified()));

                Poll::Ready(Some(Ok(d)))
            }
        }
    }
}
