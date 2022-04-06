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
use crate::error::other;
use crate::error::ObjectError;
use crate::Accessor;
use crate::Object;
use crate::ObjectMode;

pub struct Readdir {
    acc: Arc<dyn Accessor>,
    root: String,
    path: String,

    rd: std::fs::ReadDir,
}

impl Readdir {
    pub fn new(acc: Arc<dyn Accessor>, root: &str, path: &str, rd: std::fs::ReadDir) -> Self {
        Self {
            acc,
            root: root.to_string(),
            path: path.to_string(),
            rd,
        }
    }
}

impl futures::Stream for Readdir {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rd.next() {
            None => {
                debug!("object {} list done", &self.path);
                Poll::Ready(None)
            }
            Some(Err(e)) => {
                error!("object {} stream poll_next: {:?}", &self.path, e);
                Poll::Ready(Some(Err(parse_io_error(e, "list", &self.path))))
            }
            Some(Ok(de)) => {
                let de_path = de.path();
                let de_path = de_path.strip_prefix(&self.root).map_err(|e| {
                    let e = other(ObjectError::new("list", &de.path().to_string_lossy(), e));
                    error!("object {:?} path strip_prefix: {:?}", &de.path(), e);
                    e
                })?;
                let path = de_path.to_string_lossy();

                let mut o = Object::new(self.acc.clone(), &path);

                // On Windows and most Unix platforms this function is free
                // (no extra system calls needed), but some Unix platforms may
                // require the equivalent call to symlink_metadata to learn about
                // the target file type.
                let de = de.file_type()?;

                let meta = o.metadata_mut();
                if de.is_file() {
                    meta.set_mode(ObjectMode::FILE);
                    meta.set_path(&path);
                } else if de.is_dir() {
                    // Make sure we are returning the correct path.
                    meta.set_path(&format!("{}/", &path));
                    meta.set_mode(ObjectMode::DIR);
                } else {
                    meta.set_path(&path);
                    meta.set_mode(ObjectMode::Unknown);
                }

                debug!("object {} got entry, path: {}", &self.path, meta.path());
                Poll::Ready(Some(Ok(o)))
            }
        }
    }
}
