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

use std::clone::Clone;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result;
use std::pin::Pin;
use std::str;
use std::str::FromStr;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use suppaftp::list::File;

use super::err::parse_io_error;
use super::Backend;
use crate::ops::Operation;
use crate::ObjectEntry;
use crate::ObjectMode;

pub struct ReadDir {
    files: Vec<String>,
    index: usize,
}

impl Iterator for ReadDir {
    type Item = Result<File>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.files.len() {
            return None;
        }
        self.index += 1;
        let result = match File::from_str(self.files[self.index - 1].as_str()) {
            Ok(f) => Ok(f),
            Err(e) => Err(Error::new(ErrorKind::InvalidData, e)),
        };

        Some(result)
    }
}

impl ReadDir {
    pub fn new(files: Vec<String>) -> ReadDir {
        ReadDir { files, index: 0 }
    }
}

pub struct DirStream {
    backend: Arc<Backend>,
    path: String,
    rd: ReadDir,
}

impl DirStream {
    pub fn new(backend: Arc<Backend>, path: &str, rd: ReadDir) -> Self {
        Self {
            backend,
            path: path.to_string(),
            rd,
        }
    }
}

impl futures::Stream for DirStream {
    type Item = Result<ObjectEntry>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rd.by_ref().next() {
            None => Poll::Ready(None),
            Some(Err(e)) => Poll::Ready(Some(Err(parse_io_error(e, Operation::List, &self.path)))),
            Some(Ok(de)) => {
                let path = self.path.to_string() + de.name();

                let d = if de.is_file() {
                    ObjectEntry::new(self.backend.clone(), ObjectMode::FILE, &path)
                } else if de.is_directory() {
                    ObjectEntry::new(
                        self.backend.clone(),
                        ObjectMode::DIR,
                        &format!("{}/", &path),
                    )
                } else {
                    ObjectEntry::new(self.backend.clone(), ObjectMode::Unknown, &path)
                };

                Poll::Ready(Some(Ok(d)))
            }
        }
    }
}
