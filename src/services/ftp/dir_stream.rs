// Copyright 2022 Datafuse Labs
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

use std::str;
use std::str::FromStr;

use async_trait::async_trait;
use suppaftp::list::File;
use time::OffsetDateTime;

use crate::raw::*;
use crate::*;

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
            Err(e) => {
                Err(Error::new(ErrorKind::Unexpected, "parse file from response").set_source(e))
            }
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
    path: String,
    size: usize,
    rd: ReadDir,
}

impl DirStream {
    pub fn new(path: &str, rd: ReadDir, limit: Option<usize>) -> Self {
        Self {
            path: path.to_string(),
            size: limit.unwrap_or(1000),
            rd,
        }
    }
}

#[async_trait]
impl output::Page for DirStream {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        let mut oes: Vec<output::Entry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next() {
                Some(de) => de?,
                None => break,
            };

            let path = self.path.to_string() + de.name();

            let d = if de.is_file() {
                output::Entry::new(
                    &path,
                    ObjectMetadata::new(ObjectMode::FILE)
                        .with_content_length(de.size() as u64)
                        .with_last_modified(OffsetDateTime::from(de.modified())),
                )
            } else if de.is_directory() {
                output::Entry::new(&format!("{}/", &path), ObjectMetadata::new(ObjectMode::DIR))
            } else {
                output::Entry::new(&path, ObjectMetadata::new(ObjectMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
