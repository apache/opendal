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

use std::str;
use std::str::FromStr;
use std::vec::IntoIter;

use async_trait::async_trait;
use suppaftp::list::File;
use time::OffsetDateTime;

use crate::raw::*;
use crate::*;

pub struct FtpPager {
    path: String,
    size: usize,
    file_iter: IntoIter<String>,
}

impl FtpPager {
    pub fn new(path: &str, files: Vec<String>, limit: Option<usize>) -> Self {
        Self {
            path: path.to_string(),
            size: limit.unwrap_or(1000),
            file_iter: files.into_iter(),
        }
    }
}

#[async_trait]
impl oio::Page for FtpPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut oes: Vec<oio::Entry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.file_iter.next() {
                Some(file_str) => File::from_str(file_str.as_str()).map_err(|e| {
                    Error::new(ErrorKind::Unexpected, "parse file from response").set_source(e)
                })?,
                None => break,
            };

            let path = self.path.to_string() + de.name();

            let d = if de.is_file() {
                oio::Entry::new(
                    &path,
                    Metadata::new(EntryMode::FILE)
                        .with_content_length(de.size() as u64)
                        .with_last_modified(OffsetDateTime::from(de.modified())),
                )
            } else if de.is_directory() {
                oio::Entry::new(&format!("{}/", &path), Metadata::new(EntryMode::DIR))
            } else {
                oio::Entry::new(&path, Metadata::new(EntryMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
