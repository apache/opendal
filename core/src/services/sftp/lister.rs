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

use std::pin::Pin;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use futures::StreamExt;
use openssh_sftp_client::fs::DirEntry;
use openssh_sftp_client::fs::ReadDir;

use crate::raw::oio;
use crate::Result;

use super::error::parse_sftp_error;

pub struct SftpLister {
    dir: Pin<Box<ReadDir>>,
    prefix: String,
}

impl SftpLister {
    pub fn new(dir: ReadDir, path: String) -> Self {
        let prefix = if path == "/" { "".to_owned() } else { path };

        SftpLister {
            dir: Box::pin(dir),
            prefix,
        }
    }
}

#[async_trait]
impl oio::List for SftpLister {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        let item = ready!(self.dir.poll_next_unpin(cx))
            .transpose()
            .map_err(parse_sftp_error)?;

        match item {
            Some(e) => {
                if e.filename().to_str() == Some(".") || e.filename().to_str() == Some("..") {
                    self.poll_next(cx)
                } else {
                    Poll::Ready(Ok(Some(map_entry(self.prefix.as_str(), e))))
                }
            }
            None => Poll::Ready(Ok(None)),
        }
    }
}

fn map_entry(prefix: &str, value: DirEntry) -> oio::Entry {
    let path = format!(
        "{}{}{}",
        prefix,
        value.filename().to_str().unwrap(),
        if value.file_type().unwrap().is_dir() {
            "/"
        } else {
            ""
        }
    );

    oio::Entry::new(path.as_str(), value.metadata().into())
}
