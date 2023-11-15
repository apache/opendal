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

use std::fs::FileType;
use std::path::Path;
use std::path::PathBuf;
use std::task::{ready, Context, Poll};

use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::FutureExt;

use crate::raw::*;
use crate::EntryMode;
use crate::Metadata;
use crate::Result;

pub struct FsLister<P> {
    root: PathBuf,

    size: usize,
    rd: P,

    fut: Option<BoxFuture<'static, (tokio::fs::DirEntry, Result<FileType>)>>,
}

impl<P> FsLister<P> {
    pub fn new(root: &Path, rd: P, limit: Option<usize>) -> Self {
        Self {
            root: root.to_owned(),
            size: limit.unwrap_or(1000),
            rd,

            fut: None,
        }
    }
}

/// # Safety
///
/// We will only take `&mut Self` reference for FsLister.
unsafe impl<P> Sync for FsLister<P> {}

#[async_trait]
impl oio::List for FsLister<tokio::fs::ReadDir> {
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<oio::Entry>>> {
        if let Some(fut) = self.fut.as_mut() {
            let (de, ft) = futures::ready!(fut.poll_unpin(cx));
            let ft = match ft {
                Ok(ft) => {
                    self.fut = None;
                    ft
                }
                Err(e) => {
                    let fut = async move {
                        let ft = de.file_type().await.map_err(new_std_io_error);
                        (de, ft)
                    };
                    self.fut = Some(Box::pin(fut));
                    return Poll::Ready(Err(e));
                }
            };

            let entry_path = de.path();
            let rel_path = normalize_path(
                &entry_path
                    .strip_prefix(&self.root)
                    .expect("cannot fail because the prefix is iterated")
                    .to_string_lossy()
                    .replace('\\', "/"),
            );

            let d = if ft.is_file() {
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
            } else if ft.is_dir() {
                // Make sure we are returning the correct path.
                oio::Entry::new(&format!("{rel_path}/"), Metadata::new(EntryMode::DIR))
            } else {
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
            };

            return Poll::Ready(Ok(Some(d)));
        }

        let de = ready!(self.rd.poll_next_entry(cx)).map_err(new_std_io_error)?;
        match de {
            Some(de) => {
                let fut = async move {
                    let ft = de.file_type().await.map_err(new_std_io_error);
                    (de, ft)
                };
                self.fut = Some(Box::pin(fut));
                self.poll_next(cx)
            }
            None => Poll::Ready(Ok(None)),
        }
    }
}

impl oio::BlockingList for FsLister<std::fs::ReadDir> {
    fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        let mut oes: Vec<oio::Entry> = Vec::with_capacity(self.size);

        for _ in 0..self.size {
            let de = match self.rd.next() {
                Some(de) => de.map_err(new_std_io_error)?,
                None => break,
            };

            let entry_path = de.path();
            let rel_path = normalize_path(
                &entry_path
                    .strip_prefix(&self.root)
                    .expect("cannot fail because the prefix is iterated")
                    .to_string_lossy()
                    .replace('\\', "/"),
            );

            // On Windows and most Unix platforms this function is free
            // (no extra system calls needed), but some Unix platforms may
            // require the equivalent call to symlink_metadata to learn about
            // the target file type.
            let file_type = de.file_type().map_err(new_std_io_error)?;

            let d = if file_type.is_file() {
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::FILE))
            } else if file_type.is_dir() {
                // Make sure we are returning the correct path.
                oio::Entry::new(&format!("{rel_path}/"), Metadata::new(EntryMode::DIR))
            } else {
                oio::Entry::new(&rel_path, Metadata::new(EntryMode::Unknown))
            };

            oes.push(d)
        }

        Ok(if oes.is_empty() { None } else { Some(oes) })
    }
}
