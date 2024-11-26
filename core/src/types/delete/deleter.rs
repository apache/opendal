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

use crate::raw::oio::Delete;
use crate::raw::*;
use crate::*;
use futures::{Stream, StreamExt};
use std::future::IntoFuture;
use std::pin::pin;

pub struct Deleter {
    deleter: oio::Deleter,

    max_size: usize,
    cur_size: usize,
    closed: bool,
}

impl Deleter {
    pub(crate) async fn create(acc: Accessor) -> Result<Self> {
        let max_size = acc.info().full_capability().delete_max_size.unwrap_or(1);
        let (_, deleter) = acc.delete().await?;

        Ok(Self {
            deleter,
            max_size,
            cur_size: 0,
            closed: false,
        })
    }

    /// Delete a path.
    pub async fn delete(&mut self, input: impl IntoDeleteInput) -> Result<()> {
        if self.cur_size >= self.max_size {
            let deleted = self.deleter.flush().await?;
            self.cur_size -= deleted;
        }

        let input = input.into_delete_input();
        let mut op = OpDelete::default();
        if let Some(version) = &input.version {
            op = op.with_version(version);
        }

        self.deleter.delete(&input.path, op)?;
        self.cur_size += 1;
        Ok(())
    }

    /// Delete a stream of paths.
    pub async fn delete_from<S, E>(&mut self, mut stream: S) -> Result<()>
    where
        S: Stream<Item = Result<E>>,
        E: IntoDeleteInput,
    {
        let mut stream = pin!(stream);
        loop {
            match stream.next().await {
                Some(Ok(entry)) => {
                    self.delete(entry).await?;
                }
                Some(Err(err)) => return Err(err),
                None => break,
            }
        }

        Ok(())
    }

    /// Flush the deleter, returns the number of deleted paths.
    pub async fn flush(&mut self) -> Result<usize> {
        let deleted = self.deleter.flush().await?;
        self.cur_size -= deleted;
        Ok(deleted)
    }

    /// Close the deleter, this will flush the deleter and wait until all paths are deleted.
    pub async fn close(&mut self) -> Result<()> {
        loop {
            self.deleter.flush().await?;
            if self.cur_size == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Convert the deleter into a sink.
    pub fn into_sink(self) -> FuturesDeleteSink {
        FuturesDeleteSink::new(self)
    }
}

/// DeleteInput is the input for delete operations.
#[non_exhaustive]
#[derive(Default, Debug)]
pub struct DeleteInput {
    /// The path of the path to delete.
    pub path: String,
    /// The version of the path to delete.
    pub version: Option<String>,
}

/// IntoDeleteInput is a helper trait that makes it easier for users to play with `Deleter`.
pub trait IntoDeleteInput {
    /// Convert `self` into a `DeleteInput`.
    fn into_delete_input(self) -> DeleteInput;
}

/// Implement `IntoDeleteInput` for `Entry` so we can use `Vec<String>` as a DeleteInput stream.
impl IntoDeleteInput for String {
    fn into_delete_input(self) -> DeleteInput {
        DeleteInput {
            path: self,
            ..Default::default()
        }
    }
}

/// Implement `IntoDeleteInput` for `(String, OpDelete)` so we can use `(String, OpDelete)`
/// as a DeleteInput stream.
impl IntoDeleteInput for (String, OpDelete) {
    fn into_delete_input(self) -> DeleteInput {
        let (path, args) = self;

        let mut input = DeleteInput {
            path,
            ..Default::default()
        };

        if let Some(version) = args.version() {
            input.version = Some(version.to_string());
        }
        input
    }
}

/// Implement `IntoDeleteInput` for `Entry` so we can use `Lister` as a DeleteInput stream.
impl IntoDeleteInput for Entry {
    fn into_delete_input(self) -> DeleteInput {
        let (path, mut meta) = self.into_parts();

        let mut input = DeleteInput {
            path,
            ..Default::default()
        };

        if let Some(version) = meta.version() {
            input.version = Some(version.to_string());
        }
        input
    }
}
