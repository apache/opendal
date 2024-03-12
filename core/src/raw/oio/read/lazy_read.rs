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

use std::io::SeekFrom;
use std::sync::Arc;

use bytes::Bytes;

use crate::raw::*;
use crate::*;

/// LazyReader implements [`oio::Read`] in a lazy way.
///
/// The real requests are send when users calling read or seek.
pub struct LazyReader<A: Accessor, R> {
    acc: Arc<A>,
    path: Arc<String>,
    op: OpRead,
    reader: Option<R>,
}

impl<A, R> LazyReader<A, R>
where
    A: Accessor,
{
    /// Create a new [`oio::Reader`] with lazy support.
    pub fn new(acc: Arc<A>, path: &str, op: OpRead) -> LazyReader<A, R> {
        LazyReader {
            acc,
            path: Arc::new(path.to_string()),
            op,

            reader: None,
        }
    }
}

impl<A, R> LazyReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    async fn reader(&mut self) -> Result<&mut R> {
        if self.reader.is_none() {
            let (_, r) = self.acc.read(&self.path, self.op.clone()).await?;
            self.reader = Some(r);
        }

        Ok(self.reader.as_mut().expect("reader must be valid"))
    }
}

impl<A, R> oio::Read for LazyReader<A, R>
where
    A: Accessor<Reader = R>,
    R: oio::Read,
{
    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.reader().await?.seek(pos).await
    }

    async fn read(&mut self, size: usize) -> Result<Bytes> {
        let r = self.reader().await?;
        r.read(size).await
    }
}

impl<A, R> LazyReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn blocking_reader(&mut self) -> Result<&mut R> {
        if self.reader.is_none() {
            let (_, r) = self.acc.blocking_read(&self.path, self.op.clone())?;
            self.reader = Some(r);
        }

        Ok(self.reader.as_mut().expect("reader must be valid"))
    }
}

impl<A, R> oio::BlockingRead for LazyReader<A, R>
where
    A: Accessor<BlockingReader = R>,
    R: oio::BlockingRead,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.blocking_reader()?.read(buf)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.blocking_reader()?.seek(pos)
    }

    fn next(&mut self) -> Option<Result<Bytes>> {
        let r = match self.blocking_reader() {
            Ok(r) => r,
            Err(err) => return Some(Err(err)),
        };

        r.next()
    }
}
