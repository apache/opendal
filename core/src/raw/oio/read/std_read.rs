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

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

use crate::raw::*;
use crate::*;

/// FuturesReader implements [`oio::BlockingRead`] via [`Read`] + [`Seek`].
pub struct StdReader<R: Read + Seek> {
    inner: R,
}

impl<R: Read + Seek> StdReader<R> {
    /// Create a new std reader.
    pub fn new(inner: R) -> Self {
        Self { inner }
    }
}

impl<R> oio::BlockingRead for StdReader<R>
where
    R: Read + Seek + Send + Sync,
{
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.inner.read(buf).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::BlockingRead)
                .with_context("source", "StdReader")
        })
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.inner.seek(pos).map_err(|err| {
            new_std_io_error(err)
                .with_operation(oio::ReadOperation::BlockingSeek)
                .with_context("source", "StdReader")
        })
    }
}
