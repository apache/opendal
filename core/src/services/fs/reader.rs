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

use crate::raw::*;
use crate::*;
use bytes::BufMut;

pub struct FsReader {
    f: std::fs::File,
}

impl FsReader {
    pub fn new(f: std::fs::File) -> Self {
        Self { f }
    }

    fn try_clone(&self) -> Result<Self> {
        let f = self.f.try_clone().map_err(|err| {
            Error::new(
                ErrorKind::Unexpected,
                "tokio fs clone file description failed",
            )
            .set_source(err)
        })?;

        Ok(Self { f })
    }

    #[cfg(target_family = "unix")]
    pub fn read_at_inner(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::unix::fs::FileExt;
        self.f.read_at(buf, offset).map_err(new_std_io_error)
    }

    #[cfg(target_family = "windows")]
    pub fn read_at_inner(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        use std::os::windows::fs::FileExt;
        self.f.seek_read(buf, offset).map_err(new_std_io_error)
    }
}

impl oio::Read for FsReader {
    async fn read_at(
        &self,
        buf: oio::WritableBuf,
        offset: u64,
    ) -> (oio::WritableBuf, Result<usize>) {
        let handle = match self.try_clone() {
            Ok(handle) => handle,
            Err(err) => return (buf, Err(err)),
        };

        let res = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime
                .spawn_blocking(move || oio::BlockingRead::read_at(&handle, buf, offset))
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "tokio spawn io task failed").set_source(err)
                }),
            Err(_) => Err(Error::new(
                ErrorKind::Unexpected,
                "no tokio runtime found, failed to run io task",
            )),
        };

        match res {
            Ok((buf, res)) => (buf, res),
            Err(err) => (buf, Err(err)),
        }
    }
}

impl oio::BlockingRead for FsReader {
    fn read_at(&self, mut buf: oio::WritableBuf, offset: u64) -> (oio::WritableBuf, Result<usize>) {
        let res = self.read_at_inner(buf.as_slice(), offset).map(|n| {
            // Safety: we have read n bytes from the fs
            unsafe { buf.advance_mut(n) }
        });
        (buf, res)
    }
}
