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

use bytes::BufMut;
use std::sync::Arc;

use hdrs::File;

use crate::raw::*;
use crate::*;

pub struct HdfsReader {
    f: Arc<File>,
}

impl HdfsReader {
    pub fn new(f: File) -> Self {
        Self { f: Arc::new(f) }
    }
}

impl oio::Read for HdfsReader {
    async fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        let r = Self { f: self.f.clone() };

        let mut tbuf = Vec::with_capacity(buf.remaining_mut());
        let (tbuf, res) = match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime
                .spawn_blocking(move || {
                    // tbuf has at least buf.remaining_mut() capacity
                    unsafe {
                        tbuf.set_len(tbuf.capacity());
                    }
                    let res = r.f.read_at(&mut tbuf, offset).map_err(new_std_io_error).map(|n| {
                        // Safety: we have read n bytes from the fs
                        unsafe { tbuf.set_len(n) };
                        n
                    });
                    (tbuf, res)
                })
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "tokio spawn io task failed").set_source(err)
                })?,
            Err(_) => return Err(Error::new(
                ErrorKind::Unexpected,
                "no tokio runtime found, failed to run io task",
            )),
        };

        res.map(|n| {
            buf.put(&*tbuf);
            n
        })
    }
}

impl oio::BlockingRead for HdfsReader {
    fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        self.f
            .read_at(buf.as_slice(), offset)
            .map(|n| {
                // SAFETY: hdrs guarantees that the buffer is filled with n bytes.
                unsafe {
                    buf.advance_mut(n);
                };
                n
            })
            .map_err(new_std_io_error)
    }
}
