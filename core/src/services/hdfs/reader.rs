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

use std::sync::Arc;
use bytes::BufMut;

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
    async fn read_at(&self, mut buf: oio::WritableBuf, offset: u64) -> (oio::WritableBuf, Result<usize>)  {
        let r = Self { f: self.f.clone() };

      let res =  match tokio::runtime::Handle::try_current() {
            Ok(runtime) => runtime
                .spawn_blocking(move || oio::BlockingRead::read_at(&r, buf, offset))
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

impl oio::BlockingRead for HdfsReader {
    fn read_at(&self,  mut buf: oio::WritableBuf, offset: u64) -> (oio::WritableBuf, Result<usize>)  {
       let res = self.f
            .read_at(buf.as_slice(), offset).map(|n|
                                                                      {// SAFETY: hdrs guarantees that the buffer is filled with n bytes.
           unsafe { buf.advance_mut(n); };n}).map_err(new_std_io_error);

        (buf, res)
    }
}
