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

use std::task::Context;
use std::task::Poll;

use hdfs_native::file::FileWriter;

use crate::raw::oio;
use crate::raw::oio::WriteBuf;
use crate::*;

pub struct HdfsNativeWriter {
    _f: FileWriter,
}

impl HdfsNativeWriter {
    pub fn new(f: FileWriter) -> Self {
        HdfsNativeWriter { _f: f }
    }
}

impl oio::Write for HdfsNativeWriter {
    fn poll_write(&mut self, _cx: &mut Context<'_>, _bs: &dyn WriteBuf) -> Poll<Result<usize>> {
        todo!()
    }

    fn poll_close(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        todo!()
    }

    fn poll_abort(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::Unsupported,
            "HdfsNativeWriter doesn't support abort",
        )))
    }
}
