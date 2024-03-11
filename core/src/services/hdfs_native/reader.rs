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

use bytes::Bytes;
use hdfs_native::file::FileReader;

use crate::raw::oio::Read;
use crate::*;

pub struct HdfsNativeReader {
    _f: FileReader,
}

impl HdfsNativeReader {
    pub fn new(f: FileReader) -> Self {
        HdfsNativeReader { _f: f }
    }
}

impl Read for HdfsNativeReader {
    async fn read(&mut self, size: usize) -> Result<Bytes> {
        let _ = size;

        todo!()
    }

    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let _ = pos;

        Err(Error::new(
            ErrorKind::Unsupported,
            "HdfsNativeReader doesn't support seeking",
        ))
    }
}
