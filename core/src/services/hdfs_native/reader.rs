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

use bytes::Bytes;
use futures::StreamExt;
use hdfs_native::file::FileReader;
use hdfs_native::HdfsError;

use crate::raw::*;
use crate::services::hdfs_native::error::parse_hdfs_error;
use crate::*;

pub struct HdfsNativeReader {
    read: usize,
    size: usize,
    stream: futures::stream::BoxStream<'static, Result<Bytes, HdfsError>>,
}

unsafe impl Sync for HdfsNativeReader {}

impl HdfsNativeReader {
    pub fn new(f: FileReader, offset: usize, size: usize) -> Self {
        let size = size.min(f.file_length() - offset);
        HdfsNativeReader {
            read: 0,
            size,
            stream: Box::pin(f.read_range_stream(offset, size)),
        }
    }
}

impl oio::Read for HdfsNativeReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        if let Some(bytes) = self.stream.as_mut().next().await {
            let bytes = bytes.map_err(parse_hdfs_error)?;
            let buf = Buffer::from(bytes);
            self.read += buf.len();

            Ok(buf)
        } else {
            Ok(Buffer::new())
        }
    }
}
