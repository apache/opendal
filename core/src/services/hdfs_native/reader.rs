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

use futures::StreamExt;
use hdfs_native::file::FileReader;

use crate::raw::*;
use crate::services::hdfs_native::error::parse_hdfs_error;
use crate::*;

pub struct HdfsNativeReader {
    f: FileReader,
    offset: usize,
    read: usize,
    size: usize,
    buf_size: usize,
    buf: Buffer,
}

impl HdfsNativeReader {
    pub fn new(f: FileReader, offset: usize, size: usize) -> Self {
        HdfsNativeReader {
            f,
            offset,
            read: 0,
            size,
            // Use 2 MiB as default value.
            buf_size: 2 * 1024 * 1024,
            buf: Buffer::new(),
        }
    }
}

impl oio::Read for HdfsNativeReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.read >= self.size {
            return Ok(Buffer::new());
        }

        if self.buf.is_empty() {
            let size = (self.size - self.read)
                .min(self.buf_size)
                .min(self.f.file_length() - self.read);
            let mut stream = Box::pin(self.f.read_range_stream(self.offset, size));

            if let Some(bytes) = stream.next().await {
                let bytes = bytes.map_err(parse_hdfs_error)?;
                self.buf = Buffer::from(bytes);
                self.read += self.buf.len();
                self.offset += self.buf.len();
            } else {
                return Ok(Buffer::new());
            }
        }

        Ok(std::mem::take(&mut self.buf))
    }
}
