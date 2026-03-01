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

use opendal_core::raw::*;
use opendal_core::*;

/// Reader for Git blob content
pub struct GitReader {
    content: Bytes,
    offset: u64,
}

impl GitReader {
    /// Create a new GitReader from bytes
    pub fn new(content: Bytes) -> Self {
        Self { content, offset: 0 }
    }
}

impl oio::Read for GitReader {
    async fn read(&mut self) -> Result<Buffer> {
        if self.offset >= self.content.len() as u64 {
            return Ok(Buffer::new());
        }

        let start = self.offset as usize;
        let chunk = self.content.slice(start..);
        self.offset = self.content.len() as u64;

        Ok(Buffer::from(chunk))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::raw::oio::Read;

    #[tokio::test]
    async fn test_reader_empty() {
        let mut reader = GitReader::new(Bytes::new());
        let buf = reader.read().await.unwrap();
        assert_eq!(buf.len(), 0);
    }

    #[tokio::test]
    async fn test_reader_with_content() {
        let content = Bytes::from("Hello, world!");
        let mut reader = GitReader::new(content);

        let buf = reader.read().await.unwrap();
        assert_eq!(buf.len(), 13);
        assert_eq!(&buf.to_bytes()[..], b"Hello, world!");

        // Second read should be empty
        let buf = reader.read().await.unwrap();
        assert_eq!(buf.len(), 0);
    }

    #[tokio::test]
    async fn test_reader_large_content() {
        let content = Bytes::from(vec![0u8; 1024 * 1024]); // 1MB
        let mut reader = GitReader::new(content);

        let buf = reader.read().await.unwrap();
        assert_eq!(buf.len(), 1024 * 1024);
    }
}
