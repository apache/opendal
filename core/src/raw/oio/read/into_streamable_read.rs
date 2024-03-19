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

use std::cmp::min;
use std::io::SeekFrom;

use bytes::Bytes;
use tokio::io::ReadBuf;

use crate::raw::*;
use crate::*;

/// into_streamable is used to make [`oio::Read`] or [`oio::BlockingRead`] streamable.
pub fn into_streamable_read<R>(r: R, capacity: usize) -> StreamableReader<R> {
    StreamableReader {
        r,
        buf: Vec::with_capacity(capacity),
    }
}

/// Make given read streamable.
pub struct StreamableReader<R> {
    r: R,
    buf: Vec<u8>,
}

impl<R: oio::Read> oio::Read for StreamableReader<R> {
    async fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.r.seek(pos).await
    }

    async fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        let size = min(self.buf.capacity(), limit);

        let dst = self.buf.spare_capacity_mut();
        let mut buf = ReadBuf::uninit(dst);

        // SAFETY: Read at most `size` bytes into `read_buf`.
        unsafe { buf.assume_init(size) };

        let bs = self.r.read(size).await?;
        buf.put_slice(&bs);
        buf.set_filled(bs.len());

        Ok(Bytes::from(buf.filled().to_vec()))
    }
}

impl<R: oio::BlockingRead> oio::BlockingRead for StreamableReader<R> {
    fn read_at(&self, offset: u64, limit: usize) -> Result<oio::Buffer> {
        self.r.read(limit)
    }

    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        self.r.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use crate::raw::oio::Read;
    use bytes::BufMut;
    use bytes::BytesMut;
    use rand::prelude::*;

    use super::*;

    #[tokio::test]
    async fn test_into_stream() {
        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        // Generate cap between 1B..1MB;
        let cap = rng.gen_range(1..1024 * 1024);

        let r = oio::Cursor::from(content.clone());
        let mut s = into_streamable_read(Box::new(r) as oio::Reader, cap);

        let mut bs = BytesMut::new();
        loop {
            let b = s.read(4 * 1024 * 1024).await.expect("read must success");
            if b.is_empty() {
                break;
            }
            bs.put_slice(&b);
        }
        assert_eq!(bs.freeze().to_vec(), content)
    }

    #[test]
    fn test_into_stream_blocking() {
        use oio::BlockingRead;

        let mut rng = ThreadRng::default();
        // Generate size between 1B..16MB.
        let size = rng.gen_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        // Generate cap between 1B..1MB;
        let cap = rng.gen_range(1..1024 * 1024);

        let r = oio::Cursor::from(content.clone());
        let mut s = into_streamable_read(Box::new(r) as oio::BlockingReader, cap);

        let mut bs = BytesMut::with_capacity(size);
        loop {
            let buf = s.read(size).expect("read must success");
            if buf.is_empty() {
                break;
            }
            bs.put_slice(&buf)
        }
        assert_eq!(bs.freeze().to_vec(), content)
    }
}
