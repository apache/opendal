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

use compio::buf::IoBuf;

use crate::Buffer;

unsafe impl IoBuf for Buffer {
    fn as_buf_ptr(&self) -> *const u8 {
        self.current().as_ptr()
    }

    fn buf_len(&self) -> usize {
        self.current().len()
    }

    fn buf_capacity(&self) -> usize {
        // `Bytes` doesn't expose uninitialized capacity, so treat it as the same as `len`
        self.current().len()
    }
}

// TODO: impl IoVectoredBuf for Buffer
// impl IoVectoredBuf for Buffer {
//     fn as_dyn_bufs(&self) -> impl Iterator<Item = &dyn IoBuf> {}
//
//     fn owned_iter(self) -> Result<OwnedIter<impl OwnedIterator<Inner = Self>>, Self> {
//         Ok(OwnedIter::new(BufferIter {
//             current: self.current(),
//             buf: self,
//         }))
//     }
// }

// #[derive(Debug, Clone)]
// struct BufferIter {
//     buf: Buffer,
//     current: Bytes,
// }

// impl IntoInner for BufferIter {
//     type Inner = Buffer;
//
//     fn into_inner(self) -> Self::Inner {
//         self.buf
//     }
// }

// impl OwnedIterator for BufferIter {
//     fn next(mut self) -> Result<Self, Self::Inner> {
//         let Some(current) = self.buf.next() else {
//             return Err(self.buf);
//         };
//         self.current = current;
//         Ok(self)
//     }
//
//     fn current(&self) -> &dyn IoBuf {
//         &self.current
//     }
// }

#[cfg(test)]
mod tests {
    use bytes::{Buf, Bytes};
    use rand::{thread_rng, Rng};

    use super::*;

    fn setup_buffer() -> (Buffer, usize, Bytes) {
        let mut rng = thread_rng();

        let bs = (0..100)
            .map(|_| {
                let len = rng.gen_range(1..100);
                let mut buf = vec![0; len];
                rng.fill(&mut buf[..]);
                Bytes::from(buf)
            })
            .collect::<Vec<_>>();

        let total_size = bs.iter().map(|b| b.len()).sum::<usize>();
        let total_content = bs.iter().flatten().copied().collect::<Bytes>();
        let buf = Buffer::from(bs);

        (buf, total_size, total_content)
    }

    #[test]
    fn test_io_buf() {
        let (buf, _len, _bytes) = setup_buffer();
        let slice = IoBuf::as_slice(&buf);

        assert_eq!(slice, buf.current().chunk())
    }
}
