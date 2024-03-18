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

use bytes::{BufMut, Bytes, BytesMut};
use std::collections::VecDeque;

pub struct Buffer(Inner);

enum Inner {
    Contiguous(Bytes),
    NonContiguous(VecDeque<Bytes>),
}

impl bytes::Buf for Buffer {
    #[inline]
    fn remaining(&self) -> usize {
        match &self.0 {
            Inner::Contiguous(b) => b.remaining(),
            Inner::NonContiguous(v) => v.iter().map(|b| b.remaining()).sum(),
        }
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        match &self.0 {
            Inner::Contiguous(b) => b.chunk(),
            Inner::NonContiguous(v) => {
                if let Some(b) = v.front() {
                    b.chunk()
                } else {
                    &[]
                }
            }
        }
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        match &mut self.0 {
            Inner::Contiguous(b) => b.advance(cnt),
            Inner::NonContiguous(v) => {
                let mut cnt = cnt;
                while cnt > 0 {
                    let b = &mut v[0];
                    if b.remaining() > cnt {
                        b.advance(cnt);
                        break;
                    } else {
                        cnt -= b.remaining();
                        v.remove(0);
                    }
                }
            }
        }
    }

    #[inline]
    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        match &mut self.0 {
            Inner::Contiguous(b) => b.copy_to_bytes(len),
            Inner::NonContiguous(v) => {
                if len <= v[0].remaining() {
                    let bs = v[0].copy_to_bytes(len);
                    if v[0].is_empty() {
                        v.remove(0);
                    }
                    return bs;
                }

                let mut bs = BytesMut::with_capacity(len);
                bs.put(self.take(len));
                bs.freeze()
            }
        }
    }
}
