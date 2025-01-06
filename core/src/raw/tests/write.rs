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
use bytes::BytesMut;
use rand::thread_rng;
use rand::RngCore;
use sha2::Digest;
use sha2::Sha256;

/// WriteAction represents a read action.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WriteAction {
    /// Write represents a write action with given input buf size.
    ///
    /// # NOTE
    ///
    /// The size is the input buf size, it's possible that the actual write size is smaller.
    Write(usize),
}

/// WriteAction is used to check the correctness of the write process.
pub struct WriteChecker {
    chunks: Vec<Bytes>,
    data: Bytes,
}

impl WriteChecker {
    /// Create a new WriteChecker with given size.
    pub fn new(size: Vec<usize>) -> Self {
        let mut rng = thread_rng();

        let mut chunks = Vec::with_capacity(size.len());

        for i in size {
            let mut bs = vec![0u8; i];
            rng.fill_bytes(&mut bs);
            chunks.push(Bytes::from(bs));
        }

        let data = chunks.iter().fold(BytesMut::new(), |mut acc, x| {
            acc.extend_from_slice(x);
            acc
        });

        WriteChecker {
            chunks,
            data: data.freeze(),
        }
    }

    /// Get the check's chunks.
    pub fn chunks(&self) -> &[Bytes] {
        &self.chunks
    }

    /// Check the correctness of the write process.
    pub fn check(&self, actual: &[u8]) {
        assert_eq!(
            format!("{:x}", Sha256::digest(actual)),
            format!("{:x}", Sha256::digest(&self.data)),
            "check failed: result is not expected"
        )
    }
}
