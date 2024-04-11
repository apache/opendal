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
use rand::thread_rng;
use rand::RngCore;
use sha2::Digest;
use sha2::Sha256;

use crate::*;

/// ReadAction represents a read action.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReadAction {
    /// Read represents a read action with given input buf size.
    ///
    /// # NOTE
    ///
    /// The size is the input buf size, it's possible that the actual read size is smaller.
    Read(usize, usize),
}

/// ReadChecker is used to check the correctness of the read process.
pub struct ReadChecker {
    /// Raw Data is the data we write to the storage.
    raw_data: Bytes,
}

impl ReadChecker {
    /// Create a new read checker by given size and range.
    ///
    /// It's by design that we use a random generator to generate the raw data. The content of data
    /// is not important, we only care about the correctness of the read process.
    pub fn new(size: usize) -> Self {
        let mut rng = thread_rng();
        let mut data = vec![0; size];
        rng.fill_bytes(&mut data);

        let raw_data = Bytes::from(data);

        Self { raw_data }
    }

    /// Return the raw data of this read checker.
    pub fn data(&self) -> Bytes {
        self.raw_data.clone()
    }

    /// check_read checks the correctness of the read process after a read action.
    ///
    /// - buf_size is the read action's buf size.
    /// - output is the output of this read action.
    fn check_read(&self, offset: usize, size: usize, output: &[u8]) {
        if size == 0 {
            assert_eq!(
                output.len(),
                0,
                "check read failed: output must be empty if buf_size is 0"
            );
            return;
        }

        if size > 0 && output.is_empty() {
            assert!(
                offset >= self.raw_data.len(),
                "check read failed: no data read means cur must outsides of ranged_data",
            );
            return;
        }

        assert!(
            offset + output.len() <= self.raw_data.len(),
            "check read failed: cur + output length must be less than ranged_data length, offset: {}, output: {}, ranged_data: {}",  offset, output.len(), self.raw_data.len(),
        );

        let expected = &self.raw_data[offset..offset + output.len()];

        // Check the read result
        assert_eq!(
            format!("{:x}", Sha256::digest(output)),
            format!("{:x}", Sha256::digest(expected)),
            "check read failed: output bs is different with expected bs",
        );
    }

    /// Check will check the correctness of the read process via given actions.
    ///
    /// Check will panic if any check failed.
    pub async fn check(&mut self, r: Reader, actions: &[ReadAction]) {
        for action in actions {
            match *action {
                ReadAction::Read(offset, size) => {
                    let bs = r
                        .read(offset as u64..(offset + size) as u64)
                        .await
                        .expect("read must success");
                    self.check_read(offset, size, bs.to_bytes().as_ref());
                }
            }
        }
    }

    /// Check will check the correctness of the read process via given actions.
    ///
    /// Check will panic if any check failed.
    pub fn blocking_check(&mut self, r: BlockingReader, actions: &[ReadAction]) {
        for action in actions {
            match *action {
                ReadAction::Read(offset, size) => {
                    let bs = r
                        .read(offset as u64..(offset + size) as u64)
                        .expect("read must success");
                    self.check_read(offset, size, bs.to_bytes().as_ref());
                }
            }
        }
    }
}
