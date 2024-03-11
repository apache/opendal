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
use rand::thread_rng;
use rand::RngCore;
use sha2::Digest;
use sha2::Sha256;

use crate::raw::*;
use crate::*;

/// ReadAction represents a read action.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ReadAction {
    /// Read represents a read action with given input buf size.
    ///
    /// # NOTE
    ///
    /// The size is the input buf size, it's possible that the actual read size is smaller.
    Read(usize),
    /// Seek represents a seek action with given seek position.
    ///
    /// # NOTE
    ///
    /// It's valid that seek outside of the file's end.
    Seek(SeekFrom),
}

/// ReadChecker is used to check the correctness of the read process.
pub struct ReadChecker {
    /// Raw Data is the data we write to the storage.
    raw_data: Bytes,
    /// Ranged Data is the data that we read from the storage.
    ranged_data: Bytes,
    /// Cur is the current position of the read process.
    cur: usize,
}

impl ReadChecker {
    /// Create a new read checker by given size and range.
    ///
    /// It's by design that we use a random generator to generate the raw data. The content of data
    /// is not important, we only care about the correctness of the read process.
    pub fn new(size: usize, range: impl Into<BytesRange>) -> Self {
        let mut rng = thread_rng();
        let mut data = vec![0; size];
        rng.fill_bytes(&mut data);

        let raw_data = Bytes::from(data);
        let ranged_data = range.into().apply_on_bytes(raw_data.clone());

        Self {
            raw_data,
            ranged_data,

            cur: 0,
        }
    }

    /// Return the raw data of this read checker.
    pub fn data(&self) -> Bytes {
        self.raw_data.clone()
    }

    /// check_read checks the correctness of the read process after a read action.
    ///
    /// - buf_size is the read action's buf size.
    /// - output is the output of this read action.
    fn check_read(&mut self, input: usize, output: &[u8]) {
        if input == 0 {
            assert_eq!(
                output.len(),
                0,
                "check read failed: output must be empty if buf_size is 0"
            );
            return;
        }

        if input > 0 && output.is_empty() {
            assert!(
                self.cur >= self.ranged_data.len(),
                "check read failed: no data read means cur must outsides of ranged_data",
            );
            return;
        }

        assert!(
            self.cur + output.len() <= self.ranged_data.len(),
            "check read failed: cur + output length must be less than ranged_data length, cur: {}, output: {}, ranged_data: {}",  self.cur, output.len(), self.ranged_data.len(),
        );

        let expected = &self.ranged_data[self.cur..self.cur + output.len()];

        // Check the read result
        assert_eq!(
            format!("{:x}", Sha256::digest(output)),
            format!("{:x}", Sha256::digest(expected)),
            "check read failed: output bs is different with expected bs",
        );

        // Update the current position
        self.cur += output.len();
    }

    /// check_seek checks the correctness of the read process after a seek action.
    ///
    /// - input is the `SeekFrom` passed by SeekAction.
    /// - output ts the result after the seek operation.
    fn check_seek(&mut self, input: SeekFrom, output: Result<u64>) {
        let expected = match input {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.ranged_data.len() as i64 + offset,
            SeekFrom::Current(offset) => self.cur as i64 + offset,
        };

        if expected < 0 {
            let Err(err) = output else {
                panic!("check seek failed: seek should fail with negative offset");
            };

            assert_eq!(
                err.kind(),
                ErrorKind::InvalidInput,
                "check seek failed: seek should fail with error InvalidInput with negative offset"
            );
            return;
        }

        assert_eq!(
            output.unwrap(),
            expected as u64,
            "check seek failed: seek result is different with expected result",
        );

        // only update the current position when seek succeed
        self.cur = expected as usize;
    }

    /// Check will check the correctness of the read process via given actions.
    ///
    /// Check will panic if any check failed.
    pub async fn check(&mut self, mut r: Reader, actions: &[ReadAction]) {
        use oio::Read;

        for action in actions {
            match action {
                ReadAction::Read(size) => {
                    let bs = r.read(*size).await.expect("read must success");
                    self.check_read(*size, &bs);
                }

                ReadAction::Seek(pos) => {
                    let res = r.seek(*pos).await;
                    self.check_seek(*pos, res);
                }
            }
        }
    }

    /// Check will check the correctness of the read process via given actions.
    ///
    /// Check will panic if any check failed.
    pub fn blocking_check(&mut self, mut r: BlockingReader, actions: &[ReadAction]) {
        for action in actions {
            match action {
                ReadAction::Read(size) => {
                    use oio::BlockingRead;

                    let mut buf = vec![0; *size];
                    let n = r.read(&mut buf).expect("read must success");
                    self.check_read(*size, &buf[..n]);
                }

                ReadAction::Seek(pos) => {
                    use oio::BlockingRead;

                    let res = r.seek(*pos);
                    self.check_seek(*pos, res);
                }
            }
        }
    }
}
