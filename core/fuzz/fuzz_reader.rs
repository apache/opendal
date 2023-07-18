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

#![no_main]

mod utils;

use std::io::SeekFrom;

use bytes::Bytes;
use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::arbitrary::Result;
use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use opendal::raw::oio::ReadExt;
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;

const MAX_DATA_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
enum ReaderAction {
    Read { size: usize },
    Seek(SeekFrom),
    Next,
}

#[derive(Debug, Clone)]
struct FuzzInput {
    actions: Vec<ReaderAction>,
    data: Vec<u8>,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> Result<Self> {
        let data_len = u.int_in_range(1..=MAX_DATA_SIZE)?;
        let data: Vec<u8> = u.bytes(data_len)?.to_vec();

        let mut actions = vec![];
        let mut action_count = u.int_in_range(128..=1024)?;

        while action_count != 0 {
            action_count -= 1;
            match u.int_in_range(0..=2)? {
                0 => {
                    let size = u.int_in_range(0..=data_len * 2)?;
                    actions.push(ReaderAction::Read { size });
                }
                1 => {
                    let offset: i64 = u.int_in_range(-(data_len as i64)..=(data_len as i64))?;
                    let seek_from = match u.int_in_range(0..=2)? {
                        0 => SeekFrom::Start(offset.unsigned_abs()),
                        1 => SeekFrom::End(offset),
                        _ => SeekFrom::Current(offset),
                    };
                    actions.push(ReaderAction::Seek(seek_from));
                }
                _ => actions.push(ReaderAction::Next),
            }
        }
        Ok(FuzzInput { actions, data })
    }
}

struct ReaderFuzzerChecker {
    data: Vec<u8>,
    size: usize,
    cur: usize,
}

impl ReaderFuzzerChecker {
    fn new(data: Vec<u8>) -> Self {
        Self {
            size: data.len(),
            data,
            cur: 0,
        }
    }

    fn check_read(&mut self, n: usize, output: &[u8]) {
        if n == 0 {
            return;
        }

        let expected = &self.data[self.cur..self.cur + n];

        // Check the read result
        assert_eq!(
            format!("{:x}", Sha256::digest(output)),
            format!("{:x}", Sha256::digest(expected)),
            "check read failed: output bs is different with expected bs",
        );

        // Update the current position
        self.cur += n;
    }

    fn check_seek(&mut self, seek_from: SeekFrom, output: opendal::Result<u64>) {
        let expected = match seek_from {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.size as i64 + offset,
            SeekFrom::Current(offset) => self.cur as i64 + offset,
        };

        if expected < 0 {
            assert!(output.is_err(), "check seek failed: seek should fail");
            assert_eq!(
                output.unwrap_err().kind(),
                opendal::ErrorKind::InvalidInput,
                "check seek failed: seek result is different with expected result"
            );
        } else {
            assert_eq!(
                output.unwrap(),
                expected as u64,
                "check seek failed: seek result is different with expected result",
            );

            // only update the current position when seek succeed
            self.cur = expected as usize;
        }
    }

    fn check_next(&mut self, output: Option<Bytes>) {
        if let Some(output) = output {
            assert!(
                self.cur + output.len() <= self.size,
                "check next failed: output bs is larger than remaining bs",
            );

            assert_eq!(
                format!("{:x}", Sha256::digest(&output)),
                format!(
                    "{:x}",
                    Sha256::digest(&self.data[self.cur..self.cur + output.len()])
                ),
                "check next failed: output bs is different with expected bs",
            );

            // update the current position
            self.cur += output.len();
        } else {
            assert!(
                self.cur >= self.size,
                "check next failed: output bs is None, we still have bytes to read",
            )
        }
    }
}

async fn fuzz_reader_process(input: FuzzInput, op: &Operator, name: &str) -> Result<()> {
    let len = input.data.len();
    let path = uuid::Uuid::new_v4().to_string();

    let mut checker = ReaderFuzzerChecker::new(input.data.clone());
    op.write(&path, input.data)
        .await
        .unwrap_or_else(|_| panic!("{} write must succeed", name));

    let mut o = op
        .range_reader(&path, 0..len as u64)
        .await
        .unwrap_or_else(|_| panic!("{} init range_reader must succeed", name));

    for action in input.actions {
        match action {
            ReaderAction::Read { size } => {
                let mut buf = vec![0; size];
                let n = o
                    .read(&mut buf)
                    .await
                    .unwrap_or_else(|_| panic!("{} read must succeed", name));
                checker.check_read(n, &buf[..n]);
            }

            ReaderAction::Seek(seek_from) => {
                let res = o.seek(seek_from).await;
                checker.check_seek(seek_from, res);
            }

            ReaderAction::Next => {
                let res = o
                    .next()
                    .await
                    .map(|v| v.unwrap_or_else(|_| panic!("{} next should not return error", name)));
                checker.check_next(res);
            }
        }
    }

    op.delete(&path)
        .await
        .unwrap_or_else(|_| panic!("{} delete must succeed", name));
    Ok(())
}

fn fuzz_reader(name: &str, op: &Operator, input: FuzzInput) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        fuzz_reader_process(input, op, name)
            .await
            .unwrap_or_else(|_| panic!("{} fuzz_reader must succeed", name));
    });
}

fuzz_target!(|input: FuzzInput| {
    let _ = dotenvy::dotenv();

    for service in utils::init_services() {
        if service.1.is_none() {
            continue;
        }

        let op = service.1.unwrap();

        fuzz_reader(service.0, &op, input.clone());
    }
});
