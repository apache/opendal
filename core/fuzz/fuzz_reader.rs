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

use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use opendal::raw::oio::ReadExt;
use opendal::Operator;
use std::io::SeekFrom;
use sha2::{Digest, Sha256};

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
        // Choose a suitable range for the data length
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
                    let offset: i32 = u.int_in_range(-(data_len as i32)..=(data_len as i32))?;
                    let seek_from = match u.int_in_range(0..=2)? {
                        0 => SeekFrom::Start(offset.abs() as u64),
                        1 => SeekFrom::End(offset as i64),
                        _ => SeekFrom::Current(offset as i64),
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

    fn check_seek(&mut self, seek_from: SeekFrom, output: Result<u64>) {
        let expected = match seek_from {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                if offset < 0 {
                    self.size as u64 - offset.abs() as u64
                } else {
                    self.size as u64 + offset as u64
                }
            }
            SeekFrom::Current(offset) => {
                if offset < 0 {
                    self.cur as u64 - offset.abs() as u64
                } else {
                    self.cur as u64 + offset as u64
                }
            }
        };
        assert_eq!(
            output.unwrap(),
            expected,
            "check seek failed: output offset is different with expected offset",
        );
    }

}



async fn fuzz_reader_process(input: FuzzInput, op: &Operator, name: &str) -> Result<()> {
    let len = input.data.len();
    let path = uuid::Uuid::new_v4().to_string();
    let mut checker = ReaderFuzzerChecker::new(input.data.clone());
    op.write(&path, input.data)
        .await
        .expect(format!("{} write must succeed", name).as_str());
    let mut o = op
        .range_reader(&path, 0..len as u64)
        .await
        .expect(format!("{} init range_reader must succeed", name).as_str());
    for action in input.actions {
        match action {
            ReaderAction::Read { size } => {
                let mut buf = vec![0; size];
                let n = o.read(&mut buf)
                    .await
                    .expect(format!("{} read must succeed", name).as_str());
                checker.check_read(n, &buf[..n]);
            }
            ReaderAction::Seek(seek_from) => {
                let res = o.seek(seek_from).await;

            }
            ReaderAction::Next => {
                o.next()
                    .await
                    .map(|v| v.expect(format!("{} next should not return error", name).as_str()));
            }
        }
    }
    op.delete(&path)
        .await
        .expect(format!("{} delete must succeed", name).as_str());
    Ok(())
}

fn fuzz_reader(name: &str, op: &Operator, input: FuzzInput) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        fuzz_reader_process(input, &op, name)
            .await
            .expect(format!("{} fuzz_reader must succeed", name).as_str());
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
