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

const MAX_DATA_SIZE: usize = 1000;

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
        while !u.is_empty() {
            match u.int_in_range(0..=2)? {
                0 => {
                    // Ensure size is smaller than data size
                    let size = u.int_in_range(0..=data_len)?;
                    actions.push(ReaderAction::Read { size });
                }
                1 => {
                    let offset = u.int_in_range(0..=data_len)?;
                    let seek_from = match u.int_in_range(0..=2)? {
                        0 => SeekFrom::Start(offset as u64),
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

fn fuzz_reader(name: &str, op: &Operator, input: FuzzInput) {
    let len = input.data.len();
    let result: anyhow::Result<()> = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let path = uuid::Uuid::new_v4().to_string();
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
                    o.read(&mut buf)
                        .await
                        .expect(format!("{} read must succeed", name).as_str());
                }
                ReaderAction::Seek(seek_from) => {
                    o.seek(seek_from)
                        .await
                        .expect(format!("{} seek must succeed", name).as_str());
                }
                ReaderAction::Next => {
                    o.next().await.map(|v| {
                        v.expect(format!("{} next should not return error", name).as_str())
                    });
                }
            }
        }
        op.delete(&path)
            .await
            .expect(format!("{} delete must succeed", name).as_str());
        Ok(())
    });

    result.expect(format!("{} fuzz_reader must succeed", name).as_str())
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
