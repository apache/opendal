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

use bytes::Bytes;
use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::arbitrary::Result;
use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use sha2::Digest;
use sha2::Sha256;

use opendal::Operator;

mod utils;

const MAX_DATA_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
enum WriterAction {
    Write { data: Bytes },
}

#[derive(Debug, Clone)]
struct FuzzInput {
    actions: Vec<WriterAction>,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> Result<Self> {
        let mut actions = vec![];
        let mut action_count = u.int_in_range(128..=1024)?;

        while action_count != 0 {
            action_count -= 1;
            let data_len = u.int_in_range(1..=MAX_DATA_SIZE)?;
            let data: Vec<u8> = u.bytes(data_len)?.to_vec();
            actions.push(WriterAction::Write {
                data: Bytes::from(data),
            });
        }

        Ok(FuzzInput { actions })
    }
}

struct WriterFuzzChecker {
    data: Vec<u8>,
}

impl WriterFuzzChecker {
    fn new(input: FuzzInput) -> Self {
        let mut data = vec![];

        for action in input.actions {
            match action {
                WriterAction::Write { data: d } => {
                    data.extend_from_slice(&d);
                }
            }
        }

        WriterFuzzChecker { data }
    }

    fn check(&self, actual: &[u8]) {
        assert_eq!(
            format!("{:x}", Sha256::digest(actual)),
            format!("{:x}", Sha256::digest(&self.data)),
            "check failed: result is not expected"
        )
    }
}

async fn fuzz_writer_process(input: FuzzInput, op: &Operator, name: &str) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let checker = WriterFuzzChecker::new(input.clone());

    let mut writer = op
        .writer(&path)
        .await
        .unwrap_or_else(|_| panic!("{} create must succeed", name));

    for action in input.actions {
        match action {
            WriterAction::Write { data } => {
                writer
                    .write(data)
                    .await
                    .unwrap_or_else(|_| panic!("{} write must succeed", name));
            }
        }
    }
    writer
        .close()
        .await
        .unwrap_or_else(|_| panic!("{} close must succeed", name));

    let result = op
        .read(&path)
        .await
        .unwrap_or_else(|_| panic!("{} read must succeed", name));

    checker.check(&result);

    op.delete(&path)
        .await
        .unwrap_or_else(|_| panic!("{} delete must succeed", name));
    Ok(())
}

fn fuzz_writer(name: &str, op: &Operator, input: FuzzInput) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    runtime.block_on(async {
        fuzz_writer_process(input, op, name)
            .await
            .unwrap_or_else(|_| panic!("{} fuzz writer must succeed", name));
    });
}

fuzz_target!(|input: FuzzInput| {
    let _ = dotenvy::dotenv();

    for service in utils::init_services() {
        if service.1.is_none() {
            continue;
        }

        let op = service.1.unwrap();

        fuzz_writer(service.0, &op, input.clone());
    }
});
