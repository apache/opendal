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

use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use opendal::raw::tests::init_test_service;
use opendal::raw::tests::WriteAction;
use opendal::raw::tests::WriteChecker;
use opendal::raw::tests::TEST_RUNTIME;
use opendal::Operator;
use opendal::Result;

const MAX_DATA_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
struct FuzzInput {
    actions: Vec<WriteAction>,
    concurrent: usize,
    buffer: usize,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let mut actions = vec![];
        let concurrent = u.int_in_range(0..=16)?;
        let buffer = u.int_in_range(2 * 1024 * 1024..=8 * 1024 * 1024)?;

        let count = u.int_in_range(128..=1024)?;

        for _ in 0..count {
            let size = u.int_in_range(1..=MAX_DATA_SIZE)?;
            actions.push(WriteAction::Write(size));
        }

        Ok(FuzzInput {
            actions,
            concurrent,
            buffer,
        })
    }
}

async fn fuzz_writer(op: Operator, input: FuzzInput) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let total_size = input
        .actions
        .iter()
        .map(|a| match a {
            WriteAction::Write(size) => *size,
        })
        .collect();

    let checker = WriteChecker::new(total_size);

    let mut writer = op
        .writer_with(&path)
        .buffer(input.buffer)
        .concurrent(input.concurrent)
        .await?;

    for chunk in checker.chunks() {
        writer.write(chunk.clone()).await?;
    }

    writer.close().await?;

    let result = op.read(&path).await?;

    checker.check(&result);

    op.delete(&path).await?;
    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    let _ = tracing_subscriber::fmt()
        .pretty()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let op = init_test_service().expect("operator init must succeed");
    if let Some(op) = op {
        TEST_RUNTIME.block_on(async {
            fuzz_writer(op, input.clone())
                .await
                .unwrap_or_else(|err| panic!("fuzz reader must succeed: {err:?}"));
        })
    }
});
