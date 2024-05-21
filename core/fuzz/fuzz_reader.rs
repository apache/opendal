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

use std::fmt::Debug;
use std::fmt::Formatter;

use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use opendal::raw::tests::init_test_service;
use opendal::raw::tests::ReadAction;
use opendal::raw::tests::ReadChecker;
use opendal::raw::tests::TEST_RUNTIME;
use opendal::Operator;
use opendal::Result;

const MAX_DATA_SIZE: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct FuzzInput {
    path: String,
    size: usize,
    actions: Vec<ReadAction>,
}

impl Debug for FuzzInput {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut actions = self.actions.clone();
        // Remove all Read(0) entry.
        let empty = ReadAction::Read(0, 0);
        actions.retain(|e| e != &empty);

        f.debug_struct("FuzzInput")
            .field("path", &self.path)
            .field("size", &self.size)
            .field("actions", &actions)
            .finish()
    }
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let total_size = u.int_in_range(1..=MAX_DATA_SIZE)?;

        let count = u.int_in_range(1..=1024)?;
        let mut actions = vec![];

        for _ in 0..count {
            let offset = u.int_in_range(0..=total_size)?;
            let size = u.int_in_range(0..=total_size - offset)?;

            actions.push(ReadAction::Read(offset, size));
        }

        Ok(FuzzInput {
            path: uuid::Uuid::new_v4().to_string(),
            size: total_size,
            actions,
        })
    }
}

async fn fuzz_reader(op: Operator, input: FuzzInput) -> Result<()> {
    let mut checker = ReadChecker::new(input.size);
    op.write(&input.path, checker.data()).await?;

    let r = op.reader(&input.path).await?;

    checker.check(r, &input.actions).await;

    op.delete(&input.path).await?;
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
            fuzz_reader(op, input.clone())
                .await
                .unwrap_or_else(|err| panic!("fuzz reader must succeed: {err:?}"));
        })
    }
});
