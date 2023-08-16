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
use bytes::BytesMut;
use libfuzzer_sys::arbitrary::Arbitrary;
use libfuzzer_sys::arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use opendal::Operator;
use opendal::Result;
use rand::prelude::*;
use sha2::Digest;
use sha2::Sha256;

mod utils;

const MAX_DATA_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
enum WriterAction {
    Write { size: usize },
}

#[derive(Debug, Clone)]
struct FuzzInput {
    actions: Vec<WriterAction>,
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> arbitrary::Result<Self> {
        let mut actions = vec![];

        let count = u.int_in_range(128..=1024)?;

        for _ in 0..count {
            let size = u.int_in_range(1..=MAX_DATA_SIZE)?;
            actions.push(WriterAction::Write { size });
        }

        Ok(FuzzInput { actions })
    }
}

struct WriteChecker {
    chunks: Vec<Bytes>,
    data: Bytes,
}

impl WriteChecker {
    fn new(size: Vec<usize>) -> Self {
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

    fn check(&self, actual: &[u8]) {
        assert_eq!(
            format!("{:x}", Sha256::digest(actual)),
            format!("{:x}", Sha256::digest(&self.data)),
            "check failed: result is not expected"
        )
    }
}

async fn fuzz_writer(op: Operator, input: FuzzInput) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let total_size = input
        .actions
        .iter()
        .map(|a| match a {
            WriterAction::Write { size } => *size,
        })
        .collect();

    let checker = WriteChecker::new(total_size);

    let mut writer = op.writer(&path).await?;

    for chunk in &checker.chunks {
        writer.write(chunk.clone()).await?;
    }

    writer.close().await?;

    let result = op.read(&path).await?;

    checker.check(&result);

    op.delete(&path).await?;
    Ok(())
}

fuzz_target!(|input: FuzzInput| {
    let _ = dotenvy::dotenv();

    let runtime = tokio::runtime::Runtime::new().expect("init runtime must succeed");

    for op in utils::init_services() {
        runtime.block_on(async {
            fuzz_writer(op, input.clone())
                .await
                .unwrap_or_else(|_| panic!("fuzz reader must succeed"));
        })
    }
});
