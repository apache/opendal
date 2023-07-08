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

use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
use libfuzzer_sys::fuzz_target;
use opendal::raw::oio::ReadExt;
use opendal::{services, Operator};
use std::env;

#[derive(Debug)]
enum ReaderAction {
    Read { size: usize },
    Seek { offset: usize, mode:SeekMode },
    Next,
}

#[derive(Debug)]
enum SeekMode {
    Start,
    End,
    Current,
}

#[derive(Debug)]
struct FuzzInput {
    actions: Vec<ReaderAction>,
    data: Vec<u8>,
}

impl Arbitrary<'_> for SeekMode {
    fn arbitrary(u: &mut Unstructured<'_>) -> Result<Self> {
        match u.int_in_range(0..=2)? {
            0 => Ok(SeekMode::Start),
            1 => Ok(SeekMode::End),
            _ => Ok(SeekMode::Current),
        }
    }
}

impl Arbitrary<'_> for FuzzInput {
    fn arbitrary(u: &mut Unstructured<'_>) -> Result<Self> {
        let data: Vec<u8> = {
            // Choose a suitable range for the data length
            let data_len = u.int_in_range(1..=100)?;
            u.bytes(data_len)?.to_vec()
        };
        let mut actions = vec![];
        while u.len() > 0 {
            match u.int_in_range(0..=2)? {
                0 => {
                    // Ensure size is smaller than data size
                    let size = u.int_in_range(0..=data.len())?;
                    actions.push(ReaderAction::Read { size });
                }
                1 => {
                    // Ensure offset is smaller than data size
                    let mode = SeekMode::arbitrary(u)?;
                    let offset = u.int_in_range(0..=data.len())?;
                    actions.push(ReaderAction::Seek { mode, offset });
                }
                _ => actions.push(ReaderAction::Next),
            }
        }
        Ok(FuzzInput { actions, data })
    }
}

fuzz_target!(|input: FuzzInput| {
    let len = input.data.len();
    let mut builder = services::Fs::default();
    let cwd = env::current_dir().unwrap();
    let path: String = format!("{}/tmp", cwd.to_string_lossy());
    builder.root(&path);

    let result: anyhow::Result<()> = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let op: Operator = Operator::new(builder)?.finish();
        let path = uuid::Uuid::new_v4().to_string();
        op.write(&path, input.data)
            .await
            .expect("write must succeed");
        let mut o = op
            .range_reader(&path, 0..len as u64)
            .await
            .expect("init range_reader must succeed");

        for action in input.actions {
            match action {
                ReaderAction::Read { size } => {
                    let mut buf = vec![0; size];
                    o.read(&mut buf)
                        .await
                        .expect("read must succeed");
                }
                ReaderAction::Seek { mode, offset } => {
                    let seek_from = match mode {
                        SeekMode::Start => std::io::SeekFrom::Start(offset as u64),
                        SeekMode::End => std::io::SeekFrom::End(offset as i64),
                        SeekMode::Current => std::io::SeekFrom::Current(offset as i64),
                    };
                    o.seek(seek_from)
                        .await
                        .expect("seek must succeed");
                }
                ReaderAction::Next => {
                    o.next()
                        .await
                        .map(|v| v.expect("next should not return error"));
                }
            }
        }
        op.delete(&path)
            .await
            .expect("delete must succeed");
        Ok(())
    });

    result.expect("fuzz target must succeed");
});
