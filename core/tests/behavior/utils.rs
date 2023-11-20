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

use std::fmt;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::SeekFrom;
use std::usize;

use bytes::Bytes;
use futures::Future;
use libtest_mimic::Failed;
use libtest_mimic::Trial;
use log::debug;
use opendal::raw::tests::TEST_RUNTIME;
use opendal::*;
use rand::distributions::uniform::SampleRange;
use rand::prelude::*;
use sha2::Digest;
use sha2::Sha256;

pub fn gen_bytes_with_range(range: impl SampleRange<usize>) -> (Vec<u8>, usize) {
    let mut rng = thread_rng();

    let size = rng.gen_range(range);
    let mut content = vec![0; size];
    rng.fill_bytes(&mut content);

    (content, size)
}

pub fn gen_bytes(cap: Capability) -> (Vec<u8>, usize) {
    let max_size = cap.write_total_max_size.unwrap_or(4 * 1024 * 1024);
    gen_bytes_with_range(1..max_size)
}

pub fn gen_fixed_bytes(size: usize) -> Vec<u8> {
    let (content, _) = gen_bytes_with_range(size..=size);

    content
}

pub fn gen_offset_length(size: usize) -> (u64, u64) {
    let mut rng = thread_rng();

    // Make sure at least one byte is read.
    let offset = rng.gen_range(0..size - 1);
    let length = rng.gen_range(1..(size - offset));

    (offset as u64, length as u64)
}

/// Build a new async trail as a test case.
pub fn build_async_trial<F, Fut>(name: &str, op: &Operator, f: F) -> Trial
where
    F: FnOnce(Operator) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let handle = TEST_RUNTIME.handle().clone();
    let op = op.clone();

    Trial::test(format!("behavior::{name}"), move || {
        handle
            .block_on(f(op))
            .map_err(|err| Failed::from(err.to_string()))
    })
}

#[macro_export]
macro_rules! async_trials {
    ($op:ident, $($test:ident),*) => {
        vec![$(
            build_async_trial(stringify!($test), $op, $test),
        )*]
    };
}

/// Build a new async trail as a test case.
pub fn build_blocking_trial<F>(name: &str, op: &Operator, f: F) -> Trial
where
    F: FnOnce(BlockingOperator) -> anyhow::Result<()> + Send + 'static,
{
    let op = op.blocking();

    Trial::test(format!("behavior::{name}"), move || {
        f(op).map_err(|err| Failed::from(err.to_string()))
    })
}

#[macro_export]
macro_rules! blocking_trials {
    ($op:ident, $($test:ident),*) => {
        vec![$(
            build_blocking_trial(stringify!($test), $op, $test),
        )*]
    };
}

/// ObjectReaderFuzzer is the fuzzer for object readers.
///
/// We will generate random read/seek/next operations to operate on object
/// reader to check if the output is expected.
///
/// # TODO
///
/// This fuzzer only generate valid operations.
///
/// In the future, we need to generate invalid operations to check if we
/// handled correctly.
pub struct ObjectReaderFuzzer {
    name: String,
    bs: Vec<u8>,

    offset: usize,
    size: usize,
    cur: usize,
    rng: ThreadRng,
    actions: Vec<ObjectReaderAction>,
}

#[derive(Debug, Clone, Copy)]
pub enum ObjectReaderAction {
    Read(usize),
    Seek(SeekFrom),
    Next,
}

impl ObjectReaderFuzzer {
    /// Create a new fuzzer.
    pub fn new(name: &str, bs: Vec<u8>, offset: usize, size: usize) -> Self {
        Self {
            name: name.to_string(),
            bs,

            offset,
            size,
            cur: 0,

            rng: thread_rng(),
            actions: vec![],
        }
    }

    /// Generate a new action.
    pub fn fuzz(&mut self) -> ObjectReaderAction {
        let action = match self.rng.gen_range(0..3) {
            // Generate a read action.
            0 => {
                if self.cur >= self.size {
                    ObjectReaderAction::Read(0)
                } else {
                    let size = self.rng.gen_range(0..self.size - self.cur);
                    ObjectReaderAction::Read(size)
                }
            }
            // Generate a seek action.
            1 => match self.rng.gen_range(0..3) {
                // Generate a SeekFrom::Start action.
                0 => {
                    let offset = self.rng.gen_range(0..self.size as u64);
                    ObjectReaderAction::Seek(SeekFrom::Start(offset))
                }
                // Generate a SeekFrom::End action.
                1 => {
                    let offset = self.rng.gen_range(-(self.size as i64)..0);
                    ObjectReaderAction::Seek(SeekFrom::End(offset))
                }
                // Generate a SeekFrom::Current action.
                2 => {
                    let offset = self
                        .rng
                        .gen_range(-(self.cur as i64)..(self.size - self.cur) as i64);
                    ObjectReaderAction::Seek(SeekFrom::Current(offset))
                }
                _ => unreachable!(),
            },
            // Generate a next action.
            2 => ObjectReaderAction::Next,
            _ => unreachable!(),
        };

        debug!("{} perform fuzz action: {:?}", self.name, action);
        self.actions.push(action);

        action
    }

    /// Check if read operation is expected.
    pub fn check_read(&mut self, output_n: usize, output_bs: &[u8]) {
        assert!(
            self.cur + output_n <= self.size,
            "check read failed: output bs is larger than remaining bs: actions: {:?}",
            self.actions
        );

        let current_size = self.offset + self.cur;
        let expected_bs = &self.bs[current_size..current_size + output_n];

        assert_eq!(
            format!("{:x}", Sha256::digest(output_bs)),
            format!("{:x}", Sha256::digest(expected_bs)),
            "check read failed: output bs is different with expected bs, actions: {:?}",
            self.actions,
        );

        // Update current pos.
        self.cur += output_n;
    }

    /// Check if seek operation is expected.
    pub fn check_seek(&mut self, input_pos: SeekFrom, output_pos: u64) {
        let expected_pos = match input_pos {
            SeekFrom::Start(offset) => offset as i64,
            SeekFrom::End(offset) => self.size as i64 + offset,
            SeekFrom::Current(offset) => self.cur as i64 + offset,
        };

        assert_eq!(
            output_pos, expected_pos as u64,
            "check seek failed: output pos is different with expected pos, actions: {:?}",
            self.actions
        );

        // Update current pos.
        self.cur = expected_pos as usize;
    }

    /// Check if next operation is expected.
    pub fn check_next(&mut self, output_bs: Option<Bytes>) {
        if let Some(output_bs) = output_bs {
            assert!(
                self.cur + output_bs.len() <= self.size,
                "check next failed: output bs is larger than remaining bs, actions: {:?}",
                self.actions
            );

            let current_size = self.offset + self.cur;
            let expected_bs = &self.bs[current_size..current_size + output_bs.len()];

            assert_eq!(
                format!("{:x}", Sha256::digest(&output_bs)),
                format!("{:x}", Sha256::digest(expected_bs)),
                "check next failed: output bs is different with expected bs, actions: {:?}",
                self.actions
            );

            // Update current pos.
            self.cur += output_bs.len();
        } else {
            assert!(
                self.cur >= self.size,
                "check next failed: output bs is None, we still have bytes to read, actions: {:?}",
                self.actions
            )
        }
    }
}

/// ObjectWriterFuzzer is the fuzzer for object writer.
///
/// We will generate random write operations to operate on object
/// write to check if the output is expected.
///
/// # TODO
///
/// This fuzzer only generate valid operations.
///
/// In the future, we need to generate invalid operations to check if we
/// handled correctly.
pub struct ObjectWriterFuzzer {
    name: String,
    bs: Vec<u8>,

    size: Option<usize>,
    cur: usize,
    rng: ThreadRng,
    actions: Vec<ObjectWriterAction>,
}

#[derive(Clone)]
pub enum ObjectWriterAction {
    Write(Bytes),
}

impl Debug for ObjectWriterAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ObjectWriterAction::Write(bs) => write!(f, "Write({})", bs.len()),
        }
    }
}

impl ObjectWriterFuzzer {
    /// Create a new fuzzer.
    pub fn new(name: &str, size: Option<usize>) -> Self {
        Self {
            name: name.to_string(),
            bs: Vec::new(),

            size,
            cur: 0,

            rng: thread_rng(),
            actions: vec![],
        }
    }

    /// Generate a new action.
    pub fn fuzz(&mut self) -> ObjectWriterAction {
        let max = if let Some(size) = self.size {
            size - self.cur
        } else {
            // Set max to 1MiB
            1024 * 1024
        };

        let size = self.rng.gen_range(0..max);

        let mut bs = vec![0; size];
        self.rng.fill_bytes(&mut bs);

        let bs = Bytes::from(bs);
        self.bs.extend_from_slice(&bs);
        self.cur += bs.len();

        let action = ObjectWriterAction::Write(bs);
        debug!("{} perform fuzz action: {:?}", self.name, action);

        self.actions.push(action.clone());

        action
    }

    /// Check if read operation is expected.
    pub fn check(&mut self, actual_bs: &[u8]) {
        assert_eq!(
            self.bs.len(),
            actual_bs.len(),
            "check failed: expected len is different with actual len, actions: {:?}",
            self.actions
        );

        assert_eq!(
            format!("{:x}", Sha256::digest(&self.bs)),
            format!("{:x}", Sha256::digest(actual_bs)),
            "check failed: expected bs is different with actual bs, actions: {:?}",
            self.actions,
        );
    }
}
