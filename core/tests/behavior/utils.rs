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

use std::usize;

use futures::Future;
use libtest_mimic::Failed;
use libtest_mimic::Trial;
use opendal::raw::tests::TEST_RUNTIME;
use opendal::*;
use rand::distributions::uniform::SampleRange;
use rand::prelude::*;

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
