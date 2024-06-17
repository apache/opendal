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

use std::mem;
use std::sync::Mutex;

use futures::Future;
use libtest_mimic::Failed;
use libtest_mimic::Trial;
use opendal::raw::tests::TEST_RUNTIME;
use opendal::raw::*;
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
    F: FnOnce(Operator) -> Fut + MaybeSend + 'static,
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
    F: FnOnce(BlockingOperator) -> anyhow::Result<()> + MaybeSend + 'static,
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

pub struct Fixture {
    pub paths: Mutex<Vec<String>>,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    /// Create a new fixture
    pub const fn new() -> Self {
        Self {
            paths: Mutex::new(vec![]),
        }
    }

    /// Create a new dir path
    pub fn new_dir_path(&self) -> String {
        let path = format!("{}/", uuid::Uuid::new_v4());
        self.paths.lock().unwrap().push(path.clone());

        path
    }

    /// Create a new file path
    pub fn new_file_path(&self) -> String {
        let path = format!("{}", uuid::Uuid::new_v4());
        self.paths.lock().unwrap().push(path.clone());

        path
    }

    /// Create a new file with random content
    pub fn new_file(&self, op: impl Into<Operator>) -> (String, Vec<u8>, usize) {
        let max_size = op
            .into()
            .info()
            .full_capability()
            .write_total_max_size
            .unwrap_or(4 * 1024 * 1024);

        self.new_file_with_range(uuid::Uuid::new_v4().to_string(), 1..max_size)
    }

    pub fn new_file_with_path(
        &self,
        op: impl Into<Operator>,
        path: &str,
    ) -> (String, Vec<u8>, usize) {
        let max_size = op
            .into()
            .info()
            .full_capability()
            .write_total_max_size
            .unwrap_or(4 * 1024 * 1024);

        self.new_file_with_range(path, 1..max_size)
    }

    /// Create a new file with random content in range.
    fn new_file_with_range(
        &self,
        path: impl Into<String>,
        range: impl SampleRange<usize>,
    ) -> (String, Vec<u8>, usize) {
        let path = path.into();
        self.paths.lock().unwrap().push(path.clone());

        let mut rng = thread_rng();

        let size = rng.gen_range(range);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);

        (path, content, size)
    }

    /// Perform cleanup
    pub async fn cleanup(&self, op: impl Into<Operator>) {
        let op = op.into();
        let paths: Vec<_> = mem::take(self.paths.lock().unwrap().as_mut());
        for path in paths.iter() {
            // We try our best to cleanup fixtures, but won't panic if failed.
            let _ = op.delete(path).await.map_err(|err| {
                log::error!("fixture cleanup path {path} failed: {:?}", err);
            });
            log::info!("fixture cleanup path {path} succeeded")
        }
    }
}
