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

// utils that used by tests
#[macro_use]
mod utils;

pub use utils::*;

// Async test cases
mod append;
mod copy;
mod fuzz;
mod list;
mod list_only;
mod presign;
mod read_only;
mod rename;
mod write;
use append::behavior_append_tests;
use copy::behavior_copy_tests;
use fuzz::behavior_fuzz_tests;
use list::behavior_list_tests;
use list_only::behavior_list_only_tests;
use presign::behavior_presign_tests;
use read_only::behavior_read_only_tests;
use rename::behavior_rename_tests;
use write::behavior_write_tests;

// Blocking test cases
mod blocking_append;
mod blocking_copy;
mod blocking_list;
mod blocking_read_only;
mod blocking_rename;
mod blocking_write;
use blocking_append::behavior_blocking_append_tests;
use blocking_copy::behavior_blocking_copy_tests;
use blocking_list::behavior_blocking_list_tests;
use blocking_read_only::behavior_blocking_read_only_tests;
use blocking_rename::behavior_blocking_rename_tests;
use blocking_write::behavior_blocking_write_tests;
// External dependencies
use libtest_mimic::Arguments;
use libtest_mimic::Trial;
use opendal::raw::tests::init_test_service;
use opendal::*;

fn main() -> anyhow::Result<()> {
    let args = Arguments::from_args();

    let op = if let Some(op) = init_test_service()? {
        op
    } else {
        return Ok(());
    };

    let mut tests = Vec::new();
    // Blocking tests
    tests.extend(behavior_blocking_append_tests(&op));
    tests.extend(behavior_blocking_copy_tests(&op));
    tests.extend(behavior_blocking_list_tests(&op));
    tests.extend(behavior_blocking_read_only_tests(&op));
    tests.extend(behavior_blocking_rename_tests(&op));
    tests.extend(behavior_blocking_write_tests(&op));
    // Async tests
    tests.extend(behavior_append_tests(&op));
    tests.extend(behavior_copy_tests(&op));
    tests.extend(behavior_list_only_tests(&op));
    tests.extend(behavior_list_tests(&op));
    tests.extend(behavior_presign_tests(&op));
    tests.extend(behavior_read_only_tests(&op));
    tests.extend(behavior_rename_tests(&op));
    tests.extend(behavior_write_tests(&op));
    tests.extend(behavior_fuzz_tests(&op));

    // Don't init logging while building operator which may break cargo
    // nextest output
    let _ = tracing_subscriber::fmt()
        .pretty()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    libtest_mimic::run(&args, tests).exit();
}
