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

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

use std::future::Future;

use futures::future::LocalBoxFuture;
use opendal::raw::BoxedStaticFuture;
use opendal::*;
use wasm_bindgen_test::{console_log, wasm_bindgen_test, wasm_bindgen_test_configure};

#[macro_use]
#[path = "../../../tests/behavior/utils.rs"]
mod utils;
use utils::*;

#[path = "../../../tests/behavior/async_create_dir.rs"]
mod async_create_dir;
#[path = "../../../tests/behavior/async_read.rs"]
mod async_read;
#[path = "../../../tests/behavior/async_stat.rs"]
mod async_stat;

#[path = "../../../tests/behavior/async_write.rs"]
mod async_write;

wasm_bindgen_test_configure!(run_in_browser);

static TEST_FIXTURE: Fixture = Fixture::new();

type Trial = (&'static str, LocalBoxFuture<'static, anyhow::Result<()>>);

fn build_async_trial<F>(
    name: &'static str,
    op: &Operator,
    test: impl FnOnce(Operator) -> F,
) -> Trial
where
    F: Future<Output = anyhow::Result<()>> + 'static,
{
    (name, Box::pin(test(op.clone())))
}

struct BrowserExecutor;

impl Execute for BrowserExecutor {
    fn execute(&self, future: BoxedStaticFuture<()>) {
        wasm_bindgen_futures::spawn_local(future);
    }
}

#[wasm_bindgen_test]
async fn memory_behavior() {
    let op = Operator::new(services::Memory::default().root("/behavior/"))
        .expect("build Memory operator")
        .with_context(OperationContext::default().with_executor(Executor::with(BrowserExecutor)));

    let mut tests = Vec::new();
    async_read::tests(&op, &mut tests);
    async_stat::tests(&op, &mut tests);
    async_create_dir::tests(&op, &mut tests);
    async_write::tests(&op, &mut tests);

    assert!(!tests.is_empty(), "Memory should register behavior tests");
    for (name, test) in tests {
        console_log!("behavior::{name}");
        test.await
            .unwrap_or_else(|err| panic!("behavior::{name} failed: {err:#}"));
    }
    TEST_FIXTURE.cleanup(op).await;
}
