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

use std::time::Duration;

use codspeed_criterion_compat::BatchSize;
use codspeed_criterion_compat::Criterion;
use once_cell::sync::Lazy;
use opendal::raw::ConcurrentTasks;
use opendal::Executor;

pub static TOKIO: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().expect("build tokio runtime"));

pub fn bench_concurrent_tasks(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_concurrent_tasks");

    for concurrent in [1, 2, 4, 8, 16] {
        group.bench_with_input(concurrent.to_string(), &concurrent, |b, concurrent| {
            b.to_async(&*TOKIO).iter_batched(
                || {
                    ConcurrentTasks::new(Executor::new(), *concurrent, |()| {
                        Box::pin(async {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                            ((), Ok(()))
                        })
                    })
                },
                |mut tasks| async move {
                    for _ in 0..100 {
                        let _ = tasks.execute(()).await;
                    }

                    loop {
                        if tasks.next().await.is_none() {
                            break;
                        }
                    }
                },
                BatchSize::PerIteration,
            )
        });
    }

    group.finish()
}
