// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use criterion::Criterion;
use futures::future::join_all;
use opendal::Operator;
use rand::prelude::*;

use super::utils::*;

const TOTAL_SIZE: usize = 16 * 1024 * 1024;
const PARALLEL_RUNTIME_THREAD: usize = 4;

pub fn bench(c: &mut Criterion) {
    let mut rng = thread_rng();
    let content = gen_bytes(&mut rng, TOTAL_SIZE);

    for case in services() {
        if case.1.is_none() {
            println!("{} not set, ignore", case.0);
            continue;
        }

        let op = Operator::new(case.1.unwrap());
        let path = uuid::Uuid::new_v4().to_string();
        // generate test file before test.
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(PARALLEL_RUNTIME_THREAD)
            .build()
            .expect("build parallel runtime");

        let mut group = c.benchmark_group(format!("{}_parallel", case.0));

        for parallel in [2, 4, 8, 16] {
            group.throughput(criterion::Throughput::Bytes(
                parallel as u64 * TOTAL_SIZE as u64 / 2,
            ));
            group.bench_with_input(
                format!("parallel_range_read_{}", parallel),
                &(op.clone(), &path, content.clone()),
                |b, input| {
                    let pos = rng.gen_range(0..(TOTAL_SIZE / 2) as u64) as u64;
                    b.to_async(&runtime).iter(|| {
                        let futures = (0..parallel)
                            .map(|_| async {
                                super::normal::bench_range_read(input.0.clone(), input.1, pos)
                                    .await;

                                let mut d = 0;
                                // mock same little cpu work
                                for c in pos..pos + 100u64 {
                                    d += c & (0x1f1f1f1f + c % 256);
                                }
                                let _ = d;
                            })
                            .collect::<Vec<_>>();
                        join_all(futures)
                    })
                },
            );
        }

        group.finish();
        std::mem::drop(temp_data);
    }
}
