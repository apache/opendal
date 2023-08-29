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

use criterion::Criterion;
use opendal::Operator;
use rand::prelude::*;
use size::Size;

use super::utils::*;

pub fn bench(c: &mut Criterion) {
    for case in services() {
        if case.1.is_none() {
            println!("{} not set, ignore", case.0);
            continue;
        }

        let op = case.1.unwrap();

        bench_write_once(c, case.0, op.clone());
    }
}

fn bench_write_once(c: &mut Criterion, name: &str, op: Operator) {
    let mut group = c.benchmark_group(format!("service_{name}_write_once"));

    let mut rng = thread_rng();

    for size in [
        Size::from_kibibytes(4),
        Size::from_kibibytes(256),
        Size::from_mebibytes(4),
        Size::from_mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, size.bytes() as usize);
        let path = uuid::Uuid::new_v4().to_string();
        let temp_data = TempData::existing(op.clone(), &path);

        group.throughput(criterion::Throughput::Bytes(size.bytes() as u64));
        group.bench_with_input(
            size.to_string(),
            &(op.clone(), &path, content.clone()),
            |b, (op, path, content)| {
                b.to_async(&*TOKIO).iter(|| async {
                    op.write(path, content.clone()).await.unwrap();
                })
            },
        );

        std::mem::drop(temp_data);
    }

    group.finish()
}
