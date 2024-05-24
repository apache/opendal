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
use futures::io;
use opendal::raw::tests::init_test_service;
use opendal::raw::tests::TEST_RUNTIME;
use opendal::Operator;
use rand::prelude::*;
use size::Size;

use super::utils::*;

pub fn bench(c: &mut Criterion) {
    if let Some(op) = init_test_service().unwrap() {
        bench_read_full(c, op.info().scheme().into_static(), op.clone());
        bench_read_parallel(c, op.info().scheme().into_static(), op.clone());
    }
}

fn bench_read_full(c: &mut Criterion, name: &str, op: Operator) {
    let mut group = c.benchmark_group(format!("service_{name}_read_full"));

    let mut rng = thread_rng();

    for size in [
        Size::from_kibibytes(4),
        Size::from_kibibytes(256),
        Size::from_mebibytes(4),
        Size::from_mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, size.bytes() as usize);
        let path = uuid::Uuid::new_v4().to_string();
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        group.throughput(criterion::Throughput::Bytes(size.bytes() as u64));
        group.bench_with_input(size.to_string(), &(op.clone(), &path), |b, (op, path)| {
            b.to_async(&*TEST_RUNTIME).iter(|| async {
                let r = op.reader_with(path).await.unwrap();

                let r = r
                    .into_futures_async_read(0..size.bytes() as u64)
                    .await
                    .unwrap();
                io::copy(r, &mut io::sink()).await.unwrap();
            })
        });

        drop(temp_data);
    }

    group.finish()
}

fn bench_read_parallel(c: &mut Criterion, name: &str, op: Operator) {
    let mut group = c.benchmark_group(format!("service_{name}_read_parallel"));

    let mut rng = thread_rng();

    for size in [
        Size::from_kibibytes(4),
        Size::from_kibibytes(256),
        Size::from_mebibytes(4),
        Size::from_mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, (size.bytes() * 2) as usize);
        let path = uuid::Uuid::new_v4().to_string();
        let offset = (size.bytes() / 2) as u64;
        let buf_size = size.bytes() as usize;
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        for parallel in [1, 2, 4, 8, 16] {
            group.throughput(criterion::Throughput::Bytes(parallel * size.bytes() as u64));
            group.bench_with_input(
                format!("{}x{}", parallel, size.to_string()),
                &(op.clone(), &path, buf_size),
                |b, (op, path, buf_size)| {
                    b.to_async(&*TEST_RUNTIME).iter(|| async {
                        let futures = (0..parallel)
                            .map(|_| async {
                                let r = op.reader_with(path).await.unwrap();
                                let _ = r.read(offset..offset + *buf_size as u64).await.unwrap();

                                let mut d = 0;
                                // mock same little cpu work
                                for c in offset..offset + 100u64 {
                                    d += c & (0x1f1f1f1f + c % 256);
                                }
                                let _ = d;
                            })
                            .collect::<Vec<_>>();
                        futures::future::join_all(futures).await
                    })
                },
            );
        }

        drop(temp_data);
    }

    group.finish()
}
