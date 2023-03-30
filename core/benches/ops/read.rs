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
use futures::AsyncReadExt;
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

        bench_read_full(c, case.0, op.clone());
        bench_read_part(c, case.0, op.clone());
        bench_read_parallel(c, case.0, op.clone());
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
            b.to_async(&*TOKIO).iter(|| async {
                let r = op
                    .range_reader(path, 0..=size.bytes() as u64)
                    .await
                    .unwrap();
                io::copy(r, &mut io::sink()).await.unwrap();
            })
        });

        std::mem::drop(temp_data);
    }

    group.finish()
}

/// Read from 1/4 to 3/4 and than drop the reader without consuming all data;
fn bench_read_part(c: &mut Criterion, name: &str, op: Operator) {
    let mut group = c.benchmark_group(format!("service_{name}_read_part"));

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
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        group.throughput(criterion::Throughput::Bytes(size.bytes() as u64));
        group.bench_with_input(size.to_string(), &(op.clone(), &path), |b, (op, path)| {
            b.to_async(&*TOKIO).iter(|| async {
                let r = op.range_reader(path, offset..).await.unwrap();
                io::copy(r, &mut io::sink()).await.unwrap();
            })
        });

        std::mem::drop(temp_data);
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
        let buf = vec![0; size.bytes() as usize];
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        for parallel in [1, 2, 4, 8, 16] {
            group.throughput(criterion::Throughput::Bytes(parallel * size.bytes() as u64));
            group.bench_with_input(
                format!("{}x{}", parallel, size.to_string()),
                &(op.clone(), &path, buf.clone()),
                |b, (op, path, buf)| {
                    b.to_async(&*TOKIO).iter(|| async {
                        let futures = (0..parallel)
                            .map(|_| async {
                                let mut buf = buf.clone();
                                let mut r = op
                                    .range_reader(path, offset..=offset + size.bytes() as u64)
                                    .await
                                    .unwrap();
                                r.read_exact(&mut buf).await.unwrap();

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

        std::mem::drop(temp_data);
    }

    group.finish()
}
