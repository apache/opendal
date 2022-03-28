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
use futures::io;
use futures::AsyncReadExt;
use futures::AsyncWriteExt;
use futures::StreamExt;
use opendal::Operator;
use rand::prelude::*;
use size::Base;
use size::Size;
use size::Style;

use super::utils::*;

pub fn bench(c: &mut Criterion) {
    for case in services() {
        if case.1.is_none() {
            println!("{} not set, ignore", case.0);
            continue;
        }

        let op = Operator::new(case.1.unwrap());

        bench_read_full(c, op.clone());
        bench_read_part(c, op.clone());
        bench_read_parallel(c, op.clone());
    }
}

fn bench_read_full(c: &mut Criterion, op: Operator) {
    let mut group = c.benchmark_group("read_full");

    let mut rng = thread_rng();

    for size in [
        Size::Kibibytes(4_usize),
        Size::Kibibytes(256),
        Size::Mebibytes(4),
        Size::Mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, size.bytes() as usize);
        let path = uuid::Uuid::new_v4().to_string();
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        group.throughput(criterion::Throughput::Bytes(size.bytes()));
        group.bench_with_input(
            size.to_string(Base::Base2, Style::Abbreviated),
            &(op.clone(), &path),
            |b, (op, path)| {
                b.to_async(&*TOKIO).iter(|| async {
                    // let mut r = op.object(path).limited_reader(size.bytes());
                    // io::copy(r, &mut io::sink()).await.unwrap();

                    let mut s = op
                        .object(path)
                        .stream(None, Some(size.bytes()))
                        .await
                        .unwrap();
                    let mut w = io::sink();
                    while let Some(Ok(v)) = s.next().await {
                        w.write_all(&v).await.unwrap();
                    }
                })
            },
        );

        std::mem::drop(temp_data);
    }

    group.finish()
}

/// Read from 1/4 to 3/4 and than drop the reader without consuming all data;
fn bench_read_part(c: &mut Criterion, op: Operator) {
    let mut group = c.benchmark_group("read_part");

    let mut rng = thread_rng();

    for size in [
        Size::Kibibytes(4_usize),
        Size::Kibibytes(256),
        Size::Mebibytes(4),
        Size::Mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, (size.bytes() * 2) as usize);
        let path = uuid::Uuid::new_v4().to_string();
        let offset = size.bytes() / 2;
        let buf = vec![0; size.bytes() as usize];
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        group.throughput(criterion::Throughput::Bytes(size.bytes()));
        group.bench_with_input(
            size.to_string(Base::Base2, Style::Abbreviated),
            &(op.clone(), &path, buf.clone()),
            |b, (op, path, buf)| {
                b.to_async(&*TOKIO).iter(|| async {
                    let mut buf = buf.clone();
                    let mut r = op.object(path).offset_reader(offset);
                    r.read_exact(&mut buf).await.unwrap();
                })
            },
        );

        std::mem::drop(temp_data);
    }

    group.finish()
}

fn bench_read_parallel(c: &mut Criterion, op: Operator) {
    let mut group = c.benchmark_group("read_parallel");

    let mut rng = thread_rng();

    for size in [
        Size::Kibibytes(4_usize),
        Size::Kibibytes(256),
        Size::Mebibytes(4),
        Size::Mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, (size.bytes() * 2) as usize);
        let path = uuid::Uuid::new_v4().to_string();
        let offset = size.bytes() / 2;
        let buf = vec![0; size.bytes() as usize];
        let temp_data = TempData::generate(op.clone(), &path, content.clone());

        for parallel in [1, 2, 4, 8, 16] {
            group.throughput(criterion::Throughput::Bytes(parallel * size.bytes()));
            group.bench_with_input(
                format!(
                    "{}x{}",
                    parallel,
                    size.to_string(Base::Base2, Style::Abbreviated)
                ),
                &(op.clone(), &path, buf.clone()),
                |b, (op, path, buf)| {
                    b.to_async(&*TOKIO).iter(|| async {
                        let futures = (0..parallel)
                            .map(|_| async {
                                let mut buf = buf.clone();
                                let mut r = op.object(path).range_reader(offset, size.bytes());
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
