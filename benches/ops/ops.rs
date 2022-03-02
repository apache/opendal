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

use std::io::SeekFrom;

use criterion::BenchmarkId;
use criterion::Criterion;
use futures::io;
use futures::io::BufReader;
use futures::AsyncSeekExt;
use opendal::Operator;
use opendal_test::services::fs;
use opendal_test::services::s3;
use rand::prelude::*;

const TOTAL_SIZE: usize = 16 * 1024 * 1024;
const BATCH_SIZE: usize = 4 * 1024 * 1024;

pub fn bench(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut rng = thread_rng();

    let content = gen_bytes(&mut rng, TOTAL_SIZE);

    let cases = runtime.block_on(async {
        vec![
            ("fs", fs::new().await.expect("init fs")),
            ("s3", s3::new().await.expect("init s3")),
        ]
    });

    for case in cases {
        if case.1.is_none() {
            println!("{} not set, ignore", case.0);
            continue;
        }
        let op = Operator::new(case.1.unwrap());
        let path = uuid::Uuid::new_v4().to_string();

        // Write file before test.
        runtime
            .block_on(op.object(&path).writer().write_bytes(content.clone()))
            .expect("write failed");

        let mut group = c.benchmark_group(case.0);
        group.throughput(criterion::Throughput::Bytes(TOTAL_SIZE as u64));
        group.bench_with_input(
            BenchmarkId::new("read", &path),
            &(op.clone(), &path),
            |b, input| {
                b.to_async(&runtime)
                    .iter(|| bench_read(input.0.clone(), input.1))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("buf_read", &path),
            &(op.clone(), &path),
            |b, input| {
                b.to_async(&runtime)
                    .iter(|| bench_buf_read(input.0.clone(), input.1))
            },
        );
        group.bench_with_input(
            BenchmarkId::new("seek_read", &path),
            &(op.clone(), &path),
            |b, input| {
                let pos = rng.gen_range(0..TOTAL_SIZE - BATCH_SIZE) as u64;
                b.to_async(&runtime)
                    .iter(|| bench_seek_read(input.0.clone(), input.1, pos))
            },
        );
        group.throughput(criterion::Throughput::Bytes((TOTAL_SIZE / 2) as u64));
        group.bench_with_input(
            BenchmarkId::new("read_half", &path),
            &(op.clone(), &path),
            |b, input| {
                b.to_async(&runtime)
                    .iter(|| bench_read_half(input.0.clone(), input.1))
            },
        );

        group.throughput(criterion::Throughput::Bytes(TOTAL_SIZE as u64));
        group.bench_with_input(
            BenchmarkId::new("write", &path),
            &(op.clone(), &path, content.clone()),
            |b, input| {
                b.to_async(&runtime)
                    .iter(|| bench_write(input.0.clone(), input.1, input.2.clone()))
            },
        );
        group.finish();
    }
}

fn gen_bytes(rng: &mut ThreadRng, size: usize) -> Vec<u8> {
    let mut content = vec![0; size as usize];
    rng.fill_bytes(&mut content);

    content
}

pub async fn bench_read(op: Operator, path: &str) {
    let mut r = op.object(path).limited_reader(TOTAL_SIZE as u64);
    io::copy(&mut r, &mut io::sink()).await.unwrap();
}

pub async fn bench_seek_read(op: Operator, path: &str, pos: u64) {
    let mut r = op.object(path).limited_reader(TOTAL_SIZE as u64);
    r.seek(SeekFrom::Start(pos)).await.expect("seek");
    r.seek(SeekFrom::Start(0)).await.expect("seek");
    io::copy(&mut r, &mut io::sink()).await.unwrap();
}

pub async fn bench_buf_read(op: Operator, path: &str) {
    let r = op.object(path).limited_reader(TOTAL_SIZE as u64);
    let mut r = BufReader::with_capacity(BATCH_SIZE, r);

    io::copy(&mut r, &mut io::sink()).await.unwrap();
}

pub async fn bench_read_half(op: Operator, path: &str) {
    let mut r = op.object(path).limited_reader((TOTAL_SIZE / 2) as u64);
    io::copy(&mut r, &mut io::sink()).await.unwrap();
}

pub async fn bench_write(op: Operator, path: &str, content: Vec<u8>) {
    let w = op.object(path).writer();
    w.write_bytes(content).await.unwrap();
}
