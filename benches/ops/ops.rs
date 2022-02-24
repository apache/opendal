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

use criterion::BenchmarkId;
use criterion::Criterion;
use futures::io;
use futures::io::BufReader;
use opendal::Operator;
use rand::prelude::*;

use super::fs;
use super::s3;

pub fn bench(c: &mut Criterion) {
    dotenv::from_filename(".env").ok();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut rng = thread_rng();

    let size = 16 * 1024 * 1024; // Test with 16MB.
    let content = gen_bytes(&mut rng, size);

    let cases = vec![
        ("fs", runtime.block_on(fs::init())),
        ("s3", runtime.block_on(s3::init())),
    ];

    for case in cases {
        if case.1.is_err() {
            continue;
        }
        let op = case.1.unwrap();
        let path = uuid::Uuid::new_v4().to_string();

        // Write file before test.
        runtime
            .block_on(op.object(&path).writer().write_bytes(content.clone()))
            .expect("write failed");

        let mut group = c.benchmark_group(case.0);
        group.throughput(criterion::Throughput::Bytes(size as u64));
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
    let mut r = op.object(path).reader().total_size(16 * 1024 * 1024);
    io::copy(&mut r, &mut io::sink()).await.unwrap();
}

pub async fn bench_buf_read(op: Operator, path: &str) {
    let r = op.object(path).reader().total_size(16 * 1024 * 1024);
    let mut r = BufReader::with_capacity(4 * 1024 * 1024, r);

    io::copy(&mut r, &mut io::sink()).await.unwrap();
}

pub async fn bench_write(op: Operator, path: &str, content: Vec<u8>) {
    let w = op.object(path).writer();
    w.write_bytes(content).await.unwrap();
}
