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

use bytes::Buf;
use criterion::Criterion;
use once_cell::sync::Lazy;
use opendal::raw::oio::ExactBufWriter;
use opendal::raw::oio::Write;
use rand::thread_rng;
use size::Size;

use super::utils::*;

pub static TOKIO: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().expect("build tokio runtime"));

pub fn bench_exact_buf_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("exact_buf_write");

    let mut rng = thread_rng();

    for size in [
        Size::from_kibibytes(4),
        Size::from_kibibytes(256),
        Size::from_mebibytes(4),
        Size::from_mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, size.bytes() as usize);

        group.throughput(criterion::Throughput::Bytes(size.bytes() as u64));
        group.bench_with_input(size.to_string(), &content, |b, content| {
            b.to_async(&*TOKIO).iter(|| async {
                let mut w = ExactBufWriter::new(BlackHoleWriter, 256 * 1024);

                let mut bs = content.clone();
                while !bs.is_empty() {
                    let n =w.write(bs.clone().into()).await.unwrap() ;
                    bs.advance(n);
                }
                w.close().await.unwrap();
            })
        });
    }

    group.finish()
}
