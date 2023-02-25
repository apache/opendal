// Copyright 2022 Datafuse Labs
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
use futures::AsyncWriteExt;
use futures::StreamExt;
use once_cell::sync::Lazy;
use opendal::raw::input::into_stream;

pub static TOKIO: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().expect("build tokio runtime"));

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("into_stream");
    let size = 4 * 1024 * 1024;

    group.throughput(criterion::Throughput::Bytes(size));
    group.bench_function("into_stream", |b| {
        b.to_async(&*TOKIO).iter(|| async {
            let r = io::Cursor::new(vec![1_u8; size as usize]);
            let mut s = into_stream(r, 8 * 1024);

            let mut si = io::sink();

            while let Some(b) = s.next().await {
                si.write_all(&b.expect("stream must valid"))
                    .await
                    .expect("write must success");
            }
        })
    });

    group.throughput(criterion::Throughput::Bytes(size));
    group.bench_function("raw_reader", |b| {
        b.to_async(&*TOKIO).iter(|| async {
            let r = io::Cursor::new(vec![1_u8; size as usize]);

            let mut si = io::sink();
            io::copy(r, &mut si).await.expect("copy must success");
        })
    });
}
