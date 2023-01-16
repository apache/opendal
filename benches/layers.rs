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

use backon::ExponentialBackoff;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use once_cell::sync::Lazy;
use opendal::layers::LoggingLayer;
use opendal::layers::MetricsLayer;
use opendal::layers::RetryLayer;
use opendal::layers::TracingLayer;
use opendal::Operator;
use opendal::Scheme;

criterion_group!(benches, bench_tracing_layer);
criterion_main!(benches);

pub static TOKIO: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().expect("build tokio runtime"));

/// TODO: This bench's result is weired.
///
/// read_with_layer is much faster than read. That's impossible.
/// Something must be wrong.
fn bench_tracing_layer(c: &mut Criterion) {
    let mut group = c.benchmark_group("tracing_layers");

    let _ = dotenvy::dotenv();
    let op = Operator::from_env(Scheme::S3).expect("init operator must succeed");
    let layered_op = Operator::from_env(Scheme::S3)
        .expect("init operator must succeed")
        .layer(RetryLayer::new(ExponentialBackoff::default()))
        .layer(LoggingLayer::default())
        .layer(TracingLayer)
        .layer(MetricsLayer);
    TOKIO.block_on(async {
        op.object("test")
            .write("0".repeat(16 * 1024 * 1024).into_bytes())
            .await
            .expect("write must succeed")
    });

    group.bench_function("metadata", |b| {
        b.iter(|| {
            let _ = op.metadata();
        })
    });
    group.bench_function("metadata_with_layer", |b| {
        b.iter(|| {
            let _ = layered_op.metadata();
        })
    });

    group.bench_function("read", |b| {
        b.to_async(&*TOKIO).iter(|| async {
            let _ = op.object("test").read().await.expect("read must succeed");
        })
    });
    group.bench_function("read_with_layer", |b| {
        b.to_async(&*TOKIO).iter(|| async {
            let _ = layered_op
                .object("test")
                .read()
                .await
                .expect("read must succeed");
        })
    });

    group.finish()
}
