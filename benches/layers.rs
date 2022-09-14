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

fn bench_tracing_layer(c: &mut Criterion) {
    let mut group = c.benchmark_group("tracing_layers");

    let _ = dotenv::dotenv();
    let op = Operator::from_env(Scheme::S3).expect("init operator must succeed");
    let layered_op = op
        .clone()
        .layer(RetryLayer::new(ExponentialBackoff::default()))
        .layer(LoggingLayer)
        .layer(TracingLayer)
        .layer(MetricsLayer);

    group.bench_function("with_layer", |b| {
        b.iter(|| {
            let _ = layered_op.metadata();
        })
    });
    group.bench_function("without_layer", |b| {
        b.iter(|| {
            let _ = op.metadata();
        })
    });

    group.finish()
}
