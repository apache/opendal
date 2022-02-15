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
use criterion::{criterion_group, criterion_main, Criterion};
use opendal::credential::Credential;

async fn init() {
    opendal::services::s3::Backend::build()
        .bucket("test")
        .region("test")
        .credential(Credential::hmac("test", "test"))
        .finish()
        .await
        .unwrap();
}

fn criterion_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("s3_init", |b| {
        b.to_async(&runtime).iter(|| init());
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
