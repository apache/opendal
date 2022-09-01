use std::fmt::format;
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
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;

criterion_group!(benches, bench_build_abs_path);
criterion_main!(benches);

fn bench_build_abs_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_abs_path");

    let root = format!("/{}", "a".repeat(1024));

    group.bench_function("slice", |b| {
        b.iter(|| {
            let _: String = root[1..].to_string();
        })
    });
    group.bench_function("trim_start_matches", |b| {
        b.iter(|| {
            let _: String = root.trim_start_matches('/').to_string();
        })
    });
    group.bench_function("chars", |b| {
        b.iter(|| {
            let _: String = root.chars().take(1).collect();
        })
    });

    group.bench_function("concat_with_string", |b| {
        b.iter(|| {
            let _: String = root[1..].to_string() + &root;
        })
    });
    group.bench_function("concat_with_format", |b| {
        b.iter(|| {
            let _: String = format!("{}{}", &root[1..], &root);
        })
    });
}
