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

        bench_write_once(c, op.clone());
    }
}

fn bench_write_once(c: &mut Criterion, op: Operator) {
    let mut group = c.benchmark_group("write_once");

    let mut rng = thread_rng();

    for size in [
        Size::Kibibytes(4_usize),
        Size::Kibibytes(256),
        Size::Mebibytes(4),
        Size::Mebibytes(16),
    ] {
        let content = gen_bytes(&mut rng, size.bytes() as usize);
        let path = uuid::Uuid::new_v4().to_string();
        let temp_data = TempData::existing(op.clone(), &path);

        group.throughput(criterion::Throughput::Bytes(size.bytes()));
        group.bench_with_input(
            size.to_string(Base::Base2, Style::Abbreviated),
            &(op.clone(), &path, content),
            |b, (op, path, content)| {
                b.to_async(&*TOKIO).iter(|| async {
                    let w = op.object(path).writer();
                    w.write_bytes(content.clone()).await.unwrap();
                })
            },
        );

        std::mem::drop(temp_data);
    }

    group.finish()
}
