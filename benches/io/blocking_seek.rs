// Copyright 2023 Datafuse Labs.
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
use std::io::{Seek, SeekFrom};

use opendal::{
    layers::{LoggingLayer, MetricsLayer, RetryLayer, TracingLayer},
    raw::output::BlockingRead,
    services::fs,
    Operator, OperatorNext,
};
use rand::prelude::*;

// pub fn bench(c: &mut Criterion) {
//     let mut group = c.benchmark_group("blocking_seek");
//     let size = 64 * 1024 * 1024;

//     let op = Operator::from_iter(
//         opendal::Scheme::Fs,
//         vec![("root".to_string(), "/tmp".to_string())].into_iter(),
//     )
//     .unwrap();

//     op.object("test_file")
//         .blocking_write(vec![0; size])
//         .unwrap();

//     let op = op
//         .layer(LoggingLayer::default())
//         .layer(TracingLayer)
//         .layer(MetricsLayer)
//         .layer(RetryLayer::new(backon::ExponentialBackoff::default()));

//     let mut rng = rand::thread_rng();

//     let o = op.object("test_file");
//     let mut r = o.blocking_reader().unwrap();

//     group.bench_function("seek", |b| {
//         b.iter(|| {
//             let off = rng.gen_range(0..size as u64);
//             r.seek(SeekFrom::Start(off)).unwrap();
//         })
//     });
// }

pub fn bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("blocking_seek");
    let size = 64 * 1024 * 1024;

    let op = OperatorNext {
        accessor: {
            let mut builder = fs::Builder::default();
            builder.root("/tmp");
            builder.build_next().unwrap()
        },
    }
    .layer(MetricsLayer);

    // let op = Operator::from_iter(
    //     opendal::Scheme::Fs,
    //     vec![("root".to_string(), "/tmp".to_string())].into_iter(),
    // )
    // .unwrap();

    // op.object("test_file")
    //     .blocking_write(vec![0; size])
    //     .unwrap();

    // let op = op
    //     .layer(LoggingLayer::default())
    //     .layer(TracingLayer)
    //     .layer(MetricsLayer)
    //     .layer(RetryLayer::new(backon::ExponentialBackoff::default()));

    let mut rng = rand::thread_rng();

    let mut r = op.blocking_reader("test_file").unwrap();

    group.bench_function("seek", |b| {
        b.iter(|| {
            let off = rng.gen_range(0..size as u64);
            std::io::Seek::seek(&mut r, SeekFrom::Start(off)).unwrap();
        })
    });
}
