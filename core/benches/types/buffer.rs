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

use size::Size;
use bytes::Buf;
use opendal::Buffer;
use rand::thread_rng;
use criterion::Criterion;

use super::utils::*;

pub fn bench_contiguous_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_contiguous_buffer");

    let mut rng = thread_rng();

    for size in [
        Size::from_kibibytes(256),
        Size::from_mebibytes(4),
    ] {
        let bytes_buf = gen_bytes(&mut rng, size.bytes() as usize);
        let buffer = Buffer::from(bytes_buf.clone());

        let bytes_buf_name = format!("bytes buf {}", size.to_string());
        let buffer_name = format!("contiguous buffer {}", size.to_string());

        group.bench_function(format!("{} {}", bytes_buf_name, "chunk"), |b| b.iter(|| bytes_buf.chunk() ));
        group.bench_function(format!("{} {}", buffer_name, "chunk"), |b| b.iter(|| buffer.chunk() ));

        group.bench_function(format!("{} {}", bytes_buf_name, "advance"), |b| b.iter(|| {
            let mut bytes_buf = bytes_buf.clone();
            bytes_buf.advance((size.bytes() as f64 * 0.5) as usize);
        }));
        group.bench_function(format!("{} {}", buffer_name, "advance"), |b| b.iter(|| {
            let mut buffer = buffer.clone();
            // Advance non-integer number of Bytes.
            buffer.advance((size.bytes() as f64 * 0.5) as usize);
        }));
    }

    group.finish()
}

pub fn bench_non_contiguous_buffer(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_non_contiguous_buffer");

    let mut rng = thread_rng();

    for size in [
        Size::from_kibibytes(256),
        Size::from_mebibytes(4),
    ] {
        for num in [4, 32] {
            let bytes_buf = gen_bytes(&mut rng, size.bytes() as usize * num);
            let mut bytes_vec = vec![];
            for _ in 0..num {
                bytes_vec.push(gen_bytes(&mut rng, size.bytes() as usize));
            }
            let buffer = Buffer::from(bytes_vec);

            let bytes_buf_name = format!("bytes buf {} * {} ", size.to_string(), num);
            let buffer_name = format!("non contiguous buffer {} * {}", size.to_string(), num);
    
            group.bench_function(format!("{} {}", bytes_buf_name, "chunk"), |b| b.iter(|| bytes_buf.chunk() ));
            group.bench_function(format!("{} {}", buffer_name, "chunk"), |b| b.iter(|| buffer.chunk() ));

            group.bench_function(format!("{} {}", bytes_buf_name, "advance"), |b| b.iter(|| {
                let mut bytes_buf = bytes_buf.clone();
                bytes_buf.advance((size.bytes() as f64 * 3.5) as usize);
            }));
            group.bench_function(format!("{} {}", buffer_name, "advance"), |b| b.iter(|| {
                let mut buffer = buffer.clone();
                // Advance non-integer number of Bytes.
                buffer.advance((size.bytes() as f64 * 3.5) as usize);
            }));
        }
    }

    group.finish()
}
