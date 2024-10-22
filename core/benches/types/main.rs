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

mod buffer;
mod concurrent_tasks;
mod utils;

use criterion::criterion_group;
use criterion::criterion_main;

criterion_group!(
    benches,
    buffer::bench_non_contiguous_buffer,
    buffer::bench_non_contiguous_buffer_with_extreme,
    concurrent_tasks::bench_concurrent_tasks,
);
criterion_main!(benches);
