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

use rand::prelude::*;

pub fn gen_bytes() -> (Vec<u8>, usize) {
    let mut rng = thread_rng();

    let size = rng.gen_range(1..4 * 1024 * 1024);
    let mut content = vec![0; size as usize];
    rng.fill_bytes(&mut content);

    (content, size)
}

pub fn gen_offset_length(size: usize) -> (u64, u64) {
    let mut rng = thread_rng();

    // Make sure at least one byte is read.
    let offset = rng.gen_range(0..size - 1);
    let length = rng.gen_range(1..(size - offset));

    (offset as u64, length as u64)
}
