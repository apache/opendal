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

use std::collections::HashMap;
use std::env;

use opendal::layers::LoggingLayer;
use opendal::Operator;
use opendal::Scheme;
use rand::prelude::*;

/// Init a service with given scheme.
///
/// - If `opendal_{schema}_test` is on, construct a new Operator with given root.
/// - Else, returns a `None` to represent no valid config for operator.
pub fn init_service(scheme: Scheme) -> Option<Operator> {
    let _ = env_logger::builder().is_test(true).try_init();
    let _ = dotenv::dotenv();

    let prefix = format!("opendal_{}_", scheme);

    let cfg = env::vars()
        .filter_map(|(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        })
        .collect::<HashMap<String, String>>();

    if cfg.get("test").cloned().unwrap_or_default() != "on" {
        return None;
    }

    let op = Operator::from_iter(scheme, cfg.into_iter())
        .expect("init service must succeed")
        .layer(LoggingLayer);

    Some(op)
}

pub fn gen_bytes() -> (Vec<u8>, usize) {
    let mut rng = thread_rng();

    let size = rng.gen_range(1..4 * 1024 * 1024);
    let mut content = vec![0; size as usize];
    rng.fill_bytes(&mut content);

    (content, size)
}

pub fn gen_fixed_bytes(size: usize) -> Vec<u8> {
    let mut rng = thread_rng();

    let mut content = vec![0; size];
    rng.fill_bytes(&mut content);

    content
}

pub fn gen_offset_length(size: usize) -> (u64, u64) {
    let mut rng = thread_rng();

    // Make sure at least one byte is read.
    let offset = rng.gen_range(0..size - 1);
    let length = rng.gen_range(1..(size - offset));

    (offset as u64, length as u64)
}
