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

use std::env;

use opendal::services;
use opendal::Builder;
use opendal::Operator;

fn service<B: Builder>() -> Option<Operator> {
    let test_key = format!("opendal_{}_test", B::SCHEME).to_uppercase();
    if env::var(test_key).unwrap_or_default() != "on" {
        return None;
    }

    let prefix = format!("opendal_{}_", B::SCHEME);
    let envs = env::vars()
        .filter_map(move |(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        })
        .collect();

    Some(
        Operator::from_map::<B>(envs)
            .unwrap_or_else(|_| panic!("init {} must succeed", B::SCHEME))
            .finish(),
    )
}

pub fn init_services() -> Vec<(&'static str, Option<Operator>)> {
    vec![
        ("fs", service::<services::Fs>()),
        ("memory", service::<services::Memory>()),
        ("s3", service::<services::S3>()),
    ]
}
