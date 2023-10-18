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
use std::str::FromStr;

use opendal::Operator;
use opendal::Scheme;

pub fn init_service() -> Option<Operator> {
    let scheme = if let Ok(v) = env::var("OPENDAL_TEST") {
        v
    } else {
        return None;
    };
    let scheme = Scheme::from_str(&scheme).unwrap();

    let args: Vec<String> = env::args().collect();
    if args[0].ends_with(&scheme.to_string()) {
        // if not exist, fallback to .env
        let _ = dotenvy::from_filename_override(format!(".{scheme}.env"));
    }

    let prefix = format!("opendal_{}_", scheme);
    let envs = env::vars()
        .filter_map(move |(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        })
        .collect();

    Some(Operator::via_map(scheme, envs).unwrap_or_else(|_| panic!("init {} must succeed", scheme)))
}
