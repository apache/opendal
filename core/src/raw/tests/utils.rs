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

use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use std::sync::LazyLock;

use crate::*;

/// TEST_RUNTIME is the runtime used for running tests.
pub static TEST_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

/// Init a service with given scheme.
///
/// - Load scheme from `OPENDAL_TEST`
/// - Construct a new Operator with given root.
/// - Else, returns a `None` to represent no valid config for operator.
pub fn init_test_service() -> Result<Option<Operator>> {
    let _ = dotenvy::dotenv();

    let scheme = if let Ok(v) = env::var("OPENDAL_TEST") {
        v
    } else {
        return Ok(None);
    };
    let scheme = Scheme::from_str(&scheme).unwrap();

    let prefix = format!("opendal_{scheme}_");

    let mut cfg = env::vars()
        .filter_map(|(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        })
        .collect::<HashMap<String, String>>();

    // Use random root unless OPENDAL_DISABLE_RANDOM_ROOT is set to true.
    let disable_random_root = env::var("OPENDAL_DISABLE_RANDOM_ROOT").unwrap_or_default() == "true";
    if !disable_random_root {
        let root = format!(
            "{}{}/",
            cfg.get("root").cloned().unwrap_or_else(|| "/".to_string()),
            uuid::Uuid::new_v4()
        );
        cfg.insert("root".to_string(), root);
    }

    let op = Operator::via_iter(scheme, cfg).expect("must succeed");

    #[cfg(feature = "layers-chaos")]
    let op = { op.layer(layers::ChaosLayer::new(0.1)) };

    let mut op = op
        .layer(layers::LoggingLayer::default())
        .layer(layers::TimeoutLayer::new())
        .layer(layers::RetryLayer::new().with_max_times(4));

    // Enable blocking layer if needed.
    if !op.info().full_capability().blocking {
        // Don't enable blocking layer for compfs
        if op.info().scheme() != Scheme::Compfs {
            let _guard = TEST_RUNTIME.enter();
            op = op.layer(layers::BlockingLayer::create().expect("blocking layer must be created"));
        }
    }

    Ok(Some(op))
}
