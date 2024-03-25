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

use std::{collections::HashMap, env, process::Command};

use tempfile::TempDir;
use test_context::AsyncTestContext;
use tokio::task::JoinHandle;

pub(crate) struct OfsTestContext {
    pub mount_point: TempDir,
    ofs_task: JoinHandle<anyhow::Result<()>>,
}

impl AsyncTestContext for OfsTestContext {
    async fn setup() -> Self {
        let backend = backend_scheme().unwrap();

        let mount_point = tempfile::tempdir().unwrap();

        let ofs_task = tokio::spawn(ofs::execute(ofs::Config {
            mount_path: mount_point.path().to_string_lossy().to_string(),
            backend: backend.parse().unwrap(),
        }));

        OfsTestContext {
            mount_point,
            ofs_task,
        }
    }

    async fn teardown(self) {
        // FIXME: ofs could not unmount
        Command::new("fusermount3")
            .args(["-u", self.mount_point.path().to_str().unwrap()])
            .output()
            .unwrap();

        self.ofs_task.abort();
        self.mount_point.close().unwrap();
    }
}

fn backend_scheme() -> Option<String> {
    let scheme = env::var("OPENDAL_TEST").ok()?;
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

    let params = cfg
        .into_iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(&k), urlencoding::encode(&v)))
        .collect::<Vec<_>>()
        .join("&");

    Some(format!("{scheme}://?{params}"))
}
