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

use std::process::{Child, Command};

use assert_cmd::cargo::CommandCargoExt;
use tempfile::TempDir;
use test_context::AsyncTestContext;

pub(crate) struct OfsTestContext {
    pub mount_point: TempDir,
    pub root: TempDir,
    pub ofs_process: Child,
}

impl AsyncTestContext for OfsTestContext {
    async fn setup() -> Self {
        let mount_point = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let cmd = Command::cargo_bin("ofs")
            .unwrap()
            .args([
                mount_point.path().to_str().unwrap(),
                format!("fs://?root={}", root.path().to_string_lossy()).as_str(),
            ])
            .spawn()
            .unwrap();

        OfsTestContext {
            mount_point,
            root,
            ofs_process: cmd,
        }
    }

    async fn teardown(mut self) {
        Command::new("fusermount3")
            .args(["-u", self.mount_point.path().to_str().unwrap()])
            .output()
            .unwrap();
        self.ofs_process.kill().unwrap();
        self.mount_point.close().unwrap();
        self.root.close().unwrap();
    }
}
