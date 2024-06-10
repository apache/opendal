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

use std::sync::OnceLock;

use opendal::raw::tests;
use opendal::Capability;
use tempfile::TempDir;
use test_context::TestContext;
use tokio::runtime::Runtime;
use tokio::runtime::{self};

static INIT_LOGGER: OnceLock<()> = OnceLock::new();
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub struct OfsTestContext {
    pub mount_point: TempDir,
    pub capability: Capability,
    mount_handle: ofs::fuse::MountHandle,
}

impl TestContext for OfsTestContext {
    fn setup() -> Self {
        let backend = tests::init_test_service()
            .expect("init test services failed")
            .expect("no test services has been configured");
        let capability = backend.info().full_capability();

        INIT_LOGGER.get_or_init(env_logger::init);

        let mount_point = tempfile::tempdir().unwrap();
        let mount_point_str = mount_point.path().to_string_lossy().to_string();
        let mount_handle = RUNTIME
            .get_or_init(|| {
                runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("build runtime")
            })
            .block_on(async move {
                ofs::fuse::Fuse::new()
                    .mount_with_unprivileged(mount_point_str, backend)
                    .await
            })
            .unwrap();

        OfsTestContext {
            mount_point,
            capability,
            mount_handle,
        }
    }

    // We don't care if the unmount fails, so we ignore the result.
    fn teardown(self) {
        let _ = RUNTIME
            .get()
            .expect("runtime")
            .block_on(async move { self.mount_handle.unmount().await });
        let _ = self.mount_point.close();
    }
}
