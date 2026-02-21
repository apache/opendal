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

use std::sync::Arc;

use super::OPFS_SCHEME;
use super::config::OpfsConfig;
use super::utils::*;
use opendal_core::raw::*;
use opendal_core::*;

#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct OpfsBuilder {
    pub(super) config: OpfsConfig,
}

impl Builder for OpfsBuilder {
    type Config = OpfsConfig;

    fn build(self) -> Result<impl Access> {
        Ok(OpfsBackend {})
    }
}

/// OPFS Service backend
#[derive(Default, Debug, Clone)]
pub struct OpfsBackend {}

impl Access for OpfsBackend {
    type Reader = ();

    type Writer = ();

    type Lister = ();

    type Deleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        let info = AccessorInfo::default();
        info.set_scheme(OPFS_SCHEME);
        info.set_name("opfs");
        info.set_root("/");
        info.set_native_capability(Capability {
            create_dir: true,
            ..Default::default()
        });
        Arc::new(info)
    }

    async fn create_dir(&self, path: &str, _: OpCreateDir) -> Result<RpCreateDir> {
        debug_assert!(path != "/", "root path should be handled upstream");
        get_directory_handle(path, true).await?;
        Ok(RpCreateDir::default())
    }
}
