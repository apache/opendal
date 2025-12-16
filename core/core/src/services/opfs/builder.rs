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

use crate::{
    Builder, Capability, Configurator, Error, ErrorKind, Result,
    raw::{Access, AccessorInfo},
};

use super::{OPFS_SCHEME, backend::OpfsBackend, config::OpfsConfig, core::OpfsCore};

impl Configurator for OpfsConfig {
    type Builder = OpfsBuilder;

    fn into_builder(self) -> Self::Builder {
        OpfsBuilder::new(self)
    }
}

/// OPFS[https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system] backend support.
#[derive(Default)]
pub struct OpfsBuilder {
    config: OpfsConfig,
}

impl OpfsBuilder {
    pub(crate) fn new(config: OpfsConfig) -> Self {
        Self { config }
    }

    /// Set root for backend.
    #[allow(unused)]
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for OpfsBuilder {
    type Config = OpfsConfig;

    fn build(self) -> Result<impl Access> {
        let root = self.config.root.ok_or(
            Error::new(ErrorKind::ConfigInvalid, "root is not specified")
                .with_operation("Builder::build"),
        )?;

        let info = AccessorInfo::default();
        info.set_scheme(OPFS_SCHEME)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,
                create_dir: true,
                delete: true,
                delete_with_recursive: true,
                ..Default::default()
            });

        let core = Arc::new(OpfsCore::new(Arc::new(info), root));

        Ok(OpfsBackend::new(core))
    }
}
