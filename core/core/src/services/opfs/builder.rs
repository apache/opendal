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

use crate::{Builder, Configurator, Error, ErrorKind, Result, raw::Access};

use super::{backend::OpfsBackend, config::OpfsConfig, core::OpfsCore};

impl Configurator for OpfsConfig {
    type Builder = OpfsBuilder;

    fn into_builder(self) -> Self::Builder {
        OpfsBuilder::new(self)
    }
}

#[derive(Default)]
pub struct OpfsBuilder {
    config: OpfsConfig,
}

impl OpfsBuilder {
    pub(crate) fn new(config: OpfsConfig) -> Self {
        Self { config }
    }

    /// Set root for backend.
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

        let core = Arc::new(OpfsCore::new(root));
        Ok(OpfsBackend::new(core))
    }
}
