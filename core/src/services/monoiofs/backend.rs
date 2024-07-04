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

use std::fmt::Debug;

use serde::Deserialize;

use crate::raw::*;
use crate::*;

/// Config for monoiofs services support.
#[derive(Default, Deserialize, Debug)]
#[serde(default)]
#[non_exhaustive]
pub struct MonoiofsConfig {
    /// The Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Default to `/` if not set.
    pub root: Option<String>,
}

/// File system support via [`monoio`].
#[doc = include_str!("docs.md")]
#[derive(Default, Debug)]
pub struct MonoiofsBuilder {
    config: MonoiofsConfig,
}

impl MonoiofsBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }
}

impl Builder for MonoiofsBuilder {
    const SCHEME: Scheme = Scheme::Monoiofs;

    type Accessor = MonoiofsBackend;

    fn from_map(map: std::collections::HashMap<String, String>) -> Self {
        let config = MonoiofsConfig::deserialize(ConfigDeserializer::new(map))
            .expect("config deserialize must succeed");

        MonoiofsBuilder { config }
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct MonoiofsBackend {}

impl Access for MonoiofsBackend {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type BlockingReader = ();
    type BlockingWriter = ();
    type BlockingLister = ();

    fn info(&self) -> AccessorInfo {
        todo!()
    }
}
