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

use super::core::CompioThread;
use crate::raw::*;
use crate::*;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::PathBuf;

/// [`compio`]-based file system support.
#[derive(Debug, Clone, Default)]
pub struct CompFSBuilder {
    root: Option<PathBuf>,
}

impl CompFSBuilder {
    /// Set root for CompFS
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(PathBuf::from(root))
        };

        self
    }
}

impl Builder for CompFSBuilder {
    const SCHEME: Scheme = Scheme::CompFS;
    type Accessor = ();

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = CompFSBuilder::default();

        map.get("root").map(|v| builder.root(v));

        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        todo!()
    }
}

#[derive(Debug)]
pub struct CompFSBackend {
    rt: CompioThread,
}

#[async_trait]
impl Accessor for CompFSBackend {
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
