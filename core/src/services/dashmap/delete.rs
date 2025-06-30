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

use super::core::DashmapCore;
use crate::raw::{build_abs_path, oio, OpDelete};
use crate::*;

pub struct DashmapDeleter {
    core: Arc<DashmapCore>,
    root: String,
}

impl DashmapDeleter {
    pub fn new(core: Arc<DashmapCore>, root: String) -> Self {
        Self { core, root }
    }
}

impl oio::OneShotDelete for DashmapDeleter {
    async fn delete_once(&self, path: String, _op: OpDelete) -> Result<()> {
        let p = build_abs_path(&self.root, &path);
        self.core.delete(&p)
    }
}
