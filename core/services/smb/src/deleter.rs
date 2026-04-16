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

use super::core::SmbCore;
use opendal_core::raw::*;
use opendal_core::*;

pub struct SmbDeleter {
    core: Arc<SmbCore>,
}

impl SmbDeleter {
    pub fn new(core: Arc<SmbCore>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for SmbDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let client = self.core.connect().await?;
        let is_dir = path.ends_with('/');
        let path = self.core.build_relative_path(&path);
        self.core.delete_path(&client, &path, is_dir).await
    }
}
