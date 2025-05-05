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

use super::backend::HdfsNativeBackend;
use super::error::parse_hdfs_error;
use crate::raw::*;
use crate::*;
use std::sync::Arc;

pub struct HdfsNativeDeleter {
    core: Arc<HdfsNativeBackend>,
}

impl HdfsNativeDeleter {
    pub fn new(core: Arc<HdfsNativeBackend>) -> Self {
        Self { core }
    }
}

impl oio::OneShotDelete for HdfsNativeDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let p = build_rooted_abs_path(&self.core.root, &path);

        self.core
            .client
            .delete(&p, true)
            .await
            .map_err(parse_hdfs_error)?;

        Ok(())
    }
}
