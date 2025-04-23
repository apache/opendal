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

use std::{io::Result, sync::Arc};

use hdrs::Metadata;

use crate::raw::AccessorInfo;

#[derive(Clone, Debug)]
pub struct HdfsCore {
    info: Arc<AccessorInfo>,
    client: Arc<hdrs::Client>,
}

impl HdfsCore {
    /// Creates a new HdfsCore instance with the given AccessorInfo and client.
    pub fn new(info: Arc<AccessorInfo>, client: Arc<hdrs::Client>) -> Self {
        Self { info, client }
    }

    pub fn info(&self) -> Arc<AccessorInfo> {
        self.info.clone()
    }

    pub fn client(&self) -> Arc<hdrs::Client> {
        self.client.clone()
    }

    pub fn get_metadata(&self, path: &str) -> Result<Metadata> {
        let metadata = self.client.metadata(path)?;

        Ok(metadata)
    }
}
