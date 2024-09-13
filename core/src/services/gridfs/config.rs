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
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

/// Config for Grid file system support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct GridfsConfig {
    /// The connection string of the MongoDB service.
    pub connection_string: Option<String>,
    /// The database name of the MongoDB GridFs service to read/write.
    pub database: Option<String>,
    /// The bucket name of the MongoDB GridFs service to read/write.
    pub bucket: Option<String>,
    /// The chunk size of the MongoDB GridFs service used to break the user file into chunks.
    pub chunk_size: Option<u32>,
    /// The working directory, all operations will be performed under it.
    pub root: Option<String>,
}

impl Debug for GridfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GridFsConfig")
            .field("database", &self.database)
            .field("bucket", &self.bucket)
            .field("chunk_size", &self.chunk_size)
            .field("root", &self.root)
            .finish()
    }
}
