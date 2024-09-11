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

/// Config for Atomicserver services support
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct AtomicserverConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// endpoint of this backend
    pub endpoint: Option<String>,
    /// private_key of this backend
    pub private_key: Option<String>,
    /// public_key of this backend
    pub public_key: Option<String>,
    /// parent_resource_id of this backend
    pub parent_resource_id: Option<String>,
}

impl Debug for AtomicserverConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AtomicserverConfig")
            .field("root", &self.root)
            .field("endpoint", &self.endpoint)
            .field("public_key", &self.public_key)
            .field("parent_resource_id", &self.parent_resource_id)
            .finish_non_exhaustive()
    }
}
