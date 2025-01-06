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
use serde::Serialize;

/// Config for Mysql services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct NebulaGraphConfig {
    /// The host addr of nebulagraph's graphd server
    pub host: Option<String>,
    /// The host port of nebulagraph's graphd server
    pub port: Option<u16>,
    /// The username of nebulagraph's graphd server
    pub username: Option<String>,
    /// The password of nebulagraph's graphd server
    pub password: Option<String>,

    /// The space name of nebulagraph's graphd server
    pub space: Option<String>,
    /// The tag name of nebulagraph's graphd server
    pub tag: Option<String>,
    /// The key field name of the NebulaGraph service to read/write.
    pub key_field: Option<String>,
    /// The value field name of the NebulaGraph service to read/write.
    pub value_field: Option<String>,
    /// The root for NebulaGraph
    pub root: Option<String>,
}

impl Debug for NebulaGraphConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("NebulaGraphConfig");

        d.field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &self.password)
            .field("space", &self.space)
            .field("tag", &self.tag)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root)
            .finish()
    }
}
