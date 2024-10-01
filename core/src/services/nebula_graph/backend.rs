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

use crate::services::NebulaGraphConfig;

#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct NebulaGraphBuilder {
    config: NebulaGraphConfig,
}

impl Debug for NebulaGraphBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("MysqlBuilder");

        d.field("config", &self.config).finish()
    }
}

impl NebulaGraphBuilder {
    /// Set the host addr of nebulagraph's graphd server
    pub fn host(&mut self, host: &str) -> &mut Self {
        if !host.is_empty() {
            self.config.host = Some(host.to_string());
        }
        self
    }

    /// Set the host port of nebulagraph's graphd server
    pub fn port(&mut self, port: u16) -> &mut Self {
        self.config.port = Some(port);
        self
    }

    /// Set the username of nebulagraph's graphd server
    pub fn username(&mut self, username: &str) -> &mut Self {
        if !username.is_empty() {
            self.config.username = Some(username.to_string());
        }
        self
    }

    /// Set the password of nebulagraph's graphd server
    pub fn password(&mut self, password: &str) -> &mut Self {
        if !password.is_empty() {
            self.config.password = Some(password.to_string());
        }
        self
    }

    /// Set the space name of nebulagraph's graphd server
    pub fn space(&mut self, space: &str) -> &mut Self {
        if !space.is_empty() {
            self.config.space = Some(space.to_string());
        }
        self
    }

    /// Set the tag name of nebulagraph's graphd server
    pub fn tag(&mut self, tag: &str) -> &mut Self {
        if !tag.is_empty() {
            self.config.tag = Some(tag.to_string());
        }
        self
    }

    /// Set the key field name of the NebulaGraph service to read/write.
    ///
    /// Default to `key` if not specified.
    pub fn key_field(&mut self, key_field: &str) -> &mut Self {
        if !key_field.is_empty() {
            self.config.key_field = Some(key_field.to_string());
        }
        self
    }

    /// Set the value field name of the NebulaGraph service to read/write.
    ///
    /// Default to `value` if not specified.
    pub fn value_field(&mut self, value_field: &str) -> &mut Self {
        if !value_field.is_empty() {
            self.config.value_field = Some(value_field.to_string());
        }
        self
    }

    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub fn root(&mut self, root: &str) -> &mut Self {
        if !root.is_empty() {
            self.config.root = Some(root.to_string());
        }
        self
    }
}
