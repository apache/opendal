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

/// Config for Libsql services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct LibsqlConfig {
    /// Connection string for libsql service.
    pub connection_string: Option<String>,
    /// Authentication token for libsql service.
    pub auth_token: Option<String>,

    /// Table name for libsql service.
    pub table: Option<String>,
    /// Key field name for libsql service.
    pub key_field: Option<String>,
    /// Value field name for libsql service.
    pub value_field: Option<String>,
    /// Root for libsql service.
    pub root: Option<String>,
}

impl Debug for LibsqlConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("LibsqlConfig");
        ds.field("connection_string", &self.connection_string)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root);

        if self.auth_token.is_some() {
            ds.field("auth_token", &"<redacted>");
        }

        ds.finish()
    }
}
