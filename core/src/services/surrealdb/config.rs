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

/// Config for Surrealdb services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SurrealdbConfig {
    /// The connection string for surrealdb.
    pub connection_string: Option<String>,
    /// The username for surrealdb.
    pub username: Option<String>,
    /// The password for surrealdb.
    pub password: Option<String>,
    /// The namespace for surrealdb.
    pub namespace: Option<String>,
    /// The database for surrealdb.
    pub database: Option<String>,
    /// The table for surrealdb.
    pub table: Option<String>,
    /// The key field for surrealdb.
    pub key_field: Option<String>,
    /// The value field for surrealdb.
    pub value_field: Option<String>,
    /// The root for surrealdb.
    pub root: Option<String>,
}

impl Debug for SurrealdbConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SurrealdbConfig");

        d.field("connection_string", &self.connection_string)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root)
            .finish()
    }
}
