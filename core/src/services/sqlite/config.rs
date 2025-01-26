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

/// Config for Sqlite support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct SqliteConfig {
    /// Set the connection_string of the sqlite service.
    ///
    /// This connection string is used to connect to the sqlite service.
    ///
    /// The format of connect string resembles the url format of the sqlite client:
    ///
    /// - `sqlite::memory:`
    /// - `sqlite:data.db`
    /// - `sqlite://data.db`
    ///
    /// For more information, please visit <https://docs.rs/sqlx/latest/sqlx/sqlite/struct.SqliteConnectOptions.html>.
    pub connection_string: Option<String>,

    /// Set the table name of the sqlite service to read/write.
    pub table: Option<String>,
    /// Set the key field name of the sqlite service to read/write.
    ///
    /// Default to `key` if not specified.
    pub key_field: Option<String>,
    /// Set the value field name of the sqlite service to read/write.
    ///
    /// Default to `value` if not specified.
    pub value_field: Option<String>,
    /// set the working directory, all operations will be performed under it.
    ///
    /// default: "/"
    pub root: Option<String>,
}

impl Debug for SqliteConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SqliteConfig");

        d.field("connection_string", &self.connection_string)
            .field("table", &self.table)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .field("root", &self.root);

        d.finish_non_exhaustive()
    }
}
