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

use super::backend::DbfsBuilder;
use serde::Deserialize;
use serde::Serialize;

/// [Dbfs](https://docs.databricks.com/api/azure/workspace/dbfs)'s REST API support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct DbfsConfig {
    /// The root for dbfs.
    pub root: Option<String>,
    /// The endpoint for dbfs.
    pub endpoint: Option<String>,
    /// The token for dbfs.
    pub token: Option<String>,
}

impl Debug for DbfsConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("DbfsConfig");

        ds.field("root", &self.root);
        ds.field("endpoint", &self.endpoint);

        if self.token.is_some() {
            ds.field("token", &"<redacted>");
        }

        ds.finish()
    }
}

impl crate::Configurator for DbfsConfig {
    type Builder = DbfsBuilder;
    fn into_builder(self) -> Self::Builder {
        DbfsBuilder { config: self }
    }
}
