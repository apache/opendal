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

/// Config for Mongodb service support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MongodbConfig {
    /// connection string of this backend
    pub connection_string: Option<String>,
    /// database of this backend
    pub database: Option<String>,
    /// collection of this backend
    pub collection: Option<String>,
    /// root of this backend
    pub root: Option<String>,
    /// key field of this backend
    pub key_field: Option<String>,
    /// value field of this backend
    pub value_field: Option<String>,
}

impl Debug for MongodbConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongodbConfig")
            .field("connection_string", &self.connection_string)
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("root", &self.root)
            .field("key_field", &self.key_field)
            .field("value_field", &self.value_field)
            .finish()
    }
}
