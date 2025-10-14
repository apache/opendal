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
use super::backend::D1Builder;

/// Config for [Cloudflare D1](https://developers.cloudflare.com/d1) backend support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct D1Config {
    /// Set the token of cloudflare api.
    pub token: Option<String>,
    /// Set the account id of cloudflare api.
    pub account_id: Option<String>,
    /// Set the database id of cloudflare api.
    pub database_id: Option<String>,

    /// Set the working directory of OpenDAL.
    pub root: Option<String>,
    /// Set the table of D1 Database.
    pub table: Option<String>,
    /// Set the key field of D1 Database.
    pub key_field: Option<String>,
    /// Set the value field of D1 Database.
    pub value_field: Option<String>,
}

impl Debug for D1Config {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("D1Config");
        ds.field("root", &self.root);
        ds.field("table", &self.table);
        ds.field("key_field", &self.key_field);
        ds.field("value_field", &self.value_field);
        ds.finish_non_exhaustive()
    }
}

impl crate::Configurator for D1Config {
    type Builder = D1Builder;
    fn into_builder(self) -> Self::Builder {
        D1Builder {
            config: self,
            http_client: None,
        }
    }
}

