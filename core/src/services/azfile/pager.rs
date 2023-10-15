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

use std::sync::Arc;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::de;

use super::core::AzfileCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct AzfilePager {
    core: Arc<AzfileCore>,

    path: String,
    limit: Option<usize>,

    continuation: String,
    done: bool,
}

impl AzfilePager {
    pub fn new(core: Arc<AzfileCore>, path: String, limit: Option<usize>) -> Self {
        Self {
            core,
            path,
            limit,

            continuation: "".to_string(),
            done: false,
        }
    }
}

#[async_trait]
impl oio::Page for AzfilePager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        todo!();
    }
}
