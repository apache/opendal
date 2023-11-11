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

use super::core::AlluxioCore;
use crate::raw::oio::Entry;
use crate::raw::*;
use crate::Result;

pub struct AlluxioPager {
    core: Arc<AlluxioCore>,

    path: String,

    done: bool,
}

impl AlluxioPager {
    pub(super) fn new(core: Arc<AlluxioCore>, path: &str) -> Self {
        AlluxioPager {
            core,
            path: path.to_string(),
            done: false,
        }
    }
}

#[async_trait]
impl oio::Page for AlluxioPager {
    async fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        if self.done {
            return Ok(None);
        }

        let file_infos = self.core.list_status(&self.path).await?;

        let mut entries = vec![];
        for file_info in file_infos {
            let path: String = file_info.path.clone();
            entries.push(Entry::new(&path, file_info.try_into()?));
        }

        if entries.is_empty() {
            return Ok(None);
        }

        self.done = true;

        Ok(Some(entries))
    }
}
