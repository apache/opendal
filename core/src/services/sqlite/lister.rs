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

use crate::raw::build_rel_path;
use crate::raw::oio;
use crate::services::sqlite::core::SqliteCore;
use crate::{EntryMode, Metadata};
use futures::StreamExt;
use std::sync::Arc;

pub struct SqliteLister {
    root: String,
    iter: Box<dyn Iterator<Item = crate::Result<String>> + Send + Sync>,
    iter_to_end: bool,
}

impl SqliteLister {
    pub async fn new(core: Arc<SqliteCore>, path: &str, root: String) -> crate::Result<Self> {
        let folder_path_clean = path.trim_end_matches('/');
        let search_pattern = format!("{}/%", folder_path_clean);
        Ok(Self {
            root,
            iter_to_end: false,
            iter: Box::new(
                sqlx::query_scalar(&format!(
                    r#"SELECT `{}` FROM `{}` 
                   WHERE `{}` (LIKE $1 OR LIKE $5)
                   AND (
                       `{}` NOT LIKE $2 OR  -- Not nested content
                       (`{}` LIKE $3 AND `{}` NOT LIKE $4)  -- Or is a direct subdirectory
                   )"#,
                    core.key_field,
                    core.table,
                    core.key_field,
                    core.key_field,
                    core.key_field,
                    core.key_field
                ))
                .bind(search_pattern.clone())
                .bind(format!("{}/%/%", folder_path_clean)) // Exclude nested content
                .bind(format!("{}/%/", folder_path_clean)) // But allow direct subdirs
                .bind(format!("{}/%/%/", folder_path_clean))
                .bind(format!("{}/", folder_path_clean))
                .fetch(core.get_client().await?)
                .map(|v| v.map_err(crate::services::sqlite::backend::parse_sqlite_error))
                .collect::<Vec<_>>()
                .await
                .into_iter(),
            ),
        })
    }
}

impl oio::List for SqliteLister {
    async fn next(&mut self) -> crate::Result<Option<oio::Entry>> {
        if self.iter_to_end {
            return Ok(None);
        }

        match self.iter.next() {
            Some(Err(e)) => Err(e),
            Some(Ok(r)) => Ok(Some(oio::Entry::new(
                &build_rel_path(&self.root, &r),
                Metadata::new(EntryMode::from_path(&r)),
            ))),
            None => {
                self.iter_to_end = true;
                Ok(None)
            }
        }
    }
}
