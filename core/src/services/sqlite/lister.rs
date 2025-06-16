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
    pub async fn new(
        core: Arc<SqliteCore>,
        path: &str,
        root: String,
        options: crate::raw::OpList,
    ) -> crate::Result<Self> {
        let path_clean = path.trim_end_matches('/');

        // Build SQL query based on path and recursive flag
        let (query, bindings) = if path.is_empty() || path == "/" {
            // Root path listing
            if options.recursive() {
                // Return all entries
                (
                    format!("SELECT `{}` FROM `{}`", core.key_field, core.table),
                    vec![],
                )
            } else {
                // Return only top-level entries
                (format!(
                    "SELECT `{}` FROM `{}` WHERE `{}` NOT LIKE '%/%' OR (`{}` LIKE '%/' AND `{}` NOT LIKE '%/%/')",
                    core.key_field, core.table, core.key_field, core.key_field, core.key_field
                ), vec![])
            }
        } else if !path.ends_with('/') && !options.recursive() {
            // Path without trailing slash: could be a file or directory
            // First check if it exists as a file
            let file_exists: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM `{}` WHERE `{}` = $1",
                core.table, core.key_field
            ))
            .bind(path_clean)
            .fetch_one(core.get_client().await?)
            .await
            .map_err(crate::services::sqlite::backend::parse_sqlite_error)?;

            if file_exists > 0 {
                // Return the file itself
                return Ok(Self {
                    root,
                    iter_to_end: false,
                    iter: Box::new(vec![Ok(path_clean.to_string())].into_iter()),
                });
            }

            // Check if it exists as a directory
            let dir_exists: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM `{}` WHERE `{}` LIKE $1",
                core.table, core.key_field
            ))
            .bind(format!("{path_clean}/%"))
            .fetch_one(core.get_client().await?)
            .await
            .map_err(crate::services::sqlite::backend::parse_sqlite_error)?;

            if dir_exists > 0 {
                // Return the directory itself
                return Ok(Self {
                    root,
                    iter_to_end: false,
                    iter: Box::new(vec![Ok(format!("{path_clean}/"))].into_iter()),
                });
            }
            // Check for prefix matches (for compatibility with prefix-based listing)
            let prefix_results: Vec<crate::Result<String>> = sqlx::query_scalar(&format!(
                "SELECT `{}` FROM `{}` WHERE `{}` LIKE $1 AND `{}` NOT LIKE '%/%'",
                core.key_field, core.table, core.key_field, core.key_field
            ))
            .bind(format!("{path_clean}%"))
            .fetch(core.get_client().await?)
            .map(|v| v.map_err(crate::services::sqlite::backend::parse_sqlite_error))
            .collect::<Vec<_>>()
            .await;
            if !prefix_results.is_empty() {
                // Return prefix matches
                return Ok(Self {
                    root,
                    iter_to_end: false,
                    iter: Box::new(prefix_results.into_iter()),
                });
            } else {
                // No matches found
                return Ok(Self {
                    root,
                    iter_to_end: false,
                    iter: Box::new(vec![].into_iter()),
                });
            }
        } else if options.recursive() {
            // Recursive listing under a specific path
            (
                format!(
                    "SELECT `{}` FROM `{}` WHERE `{}` LIKE $1 OR `{}` = $2",
                    core.key_field, core.table, core.key_field, core.key_field
                ),
                vec![format!("{}%", path), format!("{}/", path_clean)],
            )
        } else {
            // Non-recursive listing under a specific path
            (format!(
                "SELECT `{}` FROM `{}` WHERE (`{}` LIKE $1 OR `{}` = $2) AND (`{}` NOT LIKE $3 OR (`{}` LIKE $4 AND `{}` NOT LIKE $5))",
                core.key_field, core.table, core.key_field, core.key_field, core.key_field, core.key_field, core.key_field
            ), vec![
                format!("{}%", path),
                format!("{}/", path_clean),
                format!("{}/%/%", path_clean),
                format!("{}/%/", path_clean),
                format!("{}/%/%/", path_clean),
            ])
        };

        // Execute query
        let mut query_builder = sqlx::query_scalar(&query);
        for binding in bindings {
            query_builder = query_builder.bind(binding);
        }

        let results: Vec<crate::Result<String>> = query_builder
            .fetch(core.get_client().await?)
            .map(|v| v.map_err(crate::services::sqlite::backend::parse_sqlite_error))
            .collect::<Vec<_>>()
            .await;

        Ok(Self {
            root,
            iter_to_end: false,
            iter: Box::new(results.into_iter()),
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
