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

use async_trait::async_trait;
use http::StatusCode;
use serde::Deserialize;

use super::backend::DbfsBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct DbfsPager {
    backend: DbfsBackend,
    path: String,
    done: bool,
}

impl DbfsPager {
    pub fn new(backend: DbfsBackend, path: String) -> Self {
        Self {
            backend,
            path,
            done: false,
        }
    }
}

#[async_trait]
impl oio::Page for DbfsPager {
    async fn next(&mut self) -> Result<Option<Vec<oio::Entry>>> {
        if self.done {
            return Ok(None);
        }

        let response = self.backend.dbfs_list(&self.path).await?;

        let status_code = response.status();
        if !status_code.is_success() {
            if status_code == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let error = parse_error(response).await?;
            return Err(error);
        }

        let bytes = response.into_body().bytes().await?;
        let mut decoded_response =
            serde_json::from_slice::<DbfsOutputList>(&bytes).map_err(new_json_deserialize_error)?;

        self.done = true;

        let mut entries = Vec::with_capacity(decoded_response.files.len());

        while let Some(status) = decoded_response.files.pop() {
            let entry: oio::Entry = match status.is_dir {
                true => {
                    let normalized_path = format!("{}/", &status.path);
                    let mut meta = Metadata::new(EntryMode::DIR);
                    meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
                        status.modification_time,
                    )?);
                    oio::Entry::new(&normalized_path, meta)
                }
                false => {
                    let mut meta = Metadata::new(EntryMode::FILE);
                    meta.set_last_modified(parse_datetime_from_from_timestamp_millis(
                        status.modification_time,
                    )?);
                    meta.set_content_length(status.file_size as u64);
                    oio::Entry::new(&status.path, meta)
                }
            };
            entries.push(entry);
        }
        Ok(Some(entries))
    }
}

#[derive(Debug, Deserialize)]
struct DbfsOutputList {
    files: Vec<DbfsStatus>,
}

#[derive(Debug, Deserialize)]
struct DbfsStatus {
    path: String,
    is_dir: bool,
    file_size: i64,
    modification_time: i64,
}
