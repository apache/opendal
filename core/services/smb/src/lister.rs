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

use std::collections::VecDeque;
use std::sync::Arc;

use futures::TryStreamExt;
use log::debug;
use smb::{Directory, FileFullDirectoryInformation};

use super::core::SmbCore;
use opendal_core::EntryMode;
use opendal_core::Error;
use opendal_core::Metadata;
use opendal_core::Result;
use opendal_core::raw::oio;
use opendal_core::raw::oio::Entry;

pub struct SmbLister {
    entries: VecDeque<Entry>,
}

async fn close_dir_after_error(dir: &Directory, err: Error) -> Error {
    if let Err(close_err) = dir.close().await.map_err(super::error::parse_smb_error) {
        debug!("failed to close smb directory after error: {close_err:?}");
    }

    err
}

impl SmbLister {
    pub async fn new(
        core: Arc<SmbCore>,
        client: fastpool::bounded::Object<super::core::Manager>,
        path: String,
        abs_path: String,
        dir: Directory,
    ) -> Result<Self> {
        let mut entries = VecDeque::new();
        let list_path = if path == "/" {
            "/".to_string()
        } else {
            format!("{}/", path.trim_end_matches('/'))
        };
        entries.push_back(Entry::new(
            list_path.as_str(),
            Metadata::new(EntryMode::DIR),
        ));

        let dir = Arc::new(dir);
        let mut stream = match Directory::query::<FileFullDirectoryInformation>(&dir, "*").await {
            Ok(stream) => stream,
            Err(err) => {
                return Err(close_dir_after_error(
                    dir.as_ref(),
                    super::error::parse_smb_error(err),
                )
                .await);
            }
        };

        loop {
            let entry = match stream.try_next().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(err) => {
                    drop(stream);
                    return Err(close_dir_after_error(
                        dir.as_ref(),
                        super::error::parse_smb_error(err),
                    )
                    .await);
                }
            };

            let name = entry.file_name.to_string();
            if name == "." || name == ".." {
                continue;
            }

            let child_path = if path == "/" {
                name.clone()
            } else {
                format!("{}{}", list_path, name)
            };
            let child_abs_path = if abs_path.is_empty() {
                child_path.clone()
            } else if abs_path.ends_with('/') {
                format!("{abs_path}{name}")
            } else {
                format!("{abs_path}/{name}")
            };

            let meta = match core.stat_path(&client, &child_abs_path).await {
                Ok(meta) => meta,
                Err(err) if err.kind() == opendal_core::ErrorKind::NotFound => continue,
                Err(err) => {
                    drop(stream);
                    return Err(close_dir_after_error(dir.as_ref(), err).await);
                }
            };
            let entry_path = if meta.mode().is_dir() {
                format!("{}/", child_path.trim_end_matches('/'))
            } else {
                child_path
            };
            entries.push_back(Entry::new(entry_path.as_str(), meta));
        }

        drop(stream);
        dir.close().await.map_err(super::error::parse_smb_error)?;
        drop(client);

        Ok(Self { entries })
    }
}

impl oio::List for SmbLister {
    async fn next(&mut self) -> Result<Option<Entry>> {
        Ok(self.entries.pop_front())
    }
}
