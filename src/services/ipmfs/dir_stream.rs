// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde::Deserialize;

use super::backend::IpmfsBackend;
use super::error::parse_error;
use crate::raw::*;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Result;

pub struct DirStream {
    backend: Arc<IpmfsBackend>,
    root: String,
    path: String,
    consumed: bool,
}

impl DirStream {
    pub fn new(backend: Arc<IpmfsBackend>, root: &str, path: &str) -> Self {
        Self {
            backend,
            root: root.to_string(),
            path: path.to_string(),
            consumed: false,
        }
    }
}

#[async_trait]
impl output::Page for DirStream {
    async fn next_page(&mut self) -> Result<Option<Vec<output::Entry>>> {
        if self.consumed {
            return Ok(None);
        }

        let resp = self.backend.ipmfs_ls(&self.path).await?;

        if resp.status() != StatusCode::OK {
            return Err(parse_error(resp).await?);
        }

        let bs = resp.into_body().bytes().await?;
        let entries_body: IpfsLsResponse =
            serde_json::from_slice(&bs).map_err(new_json_deserialize_error)?;

        // Mark dir stream has been consumed.
        self.consumed = true;

        Ok(Some(
            entries_body
                .entries
                .unwrap_or_default()
                .into_iter()
                .map(|object| {
                    let path = match object.mode() {
                        ObjectMode::FILE => {
                            format!("{}{}", &self.path, object.name)
                        }
                        ObjectMode::DIR => {
                            format!("{}{}/", &self.path, object.name)
                        }
                        ObjectMode::Unknown => unreachable!(),
                    };

                    let path = build_rel_path(&self.root, &path);

                    output::Entry::new(
                        &path,
                        ObjectMetadata::new(object.mode()).with_content_length(object.size),
                    )
                })
                .collect(),
        ))
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponseEntry {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Type")]
    file_type: i64,
    #[serde(rename = "Size")]
    size: u64,
}

impl IpfsLsResponseEntry {
    /// ref: <https://github.com/ipfs/specs/blob/main/UNIXFS.md#data-format>
    ///
    /// ```protobuf
    /// enum DataType {
    ///     Raw = 0;
    ///     Directory = 1;
    ///     File = 2;
    ///     Metadata = 3;
    ///     Symlink = 4;
    ///     HAMTShard = 5;
    /// }
    /// ```
    fn mode(&self) -> ObjectMode {
        match &self.file_type {
            1 => ObjectMode::DIR,
            0 | 2 => ObjectMode::FILE,
            _ => ObjectMode::Unknown,
        }
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponse {
    #[serde(rename = "Entries")]
    entries: Option<Vec<IpfsLsResponseEntry>>,
}
