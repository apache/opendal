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

use super::core::SwiftCore;
use super::error::parse_error;
use crate::raw::*;
use crate::*;

pub struct SwiftLister {
    core: Arc<SwiftCore>,
    path: String,
    delimiter: &'static str,
}

impl SwiftLister {
    pub fn new(core: Arc<SwiftCore>, path: String, recursive: bool) -> Self {
        let delimiter = if recursive { "" } else { "/" };
        Self {
            core,
            path,
            delimiter,
        }
    }
}

#[async_trait]
impl oio::PageList for SwiftLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let response = self.core.swift_list(&self.path, self.delimiter).await?;

        let status_code = response.status();

        if !status_code.is_success() {
            let error = parse_error(response).await?;
            return Err(error);
        }

        ctx.done = true;

        let bytes = response.into_body().bytes().await?;
        let mut decoded_response = serde_json::from_slice::<Vec<ListOpResponse>>(&bytes)
            .map_err(new_json_deserialize_error)?;

        for status in decoded_response {
            let entry: oio::Entry = match status {
                ListOpResponse::Subdir { subdir } => {
                    let meta = Metadata::new(EntryMode::DIR);
                    oio::Entry::new(&subdir, meta)
                }
                ListOpResponse::FileInfo {
                    bytes,
                    hash,
                    name,
                    content_type,
                    mut last_modified,
                } => {
                    // this is the pseudo directory itself; we'll skip it.
                    if name == self.path {
                        continue;
                    }

                    let mut meta = Metadata::new(EntryMode::FILE);
                    meta.set_content_length(bytes);
                    meta.set_content_md5(hash.as_str());

                    // we'll change "2023-10-28T19:18:11.682610" to "2023-10-28T19:18:11.682610Z"
                    last_modified.push('Z');
                    meta.set_last_modified(parse_datetime_from_rfc3339(last_modified.as_str())?);

                    meta.set_content_type(content_type.as_str());

                    oio::Entry::new(&name, meta)
                }
            };
            ctx.entries.push_back(entry);
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize)]
#[serde(untagged)]
pub(super) enum ListOpResponse {
    Subdir {
        subdir: String,
    },
    FileInfo {
        bytes: u64,
        hash: String,
        name: String,
        content_type: String,
        last_modified: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_list_response_test() -> Result<()> {
        let resp = bytes::Bytes::from(
            r#"
            [
                {
                    "subdir": "animals/"
                },
                {
                    "subdir": "fruit/"
                },
                {
                    "bytes": 147,
                    "hash": "5e6b5b70b0426b1cc1968003e1afa5ad",
                    "name": "test.txt",
                    "content_type": "text/plain",
                    "last_modified": "2023-11-01T03:00:23.147480"
                }
            ]
            "#,
        );

        let mut out = serde_json::from_slice::<Vec<ListOpResponse>>(&resp)
            .map_err(new_json_deserialize_error)?;

        assert_eq!(out.len(), 3);
        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::FileInfo {
                bytes: 147,
                hash: "5e6b5b70b0426b1cc1968003e1afa5ad".to_string(),
                name: "test.txt".to_string(),
                content_type:
                    "multipart/form-data;boundary=------------------------25004a866ee9c0cb"
                        .to_string(),
                last_modified: "2023-11-01T03:00:23.147480".to_string(),
            }
        );

        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::Subdir {
                subdir: "fruit/".to_string()
            }
        );

        assert_eq!(
            out.pop().unwrap(),
            ListOpResponse::Subdir {
                subdir: "animals/".to_string()
            }
        );

        Ok(())
    }
}
