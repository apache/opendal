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

use bytes::Buf;
use http::StatusCode;
use opendal_core::raw::*;
use opendal_core::*;

use super::core::*;
use super::error::parse_error;

pub struct PcloudLister {
    core: Arc<PcloudCore>,

    path: String,
    recursive: bool,
}

impl PcloudLister {
    pub(super) fn new(core: Arc<PcloudCore>, path: &str, recursive: bool) -> Self {
        PcloudLister {
            core,
            path: path.to_string(),
            recursive,
        }
    }
}

fn append_entries(
    entries: &mut std::collections::VecDeque<oio::Entry>,
    core: &PcloudCore,
    root: &str,
    parent_path: &str,
    content: ListMetadata,
    recursive: bool,
) -> Result<()> {
    let mut absolute_path = content
        .path
        .clone()
        .unwrap_or_else(|| format!("{parent_path}/{}", content.name));
    if content.isfolder {
        absolute_path.push('/');
    } else if let Some(file_id) = content.fileid {
        core.cache_file_id(&absolute_path, file_id);
    }

    let md = parse_list_metadata(&content)?;
    let relative_path = build_rel_path(root, &absolute_path);
    entries.push_back(oio::Entry::new(&relative_path, md));

    if recursive {
        if let Some(contents) = content.contents {
            for child in contents {
                append_entries(
                    entries,
                    core,
                    root,
                    absolute_path.trim_end_matches('/'),
                    child,
                    true,
                )?;
            }
        }
    }

    Ok(())
}

impl oio::PageList for PcloudLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self.core.list_folder(&self.path, self.recursive).await?;

        let status = resp.status();

        match status {
            StatusCode::OK => {
                let bs = resp.into_body();

                let resp: ListFolderResponse = serde_json::from_reader(bs.clone().reader())
                    .map_err(new_json_deserialize_error)?;
                let result = resp.result;

                if result == 2005 {
                    ctx.done = true;
                    return Ok(());
                }

                if result != 0 {
                    return Err(Error::new(ErrorKind::Unexpected, format!("{resp:?}")));
                }

                if let Some(metadata) = resp.metadata {
                    let parent_path = metadata.path.as_deref().unwrap_or(&self.path);
                    if let Some(contents) = metadata.contents {
                        for content in contents {
                            append_entries(
                                &mut ctx.entries,
                                &self.core,
                                &self.core.root,
                                parent_path,
                                content,
                                self.recursive,
                            )?;
                        }
                    }

                    ctx.done = true;
                    return Ok(());
                }

                Err(Error::new(
                    ErrorKind::Unexpected,
                    String::from_utf8_lossy(&bs.to_bytes()),
                ))
            }
            _ => Err(parse_error(resp)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use opendal_core::EntryMode;
    use opendal_core::raw::AccessorInfo;

    use super::ListMetadata;
    use super::PcloudCore;
    use super::append_entries;

    fn core() -> PcloudCore {
        PcloudCore::new(
            Arc::new(AccessorInfo::default()),
            "/repo/".to_string(),
            "https://api.pcloud.com".to_string(),
            "user@example.com".to_string(),
            "secret".to_string(),
        )
    }

    fn file(path: &str, size: u64) -> ListMetadata {
        ListMetadata {
            path: Some(path.to_string()),
            name: path.rsplit('/').next().unwrap_or(path).to_string(),
            modified: "Mon, 18 May 2026 18:00:17 +0000".to_string(),
            isfolder: false,
            fileid: Some(1),
            size: Some(size),
            contents: None,
        }
    }

    fn dir(path: &str, contents: Vec<ListMetadata>) -> ListMetadata {
        ListMetadata {
            path: Some(path.to_string()),
            name: path.rsplit('/').next().unwrap_or(path).to_string(),
            modified: "Mon, 18 May 2026 18:00:17 +0000".to_string(),
            isfolder: true,
            fileid: None,
            size: None,
            contents: Some(contents),
        }
    }

    #[test]
    fn append_entries_flattens_recursive_contents() {
        let mut entries = VecDeque::new();

        append_entries(
            &mut entries,
            &core(),
            "/repo/",
            "/repo",
            dir(
                "/repo/data/00",
                vec![
                    file("/repo/data/00/pack1", 11),
                    dir(
                        "/repo/data/00/sub",
                        vec![file("/repo/data/00/sub/pack2", 22)],
                    ),
                ],
            ),
            true,
        )
        .expect("entries should flatten");

        let paths: Vec<_> = entries
            .iter()
            .map(|entry| entry.path().to_string())
            .collect();
        assert_eq!(
            paths,
            vec![
                "data/00/",
                "data/00/pack1",
                "data/00/sub/",
                "data/00/sub/pack2"
            ]
        );
        assert_eq!(entries[0].mode(), EntryMode::DIR);
        assert_eq!(entries[1].mode(), EntryMode::FILE);
        assert_eq!(entries[3].mode(), EntryMode::FILE);
    }

    #[test]
    fn append_entries_keeps_non_recursive_listing_shallow() {
        let mut entries = VecDeque::new();

        append_entries(
            &mut entries,
            &core(),
            "/repo/",
            "/repo",
            dir(
                "/repo/data/00",
                vec![
                    file("/repo/data/00/pack1", 11),
                    dir(
                        "/repo/data/00/sub",
                        vec![file("/repo/data/00/sub/pack2", 22)],
                    ),
                ],
            ),
            false,
        )
        .expect("entries should flatten");

        let paths: Vec<_> = entries
            .iter()
            .map(|entry| entry.path().to_string())
            .collect();
        assert_eq!(paths, vec!["data/00/"]);
    }

    #[test]
    fn append_entries_rebuilds_missing_child_paths() {
        let mut entries = VecDeque::new();

        append_entries(
            &mut entries,
            &core(),
            "/repo/",
            "/repo/keys",
            ListMetadata {
                path: None,
                name: "file1".to_string(),
                modified: "Mon, 18 May 2026 18:00:17 +0000".to_string(),
                isfolder: false,
                fileid: Some(1),
                size: Some(363),
                contents: None,
            },
            true,
        )
        .expect("entries should rebuild missing child paths");

        let paths: Vec<_> = entries
            .iter()
            .map(|entry| entry.path().to_string())
            .collect();
        assert_eq!(paths, vec!["keys/file1"]);
    }
}
