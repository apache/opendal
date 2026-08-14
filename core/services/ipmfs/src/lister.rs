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
use serde::Deserialize;

use super::core::IpmfsCore;
use super::core::parse_error;
use opendal_core::EntryMode;
use opendal_core::ErrorKind;
use opendal_core::Metadata;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::*;

pub struct IpmfsLister {
    core: Arc<IpmfsCore>,
    ctx: OperationContext,
    root: String,
    path: String,
}

impl IpmfsLister {
    pub fn new(core: Arc<IpmfsCore>, ctx: OperationContext, root: &str, path: &str) -> Self {
        Self {
            core,
            ctx,
            root: root.to_string(),
            path: path.to_string(),
        }
    }
}

/// Build a listed entry's path from the path being listed and one `files/ls` name.
///
/// `list_path` is the path the operator handed to the service, so it is already relative to the
/// root -- `IpmfsCore::ipmfs_ls` is what turns it into a rooted absolute path for the request.
/// Concatenating a name onto it therefore yields a root-relative path directly, and stripping the
/// root off it a second time is what used to truncate the result (or panic outright).
///
/// The one thing the concatenation has to handle is that `list_path` is `"/"` when the root
/// itself is listed, and an entry path carries no leading slash.
fn build_entry_path(list_path: &str, name: &str, mode: EntryMode) -> String {
    let prefix = if list_path == "/" { "" } else { list_path };

    match mode {
        EntryMode::FILE => format!("{prefix}{name}"),
        EntryMode::DIR => format!("{prefix}{name}/"),
        EntryMode::Unknown => unreachable!(),
    }
}

impl oio::PageList for IpmfsLister {
    async fn next_page(&self, ctx: &mut oio::PageContext) -> Result<()> {
        let resp = self.core.ipmfs_ls(&self.ctx, &self.path).await?;

        if resp.status() != StatusCode::OK {
            let err = parse_error(resp);
            if matches!(err.kind(), ErrorKind::NotFound) {
                // treat as empty listing
                ctx.done = true;
                return Ok(());
            }
            return Err(err);
        }

        // Add current directory entry when processing the first page
        if ctx.token.is_empty() && !ctx.done {
            let path = build_abs_path(&self.root, self.path.as_str());
            let path = build_rel_path(&self.root, &path);

            ctx.entries
                .push_back(oio::Entry::new(&path, Metadata::new(EntryMode::DIR)));
        }

        let bs = resp.into_body();
        let entries_body: IpfsLsResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        // Mark dir stream has been consumed.
        ctx.done = true;

        for object in entries_body.entries.unwrap_or_default() {
            let path = build_entry_path(&self.path, &object.name, object.mode());

            ctx.entries.push_back(oio::Entry::new(
                &path,
                Metadata::new(object.mode()).with_content_length(object.size),
            ));
        }

        Ok(())
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
    fn mode(&self) -> EntryMode {
        match &self.file_type {
            1 => EntryMode::DIR,
            0 | 2 => EntryMode::FILE,
            _ => EntryMode::Unknown,
        }
    }
}

#[derive(Deserialize, Default, Debug)]
#[serde(default)]
struct IpfsLsResponse {
    #[serde(rename = "Entries")]
    entries: Option<Vec<IpfsLsResponseEntry>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listing_the_root_yields_bare_names() {
        // `list_path` is "/" here, and an entry path carries no leading slash.
        assert_eq!(build_entry_path("/", "a.txt", EntryMode::FILE), "a.txt");
        assert_eq!(build_entry_path("/", "sub", EntryMode::DIR), "sub/");
    }

    #[test]
    fn listing_a_directory_keeps_that_directory_in_the_entry_path() {
        assert_eq!(
            build_entry_path("dir/", "a.txt", EntryMode::FILE),
            "dir/a.txt"
        );
        assert_eq!(build_entry_path("dir/", "sub", EntryMode::DIR), "dir/sub/");
    }

    #[test]
    fn the_entry_path_is_independent_of_the_service_root() {
        // This is the property the previous expression lacked. It ran the concatenation through
        // `build_rel_path(root, ..)`, which under the default root "/" is a no-op -- and under
        // any other root removed a prefix that was never there. `build_entry_path` takes no root
        // at all, so the same listing produces the same entry paths whatever the root is.
        assert_eq!(
            build_entry_path("dir/", "a.txt", EntryMode::FILE),
            "dir/a.txt"
        );
        assert_eq!(build_entry_path("/", "a.txt", EntryMode::FILE), "a.txt");
    }
}
