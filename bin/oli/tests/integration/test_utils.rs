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

// re-export test library utils
pub use assert_cmd::prelude::*;
pub use tempfile::tempdir;

use std::{
    path::{self, PathBuf},
    process::Command,
};

pub fn oli() -> Command {
    Command::new(insta_cmd::get_cargo_bin("oli"))
}

fn format_path(path: &impl AsRef<path::Path>) -> String {
    let mut path = path.as_ref().to_string_lossy().into_owned();
    // Note: in some places, e.g. in [`directory_snapshot`] to have a better control of the output,
    // we need to do regex replacements manually, outside of the insta filters. Otherwise, the
    // tabular output will be broken.
    for (pattern, replacement) in &*REPLACEMENT_REGEXS {
        path = pattern.replace_all(&path, replacement).into_owned();
    }
    path
}

pub fn directory_snapshot(dir: impl AsRef<path::Path>) -> DirectorySnapshot {
    DirectorySnapshot {
        dir: dir.as_ref().to_path_buf(),
        // default formatting options
        with_type: true,
        with_size: false,
        with_content: false,
    }
}

pub struct DirectorySnapshot {
    dir: PathBuf,

    with_type: bool,
    with_size: bool,
    with_content: bool,
}

impl DirectorySnapshot {
    pub fn with_type(mut self, with_type: bool) -> Self {
        self.with_type = with_type;
        self
    }
    pub fn with_size(mut self, with_size: bool) -> Self {
        self.with_size = with_size;
        self
    }
    pub fn with_content(mut self, with_content: bool) -> Self {
        self.with_content = with_content;
        self
    }
}

impl std::fmt::Display for DirectorySnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let walker = walkdir::WalkDir::new(self.dir.as_path())
            .sort_by_file_name()
            .into_iter();
        let mut table = comfy_table::Table::new();
        table.load_preset(comfy_table::presets::ASCII_BORDERS_ONLY_CONDENSED);
        let mut header = vec!["Path"];
        if self.with_type {
            header.push("Type");
        }
        if self.with_size {
            header.push("Size (bytes)");
        }
        if self.with_content {
            header.push("Content");
        }
        table.set_header(header);
        for entry in walker {
            let entry = entry.unwrap();
            let mut row = Vec::new();
            row.push(format_path(&entry.path()));
            if self.with_type {
                row.push(
                    if entry.file_type().is_dir() {
                        "DIR"
                    } else if entry.file_type().is_symlink() {
                        "SYMLINK"
                    } else if entry.file_type().is_file() {
                        "FILE"
                    } else {
                        "OTHER"
                    }
                    .to_string(),
                );
            }
            if self.with_size {
                row.push(format!("{}", entry.metadata().unwrap().len()));
            }
            if self.with_content && entry.file_type().is_file() {
                let content = std::fs::read_to_string(entry.path()).unwrap();
                row.push(content.to_string());
            }
            table.add_row(row);
        }
        write!(f, "{table}")
    }
}

macro_rules! assert_cmd_snapshot {
    ($spawnable:expr, @$snapshot:literal $(,)?) => {{
        apply_common_filters!();
        insta_cmd::assert_cmd_snapshot!($spawnable, @$snapshot);
    }};
}
pub(crate) use assert_cmd_snapshot;

macro_rules! assert_snapshot {
    ($($arg:tt)*) => {{
        apply_common_filters!();
        insta::assert_snapshot!($($arg)*);
    }};
}
pub(crate) use assert_snapshot;

pub const REPLACEMENTS: &[(&str, &str)] = &[
    (r"(?:/|(\s))\S*\.tmp[^/]+", "$1[TEMP_DIR]"), // New regex for specific /.tmpXXXX patterns
    (
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{9} UTC)",
        "[TIMESTAMP]",
    ),
];
pub static REPLACEMENT_REGEXS: std::sync::LazyLock<Vec<(regex::Regex, String)>> =
    std::sync::LazyLock::new(|| {
        REPLACEMENTS
            .iter()
            .map(|(pattern, replacement)| {
                (regex::Regex::new(pattern).unwrap(), replacement.to_string())
            })
            .collect()
    });

macro_rules! apply_common_filters {
    {} => {
        let mut settings = insta::Settings::clone_current();
        for (pattern, replacement) in &*REPLACEMENTS {
            settings.add_filter(pattern, *replacement);
        }
        let _bound = settings.bind_to_scope();
    }
}

pub(crate) use apply_common_filters;
