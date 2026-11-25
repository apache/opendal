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

use std::str::FromStr;
use std::vec::IntoIter;

use suppaftp::list::File;

use opendal_core::raw::*;
use opendal_core::*;

/// Returns `true` for POSIX `LIST` summary lines such as `"total 0"` or `"total 8"`.
///
/// `suppaftp`'s `File::from_str` falls back to `ListParser::parse_mlst`, which
/// treats arbitrary strings without `=` as filenames, turning `"total 8"` into a
/// phantom 0-byte file entry.
fn is_posix_total_line(line: &str) -> bool {
    let trimmed = line.trim();
    let Some(rest) = trimmed.strip_prefix("total") else {
        return false;
    };
    let Some(first) = rest.chars().next() else {
        return false;
    };
    if !first.is_ascii_whitespace() {
        return false;
    }
    let count = rest.trim_start();
    !count.is_empty() && count.bytes().all(|b| b.is_ascii_digit())
}

/// Parse a single line returned by FTP `LIST`, returning `Ok(None)` for blank
/// lines, POSIX `"total N"` summary headers, and `"."` / `".."` directory entries.
pub(crate) fn parse_list_line(line: &str) -> Result<Option<File>> {
    if line.trim().is_empty() || is_posix_total_line(line) {
        return Ok(None);
    }

    let file = File::from_str(line)
        .map_err(|e| Error::new(ErrorKind::Unexpected, "parse file from response").set_source(e))?;

    if matches!(file.name(), "" | "." | "..") {
        return Ok(None);
    }

    Ok(Some(file))
}

pub struct FtpLister {
    path: String,
    file_iter: IntoIter<String>,
}

impl FtpLister {
    pub fn new(path: &str, files: Vec<String>) -> Self {
        Self {
            path: path.to_string(),
            file_iter: files.into_iter(),
        }
    }
}

impl oio::List for FtpLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let de = loop {
            let Some(file_str) = self.file_iter.next() else {
                return Ok(None);
            };
            if let Some(de) = parse_list_line(file_str.as_str())? {
                break de;
            }
        };

        let path = self.path.to_string() + de.name();

        let mut meta = if de.is_file() {
            MetadataBuilder::file(de.size() as u64)
        } else if de.is_directory() {
            MetadataBuilder::dir()
        } else {
            MetadataBuilder::unknown()
        };
        meta.last_modified(Timestamp::try_from(de.modified())?);

        let entry = if de.is_file() {
            oio::Entry::new(&path, meta.build())
        } else if de.is_directory() {
            oio::Entry::new(&format!("{}/", path), meta.build())
        } else {
            oio::Entry::new(&path, meta.build())
        };

        Ok(Some(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::raw::oio::List as _;

    #[tokio::test]
    async fn test_lister_filters_dots_total_header_and_blank_lines() {
        let lines = vec![
            "total 16".to_string(),
            "   ".to_string(),
            "drwxr-xr-x 2 user group 4096 Nov  5  2018 .".to_string(),
            "drwxr-xr-x 4 user group 4096 Nov  5  2018 ..".to_string(),
            "drwxr-xr-x 2 user group 4096 Nov  5  2018 subdir".to_string(),
            "-rw-r--r-- 1 user group   12 Nov  5  2018 .hidden".to_string(),
            "-rw-r--r-- 1 user group   42 Nov  5  2018 total 8".to_string(),
            "10-19-20  03:19PM       <DIR>          .".to_string(),
            "10-19-20  03:19PM       <DIR>          ..".to_string(),
            "10-19-20  03:19PM       <DIR>          dosdir".to_string(),
            "04-08-14  03:09PM                  128 dos.txt".to_string(),
        ];

        let mut lister = FtpLister::new("dir/", lines);
        let mut entries = Vec::new();
        while let Some(entry) = lister.next().await.unwrap() {
            entries.push((
                entry.path().to_string(),
                entry.metadata().mode(),
                entry.metadata().content_length(),
            ));
        }

        assert_eq!(
            entries,
            vec![
                ("dir/subdir/".to_string(), EntryMode::DIR, 0),
                ("dir/.hidden".to_string(), EntryMode::FILE, 12),
                ("dir/total 8".to_string(), EntryMode::FILE, 42),
                ("dir/dosdir/".to_string(), EntryMode::DIR, 0),
                ("dir/dos.txt".to_string(), EntryMode::FILE, 128),
            ]
        );
    }

    #[test]
    fn test_is_posix_total_line() {
        assert!(is_posix_total_line("total 0"));
        assert!(is_posix_total_line("total 8"));
        assert!(is_posix_total_line("  total   1234  "));
        assert!(!is_posix_total_line("total"));
        assert!(!is_posix_total_line("total abc"));
        assert!(!is_posix_total_line("total8"));
        assert!(!is_posix_total_line(
            "-rw-r--r-- 1 user group 42 Nov  5  2018 total 8"
        ));
    }
}
