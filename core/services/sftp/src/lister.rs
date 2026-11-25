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

use std::path::Path;
use std::pin::Pin;

use futures::StreamExt;
use openssh_sftp_client::fs::ReadDir;
use openssh_sftp_client::metadata::MetaData as SftpMeta;

use super::core::SftpConnection;
use super::core::parse_sftp_error;
use super::core::to_metadata;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::MetadataBuilder;
use opendal_core::Result;
use opendal_core::raw::Timestamp;
use opendal_core::raw::oio;
use opendal_core::raw::oio::Entry;

pub struct SftpLister {
    dir: Option<Pin<Box<ReadDir>>>,
    prefix: String,
    // Keep the connection checked out until after the directory handle drops.
    _conn: Option<SftpConnection>,
}

impl SftpLister {
    pub fn new(dir: ReadDir, path: String, conn: SftpConnection) -> Self {
        let prefix = if path == "/" { "".to_owned() } else { path };

        SftpLister {
            dir: Some(Box::pin(dir)),
            prefix,
            _conn: Some(conn),
        }
    }
}

impl oio::List for SftpLister {
    async fn next(&mut self) -> Result<Option<Entry>> {
        loop {
            let Some(dir) = self.dir.as_mut() else {
                return Ok(None);
            };
            let item = dir.next().await.transpose().map_err(parse_sftp_error)?;

            match item {
                Some(e) => {
                    let filename = e.filename();
                    if filename == Path::new("..") {
                        continue;
                    } else if filename == Path::new(".") {
                        return Ok(Some(map_current_dir_entry(
                            self.prefix.as_str(),
                            e.metadata(),
                        )));
                    } else {
                        return Ok(Some(map_entry(
                            self.prefix.as_str(),
                            filename,
                            e.metadata(),
                        )?));
                    }
                }
                None => {
                    // Drop the directory handle before releasing the pooled connection.
                    self.dir = None;
                    self._conn = None;
                    return Ok(None);
                }
            }
        }
    }
}

fn map_current_dir_entry(prefix: &str, meta: SftpMeta) -> Entry {
    let path = if prefix.is_empty() { "/" } else { prefix };
    let mut metadata = MetadataBuilder::dir();
    if let Some(modified) = meta.modified()
        && let Ok(m) = Timestamp::try_from(modified.as_system_time())
    {
        metadata.last_modified(m);
    }

    Entry::new(path, metadata.build())
}

fn map_entry(prefix: &str, filename: &Path, meta: SftpMeta) -> Result<Entry> {
    let filename = filename
        .to_str()
        .ok_or_else(|| Error::new(ErrorKind::Unexpected, "sftp filename is not valid utf-8"))?;
    let is_dir = meta.file_type().is_some_and(|ft| ft.is_dir());
    let path = format!("{prefix}{filename}{}", if is_dir { "/" } else { "" });

    Ok(Entry::new(path.as_str(), to_metadata(meta)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::EntryMode;
    use openssh_sftp_client::metadata::{MetaDataBuilder, Permissions, RawPermissions};

    #[test]
    fn map_entry_handles_missing_permissions() {
        let meta = MetaDataBuilder::new().create();
        let entry = map_entry("dir/", Path::new("entry"), meta).unwrap();
        assert_eq!(entry.path(), "dir/entry");
        assert_eq!(entry.mode(), EntryMode::Unknown);
    }

    #[test]
    fn map_current_dir_entry_handles_missing_permissions() {
        let meta = MetaDataBuilder::new().create();
        let root_entry = map_current_dir_entry("", meta);
        assert_eq!(root_entry.path(), "/");
        assert_eq!(root_entry.mode(), EntryMode::DIR);

        let sub_entry = map_current_dir_entry("dir/", MetaDataBuilder::new().create());
        assert_eq!(sub_entry.path(), "dir/");
        assert_eq!(sub_entry.mode(), EntryMode::DIR);
    }

    #[test]
    fn map_entry_maps_directory_and_file() {
        let dir_perm = Permissions::from_raw(RawPermissions::from_bits_retain(0o040_755));
        let dir_meta = MetaDataBuilder::new().permissions(dir_perm).create();
        let dir_entry = map_entry("prefix/", Path::new("sub"), dir_meta).unwrap();
        assert_eq!(dir_entry.path(), "prefix/sub/");
        assert_eq!(dir_entry.mode(), EntryMode::DIR);

        let file_perm = Permissions::from_raw(RawPermissions::from_bits_retain(0o100_644));
        let file_meta = MetaDataBuilder::new()
            .permissions(file_perm)
            .len(42)
            .create();
        let file_entry = map_entry("prefix/", Path::new("data.bin"), file_meta).unwrap();
        assert_eq!(file_entry.path(), "prefix/data.bin");
        assert_eq!(file_entry.mode(), EntryMode::FILE);
        assert_eq!(file_entry.metadata().content_length(), 42);
    }

    #[cfg(unix)]
    #[test]
    fn map_entry_rejects_non_utf8_filename_without_panic() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let filename = Path::new(OsStr::from_bytes(b"bad-\xff-name"));
        let err = map_entry("", filename, MetaDataBuilder::new().create())
            .expect_err("non-utf8 filename must return an error");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }
}
