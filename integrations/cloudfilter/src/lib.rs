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

mod file;

use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use cloud_filter::{
    error::{CResult, CloudErrorKind},
    filter::{info, ticket, SyncFilter},
    metadata::Metadata,
    placeholder::{ConvertOptions, Placeholder},
    placeholder_file::PlaceholderFile,
    request::Request,
    utility::{FileTime, WriteAt},
};
use file::FileBlob;
use opendal::{BlockingOperator, Entry, Metakey, Operator};

const BUF_SIZE: usize = 65536;

pub struct CloudFilter {
    op: BlockingOperator,
    root: PathBuf,
}

impl CloudFilter {
    pub fn new(op: Operator, root: PathBuf) -> Self {
        Self {
            op: op.blocking(),
            root,
        }
    }
}

impl SyncFilter for CloudFilter {
    fn fetch_data(
        &self,
        request: Request,
        ticket: ticket::FetchData,
        info: info::FetchData,
    ) -> CResult<()> {
        log::debug!("fetch_data: {}", request.path().display());

        let _blob = bincode::deserialize::<FileBlob>(request.file_blob()).map_err(|e| {
            log::warn!("failed to deserialize file blob: {}", e);
            CloudErrorKind::ValidationFailed
        })?;

        let range = info.required_file_range();
        let path = request.path();
        let remote_path = path
            .strip_prefix(&self.root)
            .map_err(|_| CloudErrorKind::NotUnderSyncRoot)?;

        let mut reader = self
            .op
            .reader_with(&remote_path.to_string_lossy().replace('\\', "/"))
            .call()
            .map_err(|e| {
                log::warn!("failed to open file: {}", e);
                CloudErrorKind::Unsuccessful
            })?
            .into_std_read(range.clone())
            .map_err(|e| {
                log::warn!("failed to read file: {}", e);
                CloudErrorKind::Unsuccessful
            })?;

        let mut position = range.start;
        let mut buffer = [0u8; BUF_SIZE];

        loop {
            let mut bytes_read = reader.read(&mut buffer).map_err(|e| {
                log::warn!("failed to read file: {}", e);
                CloudErrorKind::Unsuccessful
            })?;

            let unaligned = bytes_read % 4096;
            if unaligned != 0 && position + (bytes_read as u64) < range.end {
                bytes_read -= unaligned;
                reader
                    .seek(SeekFrom::Current(-(unaligned as i64)))
                    .map_err(|e| {
                        log::warn!("failed to seek file: {}", e);
                        CloudErrorKind::Unsuccessful
                    })?;
            }

            ticket
                .write_at(&buffer[..bytes_read], position)
                .map_err(|e| {
                    log::warn!("failed to write file: {}", e);
                    CloudErrorKind::Unsuccessful
                })?;
            position += bytes_read as u64;

            if position >= range.end {
                break;
            }

            ticket.report_progress(range.end, position).map_err(|e| {
                log::warn!("failed to report progress: {}", e);
                CloudErrorKind::Unsuccessful
            })?;
        }

        Ok(())
    }

    fn fetch_placeholders(
        &self,
        request: Request,
        ticket: ticket::FetchPlaceholders,
        _info: info::FetchPlaceholders,
    ) -> CResult<()> {
        log::debug!("fetch_placeholders: {}", request.path().display());

        let absolute = request.path();
        let mut remote_path = absolute
            .strip_prefix(&self.root)
            .map_err(|_| CloudErrorKind::NotUnderSyncRoot)?
            .to_owned();
        remote_path.push("");

        let now = FileTime::now();
        let mut entries = self
            .op
            .lister_with(&remote_path.to_string_lossy().replace('\\', "/"))
            .metakey(Metakey::LastModified | Metakey::ContentLength)
            .call()
            .map_err(|e| {
                log::warn!("failed to list files: {}", e);
                CloudErrorKind::Unsuccessful
            })?
            .filter_map(|e| {
                let entry = e.ok()?;
                let metadata = entry.metadata();
                let entry_remote_path = PathBuf::from(entry.path());
                let relative_path = entry_remote_path
                    .strip_prefix(&remote_path)
                    .expect("valid path");
                check_in_sync(&entry, &self.root).then(|| {
                    PlaceholderFile::new(relative_path)
                        .metadata(
                            match entry.metadata().is_dir() {
                                true => Metadata::directory(),
                                false => Metadata::file(),
                            }
                            .size(metadata.content_length())
                            .written(
                                FileTime::from_unix_time(
                                    metadata.last_modified().unwrap_or_default().timestamp(),
                                )
                                .expect("valid time"),
                            )
                            .created(now),
                        )
                        .mark_in_sync()
                        .blob(
                            bincode::serialize(&FileBlob {
                                ..Default::default()
                            })
                            .expect("valid blob"),
                        )
                })
            })
            .collect::<Vec<_>>();

        _ = ticket.pass_with_placeholder(&mut entries).map_err(|e| {
            log::warn!("failed to pass placeholder: {e:?}");
        });

        Ok(())
    }
}

/// Checks if the entry is in sync.
///
/// Returns `true` if the entry is not exists, `false` otherwise.
fn check_in_sync(entry: &Entry, root: &Path) -> bool {
    let absolute = root.join(entry.path());
    println!("absolute: {}", absolute.display());

    let Ok(metadata) = fs::metadata(&absolute) else {
        return true;
    };

    if metadata.is_dir() != entry.metadata().is_dir() {
        return false;
    } else if metadata.is_file() {
        // FIXME: checksum
        if entry.metadata().content_length() != metadata.len() {
            return false;
        }
    }

    if metadata.is_dir() {
        let mut placeholder = Placeholder::open(absolute).unwrap();
        _ = placeholder
            .convert_to_placeholder(
                ConvertOptions::default()
                    .mark_in_sync()
                    .has_children()
                    .blob(
                        bincode::serialize(&FileBlob {
                            ..Default::default()
                        })
                        .expect("valid blob"),
                    ),
                None,
            )
            .map_err(|e| {
                log::error!("failed to convert to placeholder: {e:?}");
            });
    } else {
        let mut placeholder = Placeholder::from(File::open(absolute).unwrap());
        _ = placeholder
            .convert_to_placeholder(
                ConvertOptions::default().mark_in_sync().blob(
                    bincode::serialize(&FileBlob {
                        ..Default::default()
                    })
                    .expect("valid blob"),
                ),
                None,
            )
            .map_err(|e| log::error!("failed to convert to placeholder: {e:?}"));
    }

    false
}
