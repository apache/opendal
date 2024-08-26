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

//! `cloudfilter_opendal` integrates OpenDAL with [cloud sync engines](https://learn.microsoft.com/en-us/windows/win32/cfapi/build-a-cloud-file-sync-engine).
//! It provides a way to access various cloud storage on Windows.
//!
//! Note that `cloudfilter_opendal` is a read-only service, and it is not recommended to use it in production.
//!
//! # Example
//!
//! ```no_run
//! use anyhow::Result;
//! use cloud_filter::root::PopulationType;
//! use cloud_filter::root::SecurityId;
//! use cloud_filter::root::Session;
//! use cloud_filter::root::SyncRootIdBuilder;
//! use cloud_filter::root::SyncRootInfo;
//! use opendal::services;
//! use opendal::Operator;
//! use tokio::runtime::Handle;
//! use tokio::signal;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Create any service desired
//!     let op = Operator::from_iter::<services::S3>([
//!         ("bucket".to_string(), "my_bucket".to_string()),
//!         ("access_key".to_string(), "my_access_key".to_string()),
//!         ("secret_key".to_string(), "my_secret_key".to_string()),
//!         ("endpoint".to_string(), "my_endpoint".to_string()),
//!         ("region".to_string(), "my_region".to_string()),
//!     ])?
//!     .finish();
//!
//!     let client_path = std::env::var("CLIENT_PATH").expect("$CLIENT_PATH is set");
//!
//!     // Create a sync root id
//!     let sync_root_id = SyncRootIdBuilder::new("cloudfilter_opendal")
//!         .user_security_id(SecurityId::current_user()?)
//!         .build();
//!
//!     // Register the sync root if not exists
//!     if !sync_root_id.is_registered()? {
//!         sync_root_id.register(
//!             SyncRootInfo::default()
//!                 .with_display_name("OpenDAL Cloud Filter")
//!                 .with_population_type(PopulationType::Full)
//!                 .with_icon("shell32.dll,3")
//!                 .with_version("1.0.0")
//!                 .with_recycle_bin_uri("http://cloudmirror.example.com/recyclebin")?
//!                 .with_path(&client_path)?,
//!         )?;
//!     }
//!
//!     let handle = Handle::current();
//!     let connection = Session::new().connect_async(
//!         &client_path,
//!         cloudfilter_opendal::CloudFilter::new(op, client_path.clone().into()),
//!         move |f| handle.block_on(f),
//!     )?;
//!
//!     signal::ctrl_c().await?;
//!
//!     // Drop the connection before unregister the sync root
//!     drop(connection);
//!     sync_root_id.unregister()?;
//!
//!     Ok(())
//! }
//! ``````

mod file;

use std::{
    cmp::min,
    fs::{self, File},
    path::{Path, PathBuf},
};

use cloud_filter::{
    error::{CResult, CloudErrorKind},
    filter::{info, ticket, Filter, Request},
    metadata::Metadata,
    placeholder::{ConvertOptions, Placeholder},
    placeholder_file::PlaceholderFile,
    utility::{FileTime, WriteAt},
};
use file::FileBlob;
use futures::StreamExt;
use opendal::{Entry, Metakey, Operator};

const BUF_SIZE: usize = 65536;

/// CloudFilter is a adapter that adapts Windows cloud sync engines.
pub struct CloudFilter {
    op: Operator,
    root: PathBuf,
}

impl CloudFilter {
    /// Create a new CloudFilter.
    pub fn new(op: Operator, root: PathBuf) -> Self {
        Self { op, root }
    }
}

impl Filter for CloudFilter {
    async fn fetch_data(
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

        let reader = self
            .op
            .reader_with(&remote_path.to_string_lossy().replace('\\', "/"))
            .await
            .map_err(|e| {
                log::warn!("failed to open file: {}", e);
                CloudErrorKind::Unsuccessful
            })?;

        let mut position = range.start;
        let mut buffer = Vec::with_capacity(BUF_SIZE);

        loop {
            let mut bytes_read = reader
                .read_into(
                    &mut buffer,
                    position..min(range.end, position + BUF_SIZE as u64),
                )
                .await
                .map_err(|e| {
                    log::warn!("failed to read file: {}", e);
                    CloudErrorKind::Unsuccessful
                })?;

            let unaligned = bytes_read % 4096;
            if unaligned != 0 && position + (bytes_read as u64) < range.end {
                bytes_read -= unaligned;
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

            buffer.clear();

            ticket.report_progress(range.end, position).map_err(|e| {
                log::warn!("failed to report progress: {}", e);
                CloudErrorKind::Unsuccessful
            })?;
        }

        Ok(())
    }

    async fn fetch_placeholders(
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
            .await
            .map_err(|e| {
                log::warn!("failed to list files: {}", e);
                CloudErrorKind::Unsuccessful
            })?
            .filter_map(|e| async {
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
            .collect::<Vec<_>>()
            .await;

        _ = ticket.pass_with_placeholder(&mut entries).map_err(|e| {
            log::warn!("failed to pass placeholder: {e:?}");
        });

        Ok(())
    }
}

/// Checks if the entry is in sync, then convert to placeholder.
///
/// Returns `true` if the entry is not exists, `false` otherwise.
fn check_in_sync(entry: &Entry, root: &Path) -> bool {
    let absolute = root.join(entry.path());

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
