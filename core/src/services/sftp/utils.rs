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

use openssh_sftp_client::metadata::MetaData as SftpMeta;

use crate::EntryMode;
use crate::Metadata;

/// REMOVE ME: we should not implement `From<SftpMeta> for Metadata`.
impl From<SftpMeta> for Metadata {
    fn from(meta: SftpMeta) -> Self {
        let mode = meta
            .file_type()
            .map(|filetype| {
                if filetype.is_file() {
                    EntryMode::FILE
                } else if filetype.is_dir() {
                    EntryMode::DIR
                } else {
                    EntryMode::Unknown
                }
            })
            .unwrap_or(EntryMode::Unknown);

        let mut metadata = Metadata::new(mode);

        if let Some(size) = meta.len() {
            metadata.set_content_length(size);
        }

        if let Some(modified) = meta.modified() {
            metadata.set_last_modified(modified.as_system_time().into());
        }

        metadata
    }
}
