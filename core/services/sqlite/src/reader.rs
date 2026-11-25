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

use super::backend::*;
use opendal_core::raw::oio;
use opendal_core::raw::*;
use opendal_core::*;

/// Reader returned by this backend.
pub struct SqliteReader {
    backend: SqliteBackend,
    path: String,
}

impl SqliteReader {
    pub(super) fn new(backend: SqliteBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for SqliteReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let p = build_absolute_path(&backend.root, path);

        let (buffer, content_length) = match range {
            BytesRange::Range {
                offset: 0,
                size: None,
            } => match backend.core.get(&p).await? {
                Some(bs) => {
                    let content_length = bs.len() as u64;
                    (bs, content_length)
                }
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in sqlite")),
            },
            BytesRange::Range { size: Some(0), .. } | BytesRange::Suffix { size: 0 } => {
                match backend.core.get_length(&p).await? {
                    Some(content_length) => (Buffer::new(), content_length),
                    None => {
                        return Err(Error::new(ErrorKind::NotFound, "key not found in sqlite"));
                    }
                }
            }
            BytesRange::Range { offset, size } => {
                let (buffer, content_length) =
                    match backend.core.get_range(&p, offset, size).await? {
                        Some(v) => v,
                        None => {
                            return Err(Error::new(ErrorKind::NotFound, "key not found in sqlite"));
                        }
                    };
                let content_length_usize = usize::try_from(content_length).map_err(|err| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "sqlite value length exceeds usize::MAX",
                    )
                    .set_source(err)
                })?;
                let content_range = range.to_content_range(content_length_usize)?;
                debug_assert_eq!(buffer.len(), content_range.len());
                (buffer, content_length)
            }
            BytesRange::Suffix { size } => match backend.core.get_suffix(&p, size).await? {
                Some(v) => v,
                None => return Err(Error::new(ErrorKind::NotFound, "key not found in sqlite")),
            },
        };

        let metadata = MetadataBuilder::file(content_length).build();
        Ok((
            RpRead::new(metadata),
            Box::new(buffer) as Box<dyn oio::ReadStreamDyn>,
        ))
    }
}
