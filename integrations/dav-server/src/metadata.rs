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

use dav_server::fs::FsError;
use dav_server::fs::{DavMetaData, FsResult};
use opendal_core::Metadata;
use std::time::{SystemTime, UNIX_EPOCH};

/// OpendalMetaData is a `DavMetaData` implementation for opendal.
#[derive(Debug, Clone)]
pub struct OpendalMetaData {
    metadata: Metadata,
}

impl OpendalMetaData {
    /// Create a new opendal metadata.
    pub fn new(metadata: Metadata) -> Self {
        OpendalMetaData { metadata }
    }

    /// Derive an ETag from the length and modification time, like the default
    /// `DavMetaData::etag` that this impl overrides. Backends such as `fs` do
    /// not report an ETag, and without this the server sends none at all.
    fn default_etag(&self) -> Option<String> {
        let t = self.modified().ok()?.duration_since(UNIX_EPOCH).ok()?;
        let t = t.as_secs() * 1_000_000 + u64::from(t.subsec_micros());
        if self.is_file() && self.len() > 0 {
            Some(format!("{:x}-{t:x}", self.len()))
        } else {
            Some(format!("{t:x}"))
        }
    }
}

impl DavMetaData for OpendalMetaData {
    fn len(&self) -> u64 {
        self.metadata.content_length()
    }

    fn modified(&self) -> FsResult<SystemTime> {
        match self.metadata.last_modified() {
            Some(t) => Ok(t.into()),
            None => Err(FsError::GeneralFailure),
        }
    }

    fn is_dir(&self) -> bool {
        self.metadata.is_dir()
    }

    fn etag(&self) -> Option<String> {
        let Some(etag) = self.metadata.etag() else {
            return self.default_etag();
        };
        let tag = match etag.strip_prefix('"') {
            Some(quoted) => quoted.strip_suffix('"')?,
            None => etag,
        };

        // dav-server quotes the ETag header, so return only opaque tag bytes allowed by RFC 9110.
        //   entity-tag = [ weak ] opaque-tag
        //   weak       = %s"W/"
        //   opaque-tag = DQUOTE *etagc DQUOTE
        //   etagc      = %x21 / %x23-7E / obs-text
        //   obs-text   = %x80-FF
        tag.bytes()
            .all(|b| matches!(b, 0x21 | 0x23..=0x7e | 0x80..=0xff))
            .then(|| tag.to_owned())
    }

    fn is_file(&self) -> bool {
        self.metadata.is_file()
    }

    fn is_symlink(&self) -> bool {
        false
    }

    fn status_changed(&self) -> FsResult<SystemTime> {
        match self.metadata.last_modified() {
            Some(t) => Ok(t.into()),
            None => Err(FsError::GeneralFailure),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::MetadataBuilder;
    use opendal_core::raw::Timestamp;

    #[test]
    fn etag_falls_back_to_length_and_modified_time() {
        let mut builder = MetadataBuilder::file(5);
        builder.last_modified(Timestamp::new(1, 2_000).unwrap());
        let meta = OpendalMetaData::new(builder.build());
        // 1s + 2us, in microseconds, as the default `DavMetaData::etag` does.
        assert_eq!(meta.etag().as_deref(), Some("5-f4242"));

        let mut builder = MetadataBuilder::dir();
        builder.last_modified(Timestamp::new(1, 2_000).unwrap());
        assert_eq!(
            OpendalMetaData::new(builder.build()).etag().as_deref(),
            Some("f4242")
        );

        // Without a modification time there is nothing to derive it from.
        assert_eq!(
            OpendalMetaData::new(MetadataBuilder::file(5).build()).etag(),
            None
        );
    }

    #[test]
    fn backend_etag_takes_precedence() {
        let mut builder = MetadataBuilder::file(5);
        builder.last_modified(Timestamp::new(1, 0).unwrap());
        builder.etag("\"abc\"");
        assert_eq!(
            OpendalMetaData::new(builder.build()).etag().as_deref(),
            Some("abc")
        );
    }
}
