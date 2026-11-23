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

use opendal as od;

use super::ffi;

impl From<od::Metadata> for ffi::Metadata {
    fn from(meta: od::Metadata) -> Self {
        let mode = meta.mode().into();
        let content_length = meta.content_length();
        let cache_control = meta.cache_control().map(ToOwned::to_owned).into();
        let content_disposition = meta.content_disposition().map(ToOwned::to_owned).into();
        let content_md5 = meta.content_md5().map(ToOwned::to_owned).into();
        let content_type = meta.content_type().map(ToOwned::to_owned).into();
        let content_encoding = meta.content_encoding().map(ToOwned::to_owned).into();
        let etag = meta.etag().map(ToOwned::to_owned).into();
        let last_modified = to_ffi_optional_timestamp(meta.last_modified());
        let version = meta.version().map(ToOwned::to_owned).into();
        let is_current = meta.is_current().into();
        let is_deleted = meta.is_deleted();

        Self {
            mode,
            content_length,
            cache_control,
            content_disposition,
            content_md5,
            content_type,
            content_encoding,
            etag,
            last_modified,
            version,
            is_current,
            is_deleted,
        }
    }
}

impl From<od::Entry> for ffi::Entry {
    fn from(entry: od::Entry) -> Self {
        let (path, _) = entry.into_parts();
        Self { path }
    }
}

impl From<od::EntryMode> for ffi::EntryMode {
    fn from(mode: od::EntryMode) -> Self {
        match mode {
            od::EntryMode::FILE => Self::File,
            od::EntryMode::DIR => Self::Dir,
            _ => Self::Unknown,
        }
    }
}

/// Convert a UTC timestamp into the signed seconds and non-negative
/// nanoseconds used by the C++ binding.
///
/// `Timestamp::as_second` truncates toward zero and
/// `Timestamp::subsec_nanosecond` carries the same sign for instants before
/// the Unix epoch. Normalize them into a floor second and a non-negative
/// nanosecond so C++ can rebuild the instant with `seconds + nanoseconds`.
fn to_ffi_optional_timestamp(timestamp: Option<od::raw::Timestamp>) -> ffi::OptionalTimestamp {
    let Some(timestamp) = timestamp else {
        return ffi::OptionalTimestamp {
            has_value: false,
            seconds: 0,
            nanoseconds: 0,
        };
    };

    let timestamp = timestamp.into_inner();
    let mut seconds = timestamp.as_second();
    let mut nanoseconds = timestamp.subsec_nanosecond();
    if nanoseconds < 0 {
        seconds -= 1;
        nanoseconds += 1_000_000_000;
    }

    ffi::OptionalTimestamp {
        has_value: true,
        seconds,
        nanoseconds: nanoseconds as u32,
    }
}

impl From<Option<String>> for ffi::OptionalString {
    fn from(s: Option<String>) -> Self {
        match s {
            Some(s) => Self {
                has_value: true,
                value: s,
            },
            None => Self {
                has_value: false,
                value: String::default(),
            },
        }
    }
}

impl From<Option<bool>> for ffi::OptionalBool {
    fn from(b: Option<bool>) -> Self {
        match b {
            Some(b) => Self {
                has_value: true,
                value: b,
            },
            None => Self {
                has_value: false,
                value: false,
            },
        }
    }
}

impl From<Option<od::Entry>> for ffi::OptionalEntry {
    fn from(entry: Option<od::Entry>) -> Self {
        match entry {
            Some(entry) => Self {
                has_value: true,
                value: entry.into(),
            },
            None => Self {
                has_value: false,
                value: ffi::Entry {
                    path: String::default(),
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn last_modified_of(timestamp: od::raw::Timestamp) -> ffi::OptionalTimestamp {
        let mut builder = od::MetadataBuilder::file(0);
        builder.last_modified(timestamp);
        let metadata: ffi::Metadata = builder.build().into();
        metadata.last_modified
    }

    #[test]
    fn test_last_modified_keeps_subsecond_precision() {
        let timestamp = od::raw::Timestamp::new(1_700_000_000, 123_456_789).unwrap();

        let ffi = last_modified_of(timestamp);

        assert!(ffi.has_value);
        assert_eq!(ffi.seconds, 1_700_000_000);
        assert_eq!(ffi.nanoseconds, 123_456_789);
    }

    #[test]
    fn test_last_modified_normalizes_pre_epoch_timestamp() {
        // 1969-12-31T23:59:58.5Z: jiff keeps the negative fraction in
        // `subsec_nanosecond`, while the C++ bridge expects a floor second
        // with a non-negative nanosecond.
        let timestamp = od::raw::Timestamp::new(-1, -500_000_000).unwrap();

        let ffi = last_modified_of(timestamp);

        assert!(ffi.has_value);
        assert_eq!(ffi.seconds, -2);
        assert_eq!(ffi.nanoseconds, 500_000_000);
    }

    #[test]
    fn test_last_modified_absent() {
        let metadata: ffi::Metadata = od::MetadataBuilder::file(0).build().into();

        assert!(!metadata.last_modified.has_value);
    }
}
