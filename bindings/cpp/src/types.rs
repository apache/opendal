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
use super::metadata::ffi as metadata_ffi;

impl From<od::Entry> for ffi::Entry {
    fn from(entry: od::Entry) -> Self {
        let (path, _) = entry.into_parts();
        Self { path }
    }
}

impl From<od::EntryMode> for metadata_ffi::EntryMode {
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
impl From<Option<od::raw::Timestamp>> for metadata_ffi::OptionalTimestamp {
    fn from(timestamp: Option<od::raw::Timestamp>) -> Self {
        let Some(timestamp) = timestamp else {
            return Self {
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

        Self {
            has_value: true,
            seconds,
            nanoseconds: nanoseconds as u32,
        }
    }
}

impl From<Option<&str>> for metadata_ffi::OptionalString {
    fn from(s: Option<&str>) -> Self {
        match s {
            Some(s) => Self {
                has_value: true,
                value: s.to_owned(),
            },
            None => Self {
                has_value: false,
                value: String::default(),
            },
        }
    }
}

impl From<Option<od::UserMetadata<'_>>> for metadata_ffi::OptionalUserMetadata {
    fn from(user_metadata: Option<od::UserMetadata<'_>>) -> Self {
        match user_metadata {
            Some(user_metadata) => Self {
                has_value: true,
                value: user_metadata
                    .into_iter()
                    .map(|(key, value)| metadata_ffi::HashMapValue {
                        key: key.to_owned(),
                        value: value.to_owned(),
                    })
                    .collect(),
            },
            None => Self {
                has_value: false,
                value: Vec::new(),
            },
        }
    }
}

impl From<Option<bool>> for metadata_ffi::OptionalBool {
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

    fn last_modified_of(timestamp: od::raw::Timestamp) -> metadata_ffi::OptionalTimestamp {
        let mut builder = od::MetadataBuilder::file(0);
        builder.last_modified(timestamp);
        let metadata: metadata_ffi::Metadata = builder.build().into();
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
        let metadata: metadata_ffi::Metadata = od::MetadataBuilder::file(0).build().into();

        assert!(!metadata.last_modified.has_value);
    }

    #[test]
    fn test_metadata_carries_every_accessor() {
        let mut builder = od::MetadataBuilder::file(42);
        builder
            .is_current(Some(false))
            .is_deleted(true)
            .cache_control("cache_control")
            .content_md5("content_md5")
            .content_type("content_type")
            .content_encoding("content_encoding")
            .content_language("content_language")
            .last_modified(od::raw::Timestamp::new(7, 8).unwrap())
            .etag("etag")
            .content_disposition("content_disposition")
            .version("version")
            .user_metadata([("owner".to_string(), "opendal".to_string())]);

        let metadata: metadata_ffi::Metadata = builder.build().into();

        assert!(metadata.mode == metadata_ffi::EntryMode::File);
        assert!(metadata.is_current.has_value);
        assert!(!metadata.is_current.value);
        assert!(metadata.is_deleted);
        assert_eq!(metadata.content_length, 42);
        for (field, expected) in [
            (&metadata.cache_control, "cache_control"),
            (&metadata.content_md5, "content_md5"),
            (&metadata.content_type, "content_type"),
            (&metadata.content_encoding, "content_encoding"),
            (&metadata.content_language, "content_language"),
            (&metadata.etag, "etag"),
            (&metadata.content_disposition, "content_disposition"),
            (&metadata.version, "version"),
        ] {
            assert!(field.has_value, "{expected} is missing");
            assert_eq!(field.value, expected);
        }
        assert!(metadata.last_modified.has_value);
        assert_eq!(metadata.last_modified.seconds, 7);
        assert_eq!(metadata.last_modified.nanoseconds, 8);
        assert!(metadata.user_metadata.has_value);
        let pairs: Vec<_> = metadata
            .user_metadata
            .value
            .iter()
            .map(|pair| (pair.key.as_str(), pair.value.as_str()))
            .collect();
        assert_eq!(pairs, [("owner", "opendal")]);
    }

    #[test]
    fn test_metadata_keeps_absent_accessors_absent() {
        let metadata: metadata_ffi::Metadata = od::MetadataBuilder::dir().build().into();

        assert!(metadata.mode == metadata_ffi::EntryMode::Dir);
        assert!(!metadata.is_current.has_value);
        assert!(!metadata.content_language.has_value);
        assert!(!metadata.user_metadata.has_value);
    }
}
