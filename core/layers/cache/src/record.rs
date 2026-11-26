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

use std::time::Duration;

use bytes::BufMut;
use opendal_core::raw::{SystemTime, Timestamp};
use opendal_core::{Buffer, Checksum, ChecksumAlgo, EntryMode, Metadata, MetadataBuilder};

use crate::key::{Identity, Tier};

#[derive(Clone, Debug)]
pub(crate) struct Pointer {
    pub(crate) metadata: Metadata,
    pub(crate) identity: Identity,
    pub(crate) observed_at: Timestamp,
}

// The record version is independent of the cache namespace. Keep the ordered
// metadata fields and their encodings stable for caches shared by processes.
const RECORD_VERSION: u8 = 1;

// The MetadataBuilder compact field block is limited to u16::MAX bytes.
// Allow room for the fixed pointer header, field lengths, and checksum framing.
pub(crate) const MAX_RECORD_LEN: usize = u16::MAX as usize + 128;

const STRINGS: [fn(&Metadata) -> Option<&str>; 8] = [
    Metadata::cache_control,
    Metadata::content_disposition,
    Metadata::content_md5,
    Metadata::content_type,
    Metadata::content_encoding,
    Metadata::content_language,
    Metadata::etag,
    Metadata::version,
];

impl Pointer {
    pub(crate) fn from_metadata(metadata: Metadata, observed_at: Timestamp) -> Option<Self> {
        let identity = Identity::from_metadata(&metadata)?;
        Some(Self {
            metadata,
            identity,
            observed_at,
        })
    }

    pub(crate) fn is_fresh(&self, ttl: Duration, version: Option<&str>) -> bool {
        ttl == Duration::MAX
            || version.is_some()
            || (ttl != Duration::ZERO
                && SystemTime::from(Timestamp::now())
                    .duration_since(self.observed_at.into())
                    .unwrap_or_default()
                    < ttl)
    }

    pub(crate) fn encode(&self) -> Buffer {
        let mut record = Vec::new();
        record.put_u8(RECORD_VERSION);
        record.put_u8(self.identity.tier as u8);
        record.extend_from_slice(&self.identity.token);
        write_timestamp(&mut record, self.observed_at);
        let metadata = &self.metadata;
        record.put_u8(match metadata.mode() {
            EntryMode::Unknown => 0,
            EntryMode::FILE => 1,
            EntryMode::DIR => 2,
        });
        record.put_u64(metadata.content_length());
        record.put_u8(match metadata.is_current() {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        });
        record.put_u8(u8::from(metadata.is_deleted()));
        record.put_u8(u8::from(metadata.last_modified().is_some()));
        if let Some(time) = metadata.last_modified() {
            write_timestamp(&mut record, time);
        }
        for field in STRINGS {
            match field(metadata) {
                Some(value) => write_string(&mut record, value),
                None => record.put_u16(u16::MAX),
            }
        }
        if let Some(values) = metadata.user_metadata() {
            record.put_u16(values.len() as u16);
            for (key, value) in values {
                write_string(&mut record, key);
                write_string(&mut record, value);
            }
        } else {
            record.put_u16(u16::MAX);
        }
        if let Some(checksum) = metadata.checksum()
            && let Some(tag) = checksum_algo_tag(checksum.algo)
        {
            record.put_u8(tag);
            record.put_u16(checksum.value.len() as u16);
            record.extend_from_slice(&checksum.value);
        } else {
            record.put_u8(0);
        }
        record.into()
    }

    pub(crate) fn decode(record: Buffer) -> Option<Self> {
        if record.len() > MAX_RECORD_LEN {
            return None;
        }
        let bytes = record.to_vec();
        let mut input = bytes.as_slice();
        if input.read_u8()? != RECORD_VERSION {
            return None;
        }
        let tier = match input.read_u8()? {
            1 => Tier::Version,
            2 => Tier::Etag,
            3 => Tier::Modified,
            _ => return None,
        };
        let token = *input.read_array::<16>()?;
        let identity = Identity { tier, token };
        let observed_at = input.read_timestamp()?;
        let mode = input.read_u8()?;
        let length = u64::from_be_bytes(*input.read_array()?);
        let mut metadata = match (mode, length) {
            (0, 0) => MetadataBuilder::unknown(),
            (1, length) => MetadataBuilder::file(length),
            (2, 0) => MetadataBuilder::dir(),
            _ => return None,
        };
        metadata.is_current(match input.read_u8()? {
            0 => None,
            1 => Some(false),
            2 => Some(true),
            _ => return None,
        });
        metadata.is_deleted(input.read_bool()?);
        if input.read_bool()? {
            metadata.last_modified(input.read_timestamp()?);
        }
        // CompactValues stores a bitmap and an end offset per present field.
        let mut compact_len = 2;
        let setters: [fn(&mut MetadataBuilder, String) -> &mut MetadataBuilder; STRINGS.len()] = [
            MetadataBuilder::cache_control,
            MetadataBuilder::content_disposition,
            MetadataBuilder::content_md5,
            MetadataBuilder::content_type,
            MetadataBuilder::content_encoding,
            MetadataBuilder::content_language,
            MetadataBuilder::etag,
            MetadataBuilder::version,
        ];
        for set in setters {
            let len = input.read_u16()?;
            if len != u16::MAX {
                compact_len += 2 + usize::from(len);
                set(&mut metadata, input.read_string(len)?.to_owned());
            }
        }
        let count = input.read_u16()?;
        if count != u16::MAX {
            compact_len += 4;
            let mut values = Vec::new();
            for _ in 0..count {
                let key_len = input.read_u16()?;
                let key = input.read_string(key_len)?;
                let value_len = input.read_u16()?;
                let value = input.read_string(value_len)?;
                compact_len += 4 + usize::from(key_len) + usize::from(value_len);
                if values
                    .last()
                    .is_some_and(|(previous, _): &(String, String)| previous.as_str() >= key)
                {
                    return None;
                }
                values.push((key.to_owned(), value.to_owned()));
            }
            metadata.user_metadata(values);
        }
        let checksum_tag = input.read_u8()?;
        if checksum_tag != 0 {
            let algo = checksum_algo_from_tag(checksum_tag)?;
            let len = input.read_u16()?;
            if usize::from(len) != algo.digest_len() {
                return None;
            }
            let value = input.read_bytes(len)?;
            // CompactValues stores an end offset plus 1-byte algorithm tag and digest.
            compact_len += 2 + 1 + usize::from(len);
            metadata.checksum(Checksum::new(algo, value));
        }
        if !input.is_empty() || compact_len > u16::MAX as usize {
            return None;
        }
        let metadata = metadata.build();
        if Identity::from_metadata(&metadata) != Some(identity) {
            return None;
        }
        Some(Self {
            metadata,
            identity,
            observed_at,
        })
    }
}

fn checksum_algo_tag(algo: ChecksumAlgo) -> Option<u8> {
    match algo {
        ChecksumAlgo::Crc64Nvme => Some(1),
        ChecksumAlgo::Crc32c => Some(2),
        ChecksumAlgo::Md5 => Some(3),
        ChecksumAlgo::Sha256 => Some(4),
        _ => None,
    }
}

fn checksum_algo_from_tag(tag: u8) -> Option<ChecksumAlgo> {
    match tag {
        1 => Some(ChecksumAlgo::Crc64Nvme),
        2 => Some(ChecksumAlgo::Crc32c),
        3 => Some(ChecksumAlgo::Md5),
        4 => Some(ChecksumAlgo::Sha256),
        _ => None,
    }
}

fn write_timestamp(output: &mut Vec<u8>, time: Timestamp) {
    let time = time.into_inner();
    output.put_i64(time.as_second());
    output.put_i32(time.subsec_nanosecond());
}

fn write_string(output: &mut Vec<u8>, value: &str) {
    output.put_u16(value.len() as u16);
    output.extend_from_slice(value.as_bytes());
}

trait RecordInput<'a> {
    fn read_array<const N: usize>(&mut self) -> Option<&'a [u8; N]>;
    fn read_u8(&mut self) -> Option<u8>;
    fn read_bool(&mut self) -> Option<bool>;
    fn read_u16(&mut self) -> Option<u16>;
    fn read_timestamp(&mut self) -> Option<Timestamp>;
    fn read_bytes(&mut self, len: u16) -> Option<&'a [u8]>;
    fn read_string(&mut self, len: u16) -> Option<&'a str>;
}

impl<'a> RecordInput<'a> for &'a [u8] {
    fn read_array<const N: usize>(&mut self) -> Option<&'a [u8; N]> {
        let (value, rest) = self.split_first_chunk()?;
        *self = rest;
        Some(value)
    }

    fn read_u8(&mut self) -> Option<u8> {
        Some(self.read_array::<1>()?[0])
    }

    fn read_bool(&mut self) -> Option<bool> {
        match self.read_u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    fn read_u16(&mut self) -> Option<u16> {
        Some(u16::from_be_bytes(*self.read_array()?))
    }

    fn read_timestamp(&mut self) -> Option<Timestamp> {
        let seconds = i64::from_be_bytes(*self.read_array()?);
        let nanos = i32::from_be_bytes(*self.read_array()?);
        Timestamp::new(seconds, nanos).ok()
    }

    fn read_bytes(&mut self, len: u16) -> Option<&'a [u8]> {
        let (value, rest) = self.split_at_checked(usize::from(len))?;
        *self = rest;
        Some(value)
    }

    fn read_string(&mut self, len: u16) -> Option<&'a str> {
        std::str::from_utf8(self.read_bytes(len)?).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pointer_preserves_source_metadata_including_checksum_and_rejects_truncated_records() {
        let mut metadata = MetadataBuilder::file(42);
        metadata
            .version("v1")
            .etag("\"abc\"")
            .content_encoding("gzip")
            .content_language("en")
            .last_modified(Timestamp::new(-42, -123).unwrap())
            .is_current(Some(false))
            .user_metadata([("key".to_owned(), "value".to_owned())])
            .checksum(Checksum::new(
                ChecksumAlgo::Crc32c,
                [0xde, 0xad, 0xbe, 0xef],
            ));
        let pointer =
            Pointer::from_metadata(metadata.build(), Timestamp::new(123, 123).unwrap()).unwrap();
        let encoded = pointer.encode();
        let decoded = Pointer::decode(encoded.clone()).unwrap();
        assert_eq!(decoded.metadata, pointer.metadata);
        assert_eq!(decoded.metadata.checksum(), pointer.metadata.checksum());
        assert_eq!(decoded.identity, pointer.identity);
        assert_eq!(decoded.observed_at, pointer.observed_at);
        for len in 0..encoded.len() {
            assert!(Pointer::decode(encoded.slice(..len)).is_none());
        }
        // Reject corrupted checksum digest length.
        let mut bad_checksum = encoded.to_vec();
        let last = bad_checksum.len();
        bad_checksum[last - 5] = 3; // wrong digest len for Crc32c
        bad_checksum.pop();
        assert!(Pointer::decode(Buffer::from(bad_checksum)).is_none());
    }

    #[test]
    fn pointer_v1_wire_format_is_stable_and_accepts_max_metadata() {
        let mut metadata = MetadataBuilder::file(10);
        metadata
            .etag("\"abc\"")
            .checksum(Checksum::new(ChecksumAlgo::Crc64Nvme, [7; 8]));
        let pointer =
            Pointer::from_metadata(metadata.build(), Timestamp::new(100, 200).unwrap()).unwrap();
        let encoded = pointer.encode().to_vec();
        let expected: Vec<u8> = [
            &[1u8, 2u8][..], // RECORD_VERSION=1, Tier::Etag=2
            &[
                0xc2, 0x76, 0x01, 0xd6, 0x28, 0x12, 0x2b, 0x57, 0x7c, 0xa2, 0xdb, 0x46, 0x2e, 0x27,
                0xd2, 0xf4,
            ], // sha256("etag", "\"abc\"")[..16]
            &100i64.to_be_bytes(),
            &200i32.to_be_bytes(),
            &[1u8], // EntryMode::FILE
            &10u64.to_be_bytes(),
            &[0u8, 0u8, 0u8], // is_current=None, is_deleted=false, has_last_modified=false
            &[0xff, 0xff].repeat(6), // first 6 string fields absent
            &5u16.to_be_bytes(),
            b"\"abc\"",
            &[0xff, 0xff], // version absent
            &[0xff, 0xff], // user_metadata absent
            &[1u8],        // ChecksumAlgo::Crc64Nvme
            &8u16.to_be_bytes(),
            &[7u8; 8],
        ]
        .concat();
        assert_eq!(encoded, expected);
        assert_eq!(
            Pointer::decode(Buffer::from(expected)).unwrap().metadata,
            pointer.metadata
        );

        // Max-size Metadata (u16::MAX compact values + fixed header) still round-trips.
        let mut large = MetadataBuilder::file(1);
        large
            .etag("\"e\"")
            .last_modified(Timestamp::new(1, 0).unwrap())
            .content_type("a".repeat(u16::MAX as usize - 9));
        let large_ptr = Pointer::from_metadata(large.build(), Timestamp::new(1, 0).unwrap())
            .expect("valid large pointer");
        let large_encoded = large_ptr.encode();
        assert!(large_encoded.len() > u16::MAX as usize + 64);
        assert!(large_encoded.len() <= MAX_RECORD_LEN);
        assert_eq!(
            Pointer::decode(large_encoded).unwrap().metadata,
            large_ptr.metadata
        );
    }
}
