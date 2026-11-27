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

use sha2::{Digest, Sha256};

use opendal_core::Metadata;
use opendal_core::raw::{ServiceInfo, build_absolute_path, normalize_root};

pub(crate) type Subject = [u8; 32];

pub(crate) fn subject(info: &ServiceInfo, path: &str, version: Option<&str>) -> Subject {
    let absolute = build_absolute_path(&normalize_root(&info.root()), path);
    let name = info.name();
    let mut hash = Sha256::new();
    for part in [
        info.scheme().as_bytes(),
        name.as_bytes(),
        absolute.as_bytes(),
    ] {
        hash_part(&mut hash, part);
    }
    match version {
        Some(version) => hash_part(&mut hash, version.as_bytes()),
        None => hash.update(u64::MAX.to_be_bytes()),
    }
    hash.finalize().into()
}

fn digest<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> [u8; 32] {
    let mut hash = Sha256::new();
    for part in parts {
        hash_part(&mut hash, part);
    }
    hash.finalize().into()
}

fn hash_part(hash: &mut Sha256, part: &[u8]) {
    hash.update((part.len() as u64).to_be_bytes());
    hash.update(part);
}

pub(crate) fn strong_etag(metadata: &Metadata) -> Option<&str> {
    metadata.etag().filter(|etag| !etag.starts_with("W/\""))
}

/// Return whether a version identifier denotes an immutable object version.
///
/// S3-compatible services report `"null"` for objects in buckets with
/// versioning disabled or suspended, which can be overwritten in place.
pub(crate) fn is_immutable_version(version: &str) -> bool {
    !version.is_empty() && version != "null"
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Tier {
    Version = 1,
    Etag = 2,
    Modified = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) tier: Tier,
    pub(crate) token: [u8; 16],
}

impl Identity {
    pub(crate) fn from_metadata(metadata: &Metadata) -> Option<Self> {
        let etag = strong_etag(metadata);
        let version = metadata.version().filter(|v| is_immutable_version(v));
        let (tier, hash) = if let Some(version) = version {
            (
                Tier::Version,
                digest([
                    b"version",
                    version.as_bytes(),
                    etag.unwrap_or_default().as_bytes(),
                ]),
            )
        } else if let Some(etag) = etag {
            (Tier::Etag, digest([b"etag", etag.as_bytes()]))
        } else {
            let modified = metadata.last_modified()?;
            (
                Tier::Modified,
                digest([
                    b"modified",
                    &modified.into_inner().as_nanosecond().to_be_bytes()[..],
                    &metadata.content_length().to_be_bytes()[..],
                ]),
            )
        };
        Some(Self {
            tier,
            token: hash[..16].try_into().unwrap(),
        })
    }
}

pub(crate) fn meta_key(subject: &Subject) -> String {
    format!("v1/meta/{}", hex(subject))
}

pub(crate) fn data_prefix(subject: &Subject, identity: Identity, chunk_size: u64) -> String {
    format!(
        "v1/data/{}/{}/{chunk_size:x}",
        hex(subject),
        hex(&identity.token)
    )
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::MetadataBuilder;

    #[test]
    fn v1_keys_are_deterministic_with_version_and_chunk_coordinates() {
        let info = ServiceInfo::new("s3", "/prefix/", "bucket");
        let versioned = subject(&info, "object", Some("v1"));
        let mut metadata = MetadataBuilder::file(30);
        metadata.etag("\"abc\"");
        let identity = Identity::from_metadata(&metadata.build()).unwrap();
        assert_eq!(
            meta_key(&versioned),
            "v1/meta/94b1016f87521408a1d701e6c37eb5fbb56798e59cfaa801566cc1525db819e3"
        );
        assert_eq!(
            format!("{}/{:x}", data_prefix(&versioned, identity, 16), 10),
            "v1/data/94b1016f87521408a1d701e6c37eb5fbb56798e59cfaa801566cc1525db819e3/c27601d628122b577ca2db462e27d2f4/10/a"
        );
        assert_ne!(
            subject(&info, "object", None),
            subject(&info, "object", Some(""))
        );
        assert!(Identity::from_metadata(&MetadataBuilder::file(30).build()).is_none());
    }

    #[test]
    fn non_immutable_versions_do_not_form_version_identity() {
        for placeholder in ["", "null"] {
            let mut only_placeholder = MetadataBuilder::file(30);
            only_placeholder.version(placeholder);
            assert!(
                Identity::from_metadata(&only_placeholder.build()).is_none(),
                "placeholder version {placeholder:?} alone must not be cacheable"
            );

            let mut with_etag = MetadataBuilder::file(30);
            with_etag.version(placeholder).etag("\"v1\"");
            let identity = Identity::from_metadata(&with_etag.build()).unwrap();
            assert_eq!(identity.tier, Tier::Etag);
        }

        let mut immutable = MetadataBuilder::file(30);
        immutable.version("v1");
        assert_eq!(
            Identity::from_metadata(&immutable.build()).unwrap().tier,
            Tier::Version
        );
    }
}
