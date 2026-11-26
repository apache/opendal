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

/// An algorithm that computes a full-file [`Checksum`].
///
/// A service can advertise [`crate::Capability::write_with_checksum`] and
/// still reject an individual algorithm with [`crate::ErrorKind::Unsupported`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ChecksumAlgo {
    /// CRC-64/NVME, an 8-byte digest.
    Crc64Nvme,
    /// CRC-32C (Castagnoli), a 4-byte digest.
    Crc32c,
    /// MD5, a 16-byte digest.
    Md5,
    /// SHA-256, a 32-byte digest.
    Sha256,
}

impl ChecksumAlgo {
    /// Return the length in bytes of a digest produced by this algorithm.
    pub fn digest_len(self) -> usize {
        match self {
            ChecksumAlgo::Crc64Nvme => 8,
            ChecksumAlgo::Crc32c => 4,
            ChecksumAlgo::Md5 => 16,
            ChecksumAlgo::Sha256 => 32,
        }
    }

    pub(crate) fn to_tag(self) -> u8 {
        match self {
            ChecksumAlgo::Crc64Nvme => 0,
            ChecksumAlgo::Crc32c => 1,
            ChecksumAlgo::Md5 => 2,
            ChecksumAlgo::Sha256 => 3,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> Self {
        match tag {
            0 => ChecksumAlgo::Crc64Nvme,
            1 => ChecksumAlgo::Crc32c,
            2 => ChecksumAlgo::Md5,
            3 => ChecksumAlgo::Sha256,
            _ => unreachable!("compact values only store tags written by to_tag"),
        }
    }
}

/// A full-file checksum: one algorithm and its raw digest bytes.
///
/// `value` holds the raw digest in the byte order the algorithm defines (for
/// CRCs, big-endian), not a hex or base64 string. Services encode the value
/// as their wire format requires.
///
/// # Example
///
/// ```
/// use opendal_core::Checksum;
/// use opendal_core::ChecksumAlgo;
///
/// let checksum = Checksum::new(ChecksumAlgo::Crc32c, 0xe306_9283_u32.to_be_bytes().to_vec());
/// assert_eq!(checksum.algo, ChecksumAlgo::Crc32c);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Checksum {
    /// The algorithm that produced `value`.
    pub algo: ChecksumAlgo,
    /// The raw digest bytes.
    pub value: Vec<u8>,
}

impl Checksum {
    /// Create a checksum from an algorithm and its raw digest bytes.
    pub fn new(algo: ChecksumAlgo, value: impl Into<Vec<u8>>) -> Self {
        Self {
            algo,
            value: value.into(),
        }
    }

    /// Encode as a one-byte algorithm tag followed by the digest.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(1 + self.value.len());
        encoded.push(self.algo.to_tag());
        encoded.extend_from_slice(&self.value);
        encoded
    }

    pub(crate) fn decode(encoded: &[u8]) -> Self {
        let (tag, value) = encoded
            .split_first()
            .expect("encoded checksum contains a tag");
        Self::new(ChecksumAlgo::from_tag(*tag), value)
    }
}

/// The checksum verification requested for an individual write.
///
/// A service that cannot fulfill the request returns [`crate::ErrorKind::Unsupported`].
/// When the service verifies a full-file checksum, the successful write result
/// includes it in [`crate::Metadata::checksum`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WriteChecksum {
    /// Use any native write checksum, preferring CRC-family algorithms.
    ///
    /// The service may verify individual parts or blocks without producing a
    /// full-file checksum.
    Any,
    /// Use a native CRC-family checksum.
    ///
    /// Like [`Self::Any`], this allows verification of individual parts or blocks.
    Fast,
    /// Compute and verify a full-file checksum with this algorithm before commit.
    ///
    /// This request requires a non-append write.
    Algo(ChecksumAlgo),
    /// Verify the caller-supplied full-file checksum before commit.
    ///
    /// This request requires a non-append write.
    Expected(Checksum),
}

impl WriteChecksum {
    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Any => vec![0],
            Self::Fast => vec![1],
            Self::Algo(algo) => vec![2, algo.to_tag()],
            Self::Expected(checksum) => {
                let mut encoded = Vec::with_capacity(2 + checksum.value.len());
                encoded.extend_from_slice(&[3, checksum.algo.to_tag()]);
                encoded.extend_from_slice(&checksum.value);
                encoded
            }
        }
    }

    pub(crate) fn decode(encoded: &[u8]) -> Self {
        match encoded {
            [0] => Self::Any,
            [1] => Self::Fast,
            [2, algo] => Self::Algo(ChecksumAlgo::from_tag(*algo)),
            [3, checksum @ ..] => Self::Expected(Checksum::decode(checksum)),
            _ => unreachable!("compact write arguments only store encoded checksum intents"),
        }
    }
}

/// A checksum requirement that a layer can apply to writes across an operator.
///
/// Convert a policy to [`WriteChecksum`] to apply it to an individual write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WriteChecksumPolicy {
    /// Apply [`WriteChecksum::Any`] to writes.
    Any,
    /// Apply [`WriteChecksum::Fast`] to writes.
    Fast,
    /// Apply [`WriteChecksum::Algo`] with this algorithm to writes.
    Algo(ChecksumAlgo),
}

impl From<Checksum> for WriteChecksum {
    fn from(checksum: Checksum) -> Self {
        Self::Expected(checksum)
    }
}

impl From<ChecksumAlgo> for WriteChecksum {
    fn from(algo: ChecksumAlgo) -> Self {
        Self::Algo(algo)
    }
}

impl From<WriteChecksumPolicy> for WriteChecksum {
    fn from(policy: WriteChecksumPolicy) -> Self {
        match policy {
            WriteChecksumPolicy::Any => Self::Any,
            WriteChecksumPolicy::Fast => Self::Fast,
            WriteChecksumPolicy::Algo(algo) => Self::Algo(algo),
        }
    }
}
