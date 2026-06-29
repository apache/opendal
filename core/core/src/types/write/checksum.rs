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

use bytes::Bytes;

/// The algorithm used to compute a [`Checksum`].
///
/// This mirrors the additional checksum algorithms that object storage
/// services such as AWS S3 accept for verifying the integrity of uploaded
/// data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ChecksumAlgorithm {
    /// CRC32C (Castagnoli) checksum.
    Crc32c,
    /// SHA-1 checksum.
    Sha1,
    /// SHA-256 checksum.
    Sha256,
    /// CRC-64/NVME checksum.
    Crc64Nvme,
}

/// A precomputed checksum of the data being written.
///
/// Supplying a checksum lets the service verify the integrity of an upload
/// against a value you already know, instead of computing one from the body.
///
/// The `value` holds the raw digest bytes (for example the 32 bytes of a
/// SHA-256 digest), not a hex or base64 encoded string. Services encode the
/// value as required by their wire format.
///
/// # Example
///
/// ```
/// use opendal_core::Checksum;
///
/// // 32 raw bytes of a SHA-256 digest computed ahead of time.
/// let digest = vec![0u8; 32];
/// let checksum = Checksum::sha256(digest);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checksum {
    algorithm: ChecksumAlgorithm,
    value: Bytes,
}

impl Checksum {
    /// Create a checksum from an algorithm and its raw digest bytes.
    pub fn new(algorithm: ChecksumAlgorithm, value: impl Into<Bytes>) -> Self {
        Self {
            algorithm,
            value: value.into(),
        }
    }

    /// Create a CRC32C checksum from its raw digest bytes.
    pub fn crc32c(value: impl Into<Bytes>) -> Self {
        Self::new(ChecksumAlgorithm::Crc32c, value)
    }

    /// Create a SHA-1 checksum from its raw digest bytes.
    pub fn sha1(value: impl Into<Bytes>) -> Self {
        Self::new(ChecksumAlgorithm::Sha1, value)
    }

    /// Create a SHA-256 checksum from its raw digest bytes.
    pub fn sha256(value: impl Into<Bytes>) -> Self {
        Self::new(ChecksumAlgorithm::Sha256, value)
    }

    /// Create a CRC-64/NVME checksum from its raw digest bytes.
    pub fn crc64nvme(value: impl Into<Bytes>) -> Self {
        Self::new(ChecksumAlgorithm::Crc64Nvme, value)
    }

    /// Get the algorithm of this checksum.
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        self.algorithm
    }

    /// Get the raw digest bytes of this checksum.
    pub fn value(&self) -> &Bytes {
        &self.value
    }
}
