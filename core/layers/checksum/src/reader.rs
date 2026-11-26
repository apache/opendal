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

use crc_fast::{CrcAlgorithm, Digest as CrcDigest};
use md5::{Digest, Md5};
use opendal_core::raw::oio::{self, ReadStream, ReadStreamDyn};
use opendal_core::raw::{Operation, RpRead};
use opendal_core::{
    Buffer, BytesRange, Checksum, ChecksumAlgo, Error, ErrorKind, Metadata, Result,
};
use sha2::Sha256;

pub(crate) struct ChecksumReader {
    inner: oio::Reader,
    require_read: bool,
}

impl ChecksumReader {
    pub(crate) fn new(inner: oio::Reader, require_read: bool) -> Self {
        Self {
            inner,
            require_read,
        }
    }

    fn verifier(&self, range: BytesRange, response: &RpRead) -> Result<Option<ReadVerifier>> {
        let metadata = response.metadata();
        let length = metadata
            .filter(|metadata| metadata.is_file())
            .map(Metadata::content_length);
        let covers_whole_object = match range {
            BytesRange::Range {
                offset: 0,
                size: Some(size),
            }
            | BytesRange::Suffix { size } => length.is_some_and(|length| length <= size),
            _ => range.is_full(),
        };
        if !covers_whole_object {
            return Ok(None);
        }

        let verifier = metadata
            .and_then(Metadata::checksum)
            .and_then(|checksum| ReadVerifier::new(checksum, length));
        if self.require_read && verifier.is_none() {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "whole-object read has no supported checksum",
            )
            .with_operation(Operation::Read));
        }
        Ok(verifier)
    }
}

impl oio::Read for ChecksumReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn ReadStreamDyn>)> {
        let (response, inner) = self.inner.open_dyn(range).await?;
        if let Some(verifier) = self.verifier(range, &response)? {
            return Ok((
                response,
                Box::new(ChecksumStream {
                    inner,
                    verification: Ok(Some(verifier)),
                }),
            ));
        }
        Ok((response, inner))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let (response, buffer) = self.inner.read_dyn(range).await?;
        if let Some(mut verifier) = self.verifier(range, &response)? {
            verifier.update(&buffer).map_err(Mismatch::into_error)?;
            verifier.finish().map_err(Mismatch::into_error)?;
        }
        Ok((response, buffer))
    }
}

struct ChecksumStream {
    inner: Box<dyn ReadStreamDyn>,
    // A finished digest leaves None. A mismatch stays latched across polls.
    verification: std::result::Result<Option<ReadVerifier>, Mismatch>,
}

impl ChecksumStream {
    fn verify_chunk(
        verification: &mut Option<ReadVerifier>,
        buffer: &Buffer,
    ) -> std::result::Result<(), Mismatch> {
        let Some(verifier) = verification.as_mut() else {
            return if buffer.is_empty() {
                Ok(())
            } else {
                Err(Mismatch::TooMuchData)
            };
        };

        verifier.update(buffer)?;
        // A limited read can stop as soon as it receives the last byte.
        if buffer.is_empty() || verifier.remaining == Some(0) {
            verification.take().expect("active verifier").finish()?;
        }
        Ok(())
    }
}

impl ReadStream for ChecksumStream {
    async fn read(&mut self) -> Result<Buffer> {
        let verification = self
            .verification
            .as_mut()
            .map_err(|failure| failure.into_error())?;
        let buffer = self.inner.read_dyn().await?;
        if let Err(failure) = Self::verify_chunk(verification, &buffer) {
            self.verification = Err(failure);
            return Err(failure.into_error());
        }
        Ok(buffer)
    }
}

#[derive(Clone, Copy)]
enum Mismatch {
    TooMuchData,
    Truncated,
    Digest,
}

impl Mismatch {
    fn into_error(self) -> Error {
        let message = match self {
            Self::TooMuchData => "read exceeded the object size reported with the checksum",
            Self::Truncated => "read ended before the object size reported with the checksum",
            Self::Digest => "read data does not match the stored checksum",
        };
        Error::new(ErrorKind::ChecksumMismatch, message).with_operation(Operation::Read)
    }
}

struct ReadVerifier {
    expected: Checksum,
    hasher: Hasher,
    remaining: Option<u64>,
}

impl ReadVerifier {
    fn new(expected: Checksum, remaining: Option<u64>) -> Option<Self> {
        let hasher = match expected.algo {
            ChecksumAlgo::Crc64Nvme => Hasher::Crc(CrcDigest::new(CrcAlgorithm::Crc64Nvme)),
            ChecksumAlgo::Crc32c => Hasher::Crc(CrcDigest::new(CrcAlgorithm::Crc32Iscsi)),
            ChecksumAlgo::Md5 => Hasher::Md5(Md5::new()),
            ChecksumAlgo::Sha256 => Hasher::Sha256(Sha256::new()),
            _ => return None,
        };
        Some(Self {
            expected,
            hasher,
            remaining,
        })
    }

    fn update(&mut self, buffer: &Buffer) -> std::result::Result<(), Mismatch> {
        if let Some(remaining) = &mut self.remaining {
            *remaining = remaining
                .checked_sub(buffer.len() as u64)
                .ok_or(Mismatch::TooMuchData)?;
        }
        for chunk in buffer.clone() {
            match &mut self.hasher {
                Hasher::Crc(digest) => digest.update(&chunk),
                Hasher::Md5(digest) => digest.update(&chunk),
                Hasher::Sha256(digest) => digest.update(&chunk),
            }
        }
        Ok(())
    }

    fn finish(self) -> std::result::Result<(), Mismatch> {
        if self.remaining.is_some_and(|remaining| remaining != 0) {
            return Err(Mismatch::Truncated);
        }
        let expected = self.expected.value.as_slice();
        let matches = match self.hasher {
            Hasher::Crc(digest) if self.expected.algo == ChecksumAlgo::Crc32c => {
                (digest.finalize() as u32).to_be_bytes() == expected
            }
            Hasher::Crc(digest) => digest.finalize().to_be_bytes() == expected,
            Hasher::Md5(digest) => digest.finalize().as_slice() == expected,
            Hasher::Sha256(digest) => digest.finalize().as_slice() == expected,
        };
        if !matches {
            return Err(Mismatch::Digest);
        }
        Ok(())
    }
}

enum Hasher {
    Crc(CrcDigest),
    Md5(Md5),
    Sha256(Sha256),
}
