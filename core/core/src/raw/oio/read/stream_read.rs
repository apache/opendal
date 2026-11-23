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

use futures::Future;

use crate::raw::oio::ReadStream;
use crate::raw::*;
use crate::*;

/// StreamRead is used to implement [`oio::Read`] based on native range streams.
///
/// Services that implement [`StreamRead`] only need to expose their native
/// `open(range)` operation. [`StreamReader`] will provide the complete
/// [`oio::Read`] contract.
pub trait StreamRead: Send + Sync + Unpin + 'static {
    /// Open a stream to read the requested range.
    fn open(
        &self,
        range: BytesRange,
    ) -> impl Future<Output = Result<(RpRead, Box<dyn oio::ReadStreamDyn>)>> + MaybeSend;
}

/// StreamReader implements [`oio::Read`] based on [`StreamRead`].
pub struct StreamReader<R: StreamRead> {
    inner: R,
}

impl<R: StreamRead> StreamReader<R> {
    /// Create a new [`StreamReader`].
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    /// Consume the reader and return the inner [`StreamRead`].
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: StreamRead> oio::Read for StreamReader<R> {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        self.inner.open(range).await
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let expected = range
            .size()
            .ok_or_else(|| Error::new(ErrorKind::Unsupported, "read requires a bounded range"))?;

        let (rp, mut stream) = self.inner.open(range).await?;
        let buffer = stream.read_all().await?;
        let actual = buffer.len() as u64;

        let (bound, exact) = if range.is_suffix() {
            match rp.metadata().filter(|metadata| metadata.is_file()) {
                Some(metadata) => (expected.min(metadata.content_length()), true),
                // Without the object size, a suffix range can validly return
                // fewer bytes when the object is shorter than the suffix.
                None => (expected, false),
            }
        } else {
            (expected, true)
        };

        let mismatched = if exact {
            actual != bound
        } else {
            actual > bound
        };
        if mismatched {
            return Err(
                Error::new(ErrorKind::Unexpected, "reader got unexpected data size")
                    .with_context("expect", bound)
                    .with_context("actual", actual),
            );
        }

        Ok((rp, buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serves fixed content and honors the requested range.
    struct RangeStreamRead {
        content: Buffer,
    }

    impl StreamRead for RangeStreamRead {
        async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            let content = self
                .content
                .slice(range.to_content_range(self.content.len())?);
            Ok((RpRead::default(), Box::new(content)))
        }
    }

    /// Serves fixed content regardless of the requested range.
    struct FixedStreamRead {
        content: Buffer,
        content_length: Option<u64>,
    }

    impl StreamRead for FixedStreamRead {
        async fn open(&self, _: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
            let rp = self.content_length.map_or_else(RpRead::default, |length| {
                RpRead::new(MetadataBuilder::file(length).build())
            });
            Ok((rp, Box::new(self.content.clone())))
        }
    }

    #[tokio::test]
    async fn test_read_suffix_larger_than_content() -> Result<()> {
        let reader = StreamReader::new(RangeStreamRead {
            content: Buffer::from("hello"),
        });

        let (_, buffer) = oio::Read::read(&reader, BytesRange::suffix(100)).await?;
        assert_eq!(buffer.to_bytes(), "hello");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_suffix_within_content() -> Result<()> {
        let reader = StreamReader::new(RangeStreamRead {
            content: Buffer::from("hello"),
        });

        let (_, buffer) = oio::Read::read(&reader, BytesRange::suffix(3)).await?;
        assert_eq!(buffer.to_bytes(), "llo");
        Ok(())
    }

    #[tokio::test]
    async fn test_read_suffix_zero() -> Result<()> {
        let reader = StreamReader::new(RangeStreamRead {
            content: Buffer::from("hello"),
        });

        let (_, buffer) = oio::Read::read(&reader, BytesRange::suffix(0)).await?;
        assert!(buffer.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_read_suffix_rejects_extra_data() {
        let reader = StreamReader::new(FixedStreamRead {
            content: Buffer::from("hello!"),
            content_length: None,
        });

        let err = oio::Read::read(&reader, BytesRange::suffix(3))
            .await
            .expect_err("suffix read should reject data beyond the requested suffix");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_read_suffix_rejects_short_data_with_known_content_length() {
        let reader = StreamReader::new(FixedStreamRead {
            content: Buffer::from("hell"),
            content_length: Some(5),
        });

        let err = oio::Read::read(&reader, BytesRange::suffix(100))
            .await
            .expect_err("suffix read should reject data shorter than the object");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn test_read_bounded_rejects_short_data() {
        let reader = StreamReader::new(RangeStreamRead {
            content: Buffer::from("hello"),
        });

        let err = oio::Read::read(&reader, BytesRange::from(0..10))
            .await
            .expect_err("bounded read should reject a short buffer");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }
}
