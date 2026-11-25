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

use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::*;

use crate::core::{ErrorContext, GcsGrpcCore, parse_generation, parse_status};
use crate::generated::google::storage::v2::{ReadObjectRequest, ReadObjectResponse};

pub(super) struct GcsGrpcReader {
    core: Arc<GcsGrpcCore>,
    ctx: OperationContext,
    path: String,
    args: OpRead,
}

impl GcsGrpcReader {
    pub(super) fn new(
        core: Arc<GcsGrpcCore>,
        ctx: OperationContext,
        path: &str,
        args: OpRead,
    ) -> Self {
        Self {
            core,
            ctx,
            path: path.to_string(),
            args,
        }
    }
}

impl oio::Read for GcsGrpcReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let generation = parse_generation(self.args.version())?;
        let Some((read_offset, read_limit)) = parse_read_range(range)? else {
            return Ok((RpRead::default(), Box::new(Buffer::new())));
        };
        let object = self.core.object_name(&self.path);
        let request = ReadObjectRequest {
            bucket: self.core.bucket_resource(),
            object,
            generation,
            read_offset,
            read_limit,
        };
        let request = self
            .core
            .request(
                &self.ctx,
                request,
                &[("bucket", &self.core.bucket_resource())],
            )
            .await?;
        let response = self
            .core
            .client()
            .read_object(request)
            .await
            .map_err(|status| {
                parse_status(ErrorContext::new(ServiceOperation("ReadObject")), status)
            })?;
        Ok((
            RpRead::default(),
            Box::new(GcsGrpcReadStream {
                stream: response.into_inner(),
                done: false,
                error_context: ErrorContext::new(ServiceOperation("ReadObject")),
            }),
        ))
    }

    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        let (rp, mut stream) = self.open(range).await?;
        Ok((rp, stream.read_all_dyn().await?))
    }
}

fn parse_read_range(range: BytesRange) -> Result<Option<(i64, i64)>> {
    match range {
        BytesRange::Suffix { size: 0 } | BytesRange::Range { size: Some(0), .. } => Ok(None),
        BytesRange::Suffix { size } => match i64::try_from(size) {
            Ok(size) => Ok(Some((-size, 0))),
            // GCS object sizes fit in i64, so any suffix larger than i64::MAX
            // reads the full object from offset 0.
            Err(_) => Ok(Some((0, 0))),
        },
        BytesRange::Range { offset, size } => {
            let read_offset = i64::try_from(offset).map_err(|err| {
                Error::new(
                    ErrorKind::RangeNotSatisfied,
                    "GCS gRPC read offset exceeds i64",
                )
                .set_source(err)
            })?;
            // GCS treats read_limit == 0 as "read to EOF", and object sizes fit
            // in i64, so any size exceeding i64::MAX reads to EOF.
            let read_limit = match size {
                Some(size) => i64::try_from(size).unwrap_or(0),
                None => 0,
            };
            Ok(Some((read_offset, read_limit)))
        }
    }
}

struct GcsGrpcReadStream {
    stream: tonic::Streaming<ReadObjectResponse>,
    done: bool,
    error_context: ErrorContext,
}

impl oio::ReadStream for GcsGrpcReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        if self.done {
            return Ok(Buffer::new());
        }
        loop {
            match self
                .stream
                .message()
                .await
                .map_err(|status| parse_status(self.error_context, status))?
            {
                Some(response) => {
                    if let Some(data) = response.checksummed_data
                        && !data.content.is_empty()
                    {
                        return Ok(Buffer::from(data.content));
                    }
                }
                None => {
                    self.done = true;
                    return Ok(Buffer::new());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_read_range_handles_bounds_suffix_zero_and_overflow() {
        assert_eq!(
            parse_read_range(BytesRange::default()).unwrap(),
            Some((0, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::from(..)).unwrap(),
            Some((0, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::from(10..30)).unwrap(),
            Some((10, 20))
        );
        assert_eq!(
            parse_read_range(BytesRange::from(10..)).unwrap(),
            Some((10, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::new(0, Some(i64::MAX as u64))).unwrap(),
            Some((0, i64::MAX))
        );

        // Ranges whose size exceeds i64::MAX read to EOF (read_limit = 0).
        assert_eq!(
            parse_read_range(BytesRange::from(0..=u64::MAX)).unwrap(),
            Some((0, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::from(1..=u64::MAX)).unwrap(),
            Some((1, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::new(5, Some(i64::MAX as u64 + 1))).unwrap(),
            Some((5, 0))
        );

        // Suffix ranges.
        assert_eq!(
            parse_read_range(BytesRange::suffix(4)).unwrap(),
            Some((-4, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::suffix(i64::MAX as u64)).unwrap(),
            Some((-i64::MAX, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::suffix(i64::MAX as u64 + 1)).unwrap(),
            Some((0, 0))
        );
        assert_eq!(
            parse_read_range(BytesRange::suffix(u64::MAX)).unwrap(),
            Some((0, 0))
        );

        // Zero-length ranges short-circuit to empty instead of unbounded reads.
        assert_eq!(parse_read_range(BytesRange::suffix(0)).unwrap(), None);
        assert_eq!(parse_read_range(BytesRange::new(0, Some(0))).unwrap(), None);
        assert_eq!(parse_read_range(BytesRange::new(5, Some(0))).unwrap(), None);
        assert_eq!(
            parse_read_range(BytesRange::new(u64::MAX, Some(0))).unwrap(),
            None
        );

        // Offsets exceeding i64::MAX are rejected as RangeNotSatisfied.
        let err = parse_read_range(BytesRange::from(u64::MAX..)).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);

        let err = parse_read_range(BytesRange::new(i64::MAX as u64 + 1, Some(1))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::RangeNotSatisfied);
    }
}
