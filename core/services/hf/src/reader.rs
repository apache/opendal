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

use http::Response;
use http::StatusCode;
use http::header;

#[cfg(feature = "xet")]
use cas_types::FileRange;
#[cfg(feature = "xet")]
use futures::StreamExt;

use super::core::HfCore;
#[cfg(feature = "xet")]
use super::core::map_xet_error;
#[cfg(feature = "xet")]
use super::uri::RepoType;
use opendal_core::raw::*;
use opendal_core::*;
#[cfg(feature = "xet")]
use xet_data::XetFileInfo;
#[cfg(feature = "xet")]
use xet_data::streaming::XetReader;

pub enum HfReader {
    Http(HttpBody),
    #[cfg(feature = "xet")]
    Xet(XetReader),
}

impl HfReader {
    /// Create a reader, automatically choosing between XET and HTTP.
    ///
    /// Buckets always use XET. For other repo types, when XET is enabled
    /// a HEAD request probes for the `X-Xet-Hash` header. Files stored on
    /// XET are downloaded via the CAS protocol; all others fall back to HTTP GET.
    pub async fn try_new(core: &HfCore, path: &str, range: BytesRange) -> Result<Self> {
        #[cfg(feature = "xet")]
        if core.xet_enabled {
            // Buckets always use XET
            if core.repo.repo_type == RepoType::Bucket {
                if let Some(xet_file) = core.maybe_xet_file(path).await? {
                    return Self::try_new_xet(core, &xet_file, range).await;
                }
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "bucket file is missing XET metadata",
                ));
            }

            // For other repos, probe for XET
            if let Some(xet_file) = core.maybe_xet_file(path).await? {
                return Self::try_new_xet(core, &xet_file, range).await;
            }
        }

        Self::try_new_http(core, path, range).await
    }

    pub async fn try_new_http(core: &HfCore, path: &str, range: BytesRange) -> Result<Self> {
        let client = core.info.http_client();
        let uri = core.uri(path);
        let url = uri.resolve_url(&core.endpoint);

        let mut req = core.request(http::Method::GET, &url, Operation::Read);

        if !range.is_full() {
            req = req.header(header::RANGE, range.to_header());
        }

        let req = req.body(Buffer::new()).map_err(new_request_build_error)?;

        let resp = client.fetch(req).await?;
        let status = resp.status();

        match status {
            StatusCode::OK | StatusCode::PARTIAL_CONTENT => Ok(Self::Http(resp.into_body())),
            _ => {
                let (part, mut body) = resp.into_parts();
                let buf = body.to_buffer().await?;
                Err(super::error::parse_error(Response::from_parts(part, buf)))
            }
        }
    }

    #[cfg(feature = "xet")]
    async fn try_new_xet(
        core: &HfCore,
        file_info: &XetFileInfo,
        range: BytesRange,
    ) -> Result<Self> {
        let client = core.xet_client("read").await?;

        let file_range = if !range.is_full() {
            let offset = range.offset();
            let size = range.size().unwrap_or(file_info.file_size() - offset);
            Some(FileRange::new(offset, offset + size))
        } else {
            None
        };

        let reader = client
            .read(file_info.clone(), file_range, None, 256)
            .map_err(map_xet_error)?;
        Ok(Self::Xet(reader))
    }
}

impl oio::Read for HfReader {
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            Self::Http(body) => body.read().await,
            #[cfg(feature = "xet")]
            Self::Xet(stream) => match stream.next().await {
                Some(Ok(bytes)) => Ok(Buffer::from(bytes)),
                Some(Err(e)) => Err(map_xet_error(e)),
                None => Ok(Buffer::new()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "xet")]
    use super::super::backend::test_utils::mbpp_xet_operator;
    use super::super::backend::test_utils::{gpt2_operator, mbpp_operator};

    /// Parquet magic bytes: "PAR1"
    const PARQUET_MAGIC: &[u8] = b"PAR1";

    #[tokio::test]
    async fn test_read_model_config() {
        let op = gpt2_operator();
        let data = op.read("config.json").await.expect("read should succeed");
        serde_json::from_slice::<serde_json::Value>(&data.to_vec())
            .expect("config.json should be valid JSON");
    }

    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_http_parquet_header() {
        let op = mbpp_operator();
        let data = op
            .read_with("full/train-00000-of-00001.parquet")
            .range(0..4)
            .await
            .expect("read should succeed");
        assert_eq!(&data.to_vec(), PARQUET_MAGIC);
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_xet_parquet() {
        let op = mbpp_xet_operator();
        let data = op
            .read("full/train-00000-of-00001.parquet")
            .await
            .expect("xet read should succeed");
        let bytes = data.to_vec();
        assert!(bytes.len() > 8);
        assert_eq!(&bytes[..4], PARQUET_MAGIC);
        assert_eq!(&bytes[bytes.len() - 4..], PARQUET_MAGIC);
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_xet_range() {
        let op = mbpp_xet_operator();
        let data = op
            .read_with("full/train-00000-of-00001.parquet")
            .range(0..4)
            .await
            .expect("xet range read should succeed");
        let bytes = data.to_vec();
        assert_eq!(bytes.len(), 4);
        assert_eq!(&bytes, PARQUET_MAGIC);
    }
}
