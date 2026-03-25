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

use xet_client::ClientError;
use xet_data::file_reconstruction::FileReconstructionError;
use xet_data::processing::DownloadStream;
use xet_data::processing::XetFileInfo;

use super::core::HfCore;
use super::uri::RepoType;
use opendal_core::raw::*;
use opendal_core::*;

pub enum HfReader {
    Http(HttpBody),
    Xet(DownloadStream),
}

impl HfReader {
    /// Create a reader, automatically choosing between XET and HTTP.
    ///
    /// Buckets always use XET. For other repo types, a HEAD request
    /// probes for the `X-Xet-Hash` header. Files stored on XET are
    /// downloaded via the CAS protocol; all others fall back to HTTP GET.
    pub async fn try_new(core: &HfCore, path: &str, range: BytesRange) -> Result<Self> {
        if let Some(xet_file) = core.maybe_xet_file(path).await? {
            return Self::try_new_xet(core, &xet_file, range).await;
        }

        if core.repo.repo_type == RepoType::Bucket {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "bucket file is missing XET metadata",
            ));
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

    async fn try_new_xet(
        core: &HfCore,
        file_info: &XetFileInfo,
        range: BytesRange,
    ) -> Result<Self> {
        let session = core.xet_download_session().await?;

        let (_id, stream) = session
            .download_stream_range(file_info, range.to_range())
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to create xet download stream",
                )
                .set_source(err)
            })?;
        Ok(Self::Xet(stream))
    }
}

fn map_reconstruction_error(e: FileReconstructionError) -> Error {
    let kind = match &e {
        FileReconstructionError::ClientError(arc) => match arc.as_ref() {
            ClientError::FileNotFound(_) | ClientError::XORBNotFound(_) => ErrorKind::NotFound,
            ClientError::InvalidRange => ErrorKind::RangeNotSatisfied,
            _ => ErrorKind::Unexpected,
        },
        _ => ErrorKind::Unexpected,
    };
    Error::new(kind, "xet read error").set_source(e)
}

impl oio::Read for HfReader {
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            Self::Http(body) => body.read().await,
            Self::Xet(stream) => match stream.next().await {
                Ok(Some(bytes)) => Ok(Buffer::from(bytes)),
                Ok(None) => Ok(Buffer::new()),
                Err(e) => Err(map_reconstruction_error(e)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
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

    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_xet_parquet() {
        let op = mbpp_operator();
        let data = op
            .read("full/train-00000-of-00001.parquet")
            .await
            .expect("xet read should succeed");
        let bytes = data.to_vec();
        assert!(bytes.len() > 8);
        assert_eq!(&bytes[..4], PARQUET_MAGIC);
        assert_eq!(&bytes[bytes.len() - 4..], PARQUET_MAGIC);
    }

    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_read_xet_range() {
        let op = mbpp_operator();
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
