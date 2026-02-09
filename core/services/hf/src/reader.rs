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

#[cfg(feature = "xet")]
use std::pin::Pin;
#[cfg(feature = "xet")]
use std::sync::Arc;

#[cfg(feature = "xet")]
use bytes::Bytes;
use http::Response;
use http::StatusCode;
use http::header;

#[cfg(feature = "xet")]
use cas_types::FileRange;
#[cfg(feature = "xet")]
use futures::StreamExt;

use super::core::HfCore;
#[cfg(feature = "xet")]
use super::core::{XetFile, XetTokenRefresher, map_xet_error};
use opendal_core::raw::*;
use opendal_core::*;

#[cfg(feature = "xet")]
type XetByteStream =
    Pin<Box<dyn futures::Stream<Item = xet_data::errors::Result<Bytes>> + Send + Sync>>;

pub enum HfReader {
    Http(HttpBody),
    #[cfg(feature = "xet")]
    Xet(XetByteStream),
}

impl HfReader {
    /// Create a reader, automatically choosing between XET and HTTP.
    ///
    /// When XET is enabled a HEAD request probes for the `X-Xet-Hash`
    /// header. Files stored on XET are downloaded via the CAS protocol;
    /// all others fall back to a regular HTTP GET.
    pub async fn try_new(core: &HfCore, path: &str, range: BytesRange) -> Result<Self> {
        #[cfg(feature = "xet")]
        if core.xet_enabled {
            if let Some(xet_file) = core.get_xet_file(path).await? {
                return Self::download_xet(core, &xet_file, range).await;
            }
        }

        Self::download_http(core, path, range).await
    }

    pub async fn download_http(core: &HfCore, path: &str, range: BytesRange) -> Result<Self> {
        let client = core.info.http_client();
        let uri = core.repo.uri(&core.root, path);
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
    pub async fn download_xet(
        core: &HfCore,
        xet_file: &XetFile,
        range: BytesRange,
    ) -> Result<Self> {
        let token = core.get_xet_token("read").await?;

        let file_info = xet_data::XetFileInfo::new(xet_file.hash.clone(), xet_file.size);

        let file_range = if !range.is_full() {
            let offset = range.offset();
            let size = range.size().unwrap_or(xet_file.size - offset);
            let end = offset + size;
            Some(FileRange::new(offset, end))
        } else {
            None
        };

        let refresher = Arc::new(XetTokenRefresher::new(core, "read"));

        let mut streams = xet_data::data_client::download_bytes_async(
            vec![file_info],
            Some(vec![file_range]),
            Some(token.cas_url),
            Some((token.access_token, token.exp)),
            Some(refresher),
            None,
            "opendal/1.0".to_string(),
            256,
        )
        .await
        .map_err(map_xet_error)?;

        let stream = streams.remove(0);
        Ok(Self::Xet(Box::pin(stream)))
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
    use super::super::core::HfCore;
    use super::super::uri::{HfRepo, RepoType};
    use super::*;

    /// Parquet magic bytes: "PAR1"
    const PARQUET_MAGIC: &[u8] = b"PAR1";

    fn testing_core(repo_type: RepoType, repo_id: &str, _xet: bool) -> HfCore {
        let info = AccessorInfo::default();
        info.set_scheme("huggingface")
            .set_native_capability(Capability {
                read: true,
                ..Default::default()
            });

        HfCore {
            info: info.into(),
            repo: HfRepo::new(
                repo_type,
                repo_id.to_string(),
                Some("main".to_string()),
            ),
            root: "/".to_string(),
            token: None,
            endpoint: "https://huggingface.co".to_string(),
            #[cfg(feature = "xet")]
            xet_enabled: _xet,
        }
    }

    async fn read_all(reader: &mut HfReader) -> Vec<u8> {
        use oio::Read;

        let mut buf = Vec::new();
        loop {
            let chunk = reader.read().await.expect("read should succeed");
            if chunk.is_empty() {
                break;
            }
            buf.extend_from_slice(&chunk.to_bytes());
        }
        buf
    }

    #[tokio::test]
    async fn test_download_http_model() {
        let core = testing_core(RepoType::Model, "openai-community/gpt2", false);
        let mut reader = HfReader::download_http(&core, "config.json", BytesRange::default())
            .await
            .expect("download should succeed");

        let data = read_all(&mut reader).await;
        serde_json::from_slice::<serde_json::Value>(&data)
            .expect("config.json should be valid JSON");
    }

    #[tokio::test]
    #[ignore]
    async fn test_download_http_dataset_parquet() {
        let core = testing_core(RepoType::Dataset, "google-research-datasets/mbpp", false);
        let range = BytesRange::new(0, Some(4));
        let mut reader = HfReader::download_http(&core, "full/train-00000-of-00001.parquet", range)
            .await
            .expect("download should succeed");

        let data = read_all(&mut reader).await;
        assert_eq!(&data, PARQUET_MAGIC);
    }

    #[tokio::test]
    #[ignore]
    async fn test_download_http_range() {
        let core = testing_core(RepoType::Dataset, "google-research-datasets/mbpp", false);
        let range = BytesRange::new(0, Some(4));
        let mut reader = HfReader::download_http(&core, "full/train-00000-of-00001.parquet", range)
            .await
            .expect("range download should succeed");

        let data = read_all(&mut reader).await;
        assert_eq!(data.len(), 4);
        assert_eq!(&data, PARQUET_MAGIC);
    }

    #[tokio::test]
    async fn test_download_dispatches_to_http() {
        let core = testing_core(RepoType::Model, "openai-community/gpt2", false);
        let reader = HfReader::try_new(&core, "config.json", BytesRange::default())
            .await
            .expect("download should succeed");

        assert!(matches!(reader, HfReader::Http(_)));
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore]
    async fn test_download_xet_parquet() {
        let core = testing_core(RepoType::Dataset, "google-research-datasets/mbpp", true);
        let xet_file = core
            .get_xet_file("full/train-00000-of-00001.parquet")
            .await
            .expect("xet probe should succeed")
            .expect("parquet file should be xet-backed");

        let mut reader = HfReader::download_xet(&core, &xet_file, BytesRange::default())
            .await
            .expect("xet download should succeed");

        let data = read_all(&mut reader).await;
        assert!(data.len() > 8);
        assert_eq!(&data[..4], PARQUET_MAGIC);
        assert_eq!(&data[data.len() - 4..], PARQUET_MAGIC);
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore]
    async fn test_download_xet_range() {
        let core = testing_core(RepoType::Dataset, "google-research-datasets/mbpp", true);
        let xet_file = core
            .get_xet_file("full/train-00000-of-00001.parquet")
            .await
            .expect("xet probe should succeed")
            .expect("parquet file should be xet-backed");

        let range = BytesRange::new(0, Some(4));
        let mut reader = HfReader::download_xet(&core, &xet_file, range)
            .await
            .expect("xet range download should succeed");

        let data = read_all(&mut reader).await;
        assert_eq!(data.len(), 4);
        assert_eq!(&data, PARQUET_MAGIC);
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore]
    async fn test_download_dispatches_to_xet() {
        let core = testing_core(RepoType::Dataset, "google-research-datasets/mbpp", true);
        let reader = HfReader::try_new(
            &core,
            "full/train-00000-of-00001.parquet",
            BytesRange::default(),
        )
        .await
        .expect("download should succeed");

        assert!(matches!(reader, HfReader::Xet(_)));
    }
}
