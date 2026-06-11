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

use bytes::Buf;

use xet::xet_session::{SessionError, XetDownloadStream, XetFileInfo};

use super::core::{HfCore, XetFileResponse};
use opendal_core::BytesRange;
use opendal_core::raw::*;
use opendal_core::*;

pub enum HfReadStream {
    Http(HttpBody),
    Xet(XetDownloadStream),
}

impl HfReadStream {
    pub async fn try_new(core: &HfCore, path: &str, range: BytesRange) -> Result<(RpRead, Self)> {
        let resp = core.resolve(path, range, core.download_mode).await?;
        if resp.headers().contains_key("x-xet-hash") {
            let (_, mut body) = resp.into_parts();
            let buf = body.to_buffer().await?;
            let info: XetFileResponse =
                serde_json::from_reader(buf.reader()).map_err(new_json_deserialize_error)?;
            let metadata = Metadata::new(EntryMode::FILE).with_content_length(info.size);
            let reader =
                Self::try_new_xet(core, &XetFileInfo::new(info.hash, info.size), range).await?;
            Ok((RpRead::new(metadata), reader))
        } else {
            let metadata = parse_into_metadata(path, resp.headers())?;
            Ok((RpRead::new(metadata), Self::Http(resp.into_body())))
        }
    }

    async fn try_new_xet(
        core: &HfCore,
        file_info: &XetFileInfo,
        range: BytesRange,
    ) -> Result<Self> {
        let group = core.xet_download_group().await?;

        let xet_range = if range.is_full() {
            None
        } else {
            let start = range.offset();
            let end = range.size().map(|s| start + s).unwrap_or(u64::MAX);
            Some(start..end)
        };

        let mut stream = group
            .download_stream(file_info.clone(), xet_range)
            .await
            .map_err(|err| {
                Error::new(
                    ErrorKind::Unexpected,
                    "failed to create xet download stream",
                )
                .set_source(err)
            })?;
        stream.start();
        Ok(Self::Xet(stream))
    }
}

fn map_session_error(e: SessionError) -> Error {
    Error::new(ErrorKind::Unexpected, "xet read error").set_source(e)
}

impl oio::ReadStream for HfReadStream {
    async fn read(&mut self) -> Result<Buffer> {
        match self {
            Self::Http(body) => body.read().await,
            Self::Xet(stream) => match stream.next().await {
                Ok(Some(bytes)) => Ok(Buffer::from(bytes)),
                Ok(None) => Ok(Buffer::new()),
                Err(e) => Err(map_session_error(e)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::test_utils::{gpt2_operator, mbpp_operator, testing_dataset_core};
    use super::super::core::test_utils::create_test_core;
    use super::super::core::{CommitFile, DeletedFile};
    use super::super::uri::HfRepoType;
    use super::*;
    use bytes::Bytes;
    use opendal_core::raw::oio::ReadStream;

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
    async fn test_http_read_returns_metadata() -> Result<()> {
        let (core, _) = create_test_core(
            HfRepoType::Model,
            "test-user/test-repo",
            "main",
            "https://huggingface.co",
        );

        let (rp, mut reader) =
            HfReadStream::try_new(&core, "test.txt", BytesRange::default()).await?;
        let metadata = rp.metadata().expect("read metadata must be returned");

        assert_eq!(metadata.mode(), EntryMode::FILE);
        assert_eq!(metadata.content_length(), 5);
        assert!(matches!(reader, HfReadStream::Http(_)));

        let chunk = reader.read().await?;
        assert_eq!(chunk.to_bytes(), Bytes::from_static(b"hello"));

        Ok(())
    }

    /// Exercises the XET download code path against a public dataset known to
    /// have XET-stored files. Behavior tests cannot reliably cover this path
    /// because the test dataset may not contain any XET files.
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

    /// Verifies that a non-XET file (plain git blob) read in Xet mode falls back
    /// to the HTTP body path rather than erroring. Uploads a small file via the
    /// git commit API (which does not go through XET), then reads it back.
    /// Requires HF_OPENDAL_DATASET and HF_OPENDAL_TOKEN.
    #[tokio::test]
    #[ignore = "requires network access"]
    async fn test_xet_mode_falls_back_to_http_for_non_xet_file() {
        use base64::Engine;

        let core = testing_dataset_core();
        let content = b"non-xet fallback test content";
        let path = "tests/non-xet-fallback.txt";

        core.commit_git(
            vec![CommitFile {
                path: path.to_string(),
                content: base64::prelude::BASE64_STANDARD.encode(content),
                encoding: "base64".to_string(),
            }],
            vec![],
            vec![],
            vec![],
        )
        .await
        .expect("commit should succeed");

        let (_, mut reader) = HfReadStream::try_new(&core, path, BytesRange::default())
            .await
            .expect("reading non-XET file in Xet mode should succeed via HTTP fallback");

        assert!(
            matches!(reader, HfReadStream::Http(_)),
            "expected HTTP reader for non-XET file"
        );

        let mut buf = Vec::new();
        loop {
            let chunk: Buffer = reader.read().await.expect("read chunk should succeed");
            if chunk.is_empty() {
                break;
            }
            buf.extend_from_slice(&chunk.to_bytes());
        }
        assert_eq!(buf, content);

        core.commit_git(
            vec![],
            vec![],
            vec![DeletedFile {
                path: path.to_string(),
            }],
            vec![],
        )
        .await
        .ok();
    }

    /// Exercises XET range reads (XetDownloadStream with a byte range).
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
