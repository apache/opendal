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

use base64::Engine;

use super::core::{BucketOperation, CommitFile, HfCore, LfsFile};
use super::uri::RepoType;
use opendal_core::raw::*;
use opendal_core::*;
use xet::xet_session::{Sha256Policy, XetStreamUpload, XetUploadCommit};

/// Writer that handles both regular (small) and XET (large) file uploads.
pub enum HfWriter {
    /// Regular writer for small files using base64 inline commit.
    Regular {
        core: Arc<HfCore>,
        path: String,
        buf: Vec<Buffer>,
    },
    /// XET writer for large files using the session-based upload protocol.
    Xet {
        core: Arc<HfCore>,
        path: String,
        commit: XetUploadCommit,
        stream: Option<XetStreamUpload>,
    },
}

impl HfWriter {
    /// Create a new writer by determining the upload mode from the API.
    pub async fn try_new(core: Arc<HfCore>, path: String) -> Result<Self> {
        // Buckets always use XET and don't have a preupload endpoint;
        // other repo types check the preupload API.
        let use_xet = core.repo.repo_type == RepoType::Bucket
            || core.determine_upload_mode(&path).await? == "lfs";

        let writer = if use_xet {
            let refresh_url = core.repo.xet_token_url(&core.endpoint, "write");
            let refresh_headers = core.xet_token_refresh_headers();

            let commit = core
                .xet_session
                .new_upload_commit()
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "failed to create xet upload commit")
                        .set_source(err)
                })?
                .with_token_refresh_url(refresh_url, refresh_headers)
                .build()
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "failed to build xet upload commit")
                        .set_source(err)
                })?;
            let stream = commit
                .upload_stream(None, Sha256Policy::Compute)
                .await
                .map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "failed to start xet upload stream")
                        .set_source(err)
                })?;
            HfWriter::Xet {
                core,
                path,
                commit,
                stream: Some(stream),
            }
        } else {
            HfWriter::Regular {
                core,
                path,
                buf: Vec::new(),
            }
        };
        Ok(writer)
    }

    fn prepare_commit_file(path: &str, body: &[u8]) -> CommitFile {
        let content = base64::engine::general_purpose::STANDARD.encode(body);
        CommitFile {
            path: path.to_string(),
            content,
            encoding: "base64".to_string(),
        }
    }
}

impl oio::Write for HfWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        match self {
            HfWriter::Regular { buf, .. } => {
                buf.push(bs);
                Ok(())
            }
            HfWriter::Xet { stream, .. } => {
                let stream = stream.as_ref().expect("stream missing");
                stream.write(bs.to_bytes()).await.map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "failed to write xet chunk").set_source(err)
                })
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match self {
            HfWriter::Regular {
                core, path, buf, ..
            } => {
                // Flatten buffer
                let mut data = Vec::new();
                for buf in std::mem::take(buf) {
                    data.extend_from_slice(&buf.to_bytes());
                }

                let file = Self::prepare_commit_file(path, &data);
                let resp = core.commit_files(vec![file], vec![], vec![]).await?;

                let mut meta = Metadata::default().with_content_length(data.len() as u64);
                if let Some(commit_oid) = resp.commit_oid {
                    meta = meta.with_version(commit_oid);
                }
                Ok(meta)
            }
            HfWriter::Xet {
                core,
                path,
                commit,
                stream,
            } => {
                let stream = stream.take().ok_or_else(|| {
                    Error::new(ErrorKind::Unexpected, "xet writer already closed")
                })?;

                let file_meta = stream.finish().await.map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "failed to finish xet upload").set_source(err)
                })?;
                let file_info = file_meta.xet_info;
                let content_length = file_info
                    .file_size()
                    .expect("file_size must be set after finish()");
                let meta = Metadata::default().with_content_length(content_length);

                commit.commit().await.map_err(|err| {
                    Error::new(ErrorKind::Unexpected, "failed to commit xet upload").set_source(err)
                })?;

                if core.repo.repo_type == RepoType::Bucket {
                    let xet_hash = file_info.hash().to_string();
                    core.bucket_batch(vec![BucketOperation::AddFile {
                        path: path.clone(),
                        xet_hash,
                    }])
                    .await?;
                    Ok(meta)
                } else {
                    let sha256 = file_info.sha256().ok_or_else(|| {
                        Error::new(ErrorKind::Unexpected, "xet upload returned no sha256")
                    })?;
                    let lfs_file = LfsFile {
                        path: path.clone(),
                        oid: sha256.to_string(),
                        algo: "sha256".to_string(),
                        size: content_length,
                    };
                    let resp = core.commit_files(vec![], vec![lfs_file], vec![]).await?;
                    Ok(if let Some(oid) = resp.commit_oid {
                        meta.with_version(oid)
                    } else {
                        meta
                    })
                }
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self {
            HfWriter::Regular { buf, .. } => {
                buf.clear();
            }
            HfWriter::Xet { stream, .. } => {
                if let Some(s) = stream.take() {
                    s.abort();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn test_prepare_commit_file() {
        let content = b"Hello, World!";
        let file = HfWriter::prepare_commit_file("data/test.txt", content);

        assert_eq!(file.path, "data/test.txt");
        assert_eq!(file.encoding, "base64");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&file.content)
            .unwrap();
        assert_eq!(decoded, content);
    }

    #[test]
    fn test_prepare_commit_file_empty() {
        let file = HfWriter::prepare_commit_file("empty.bin", &[]);

        assert_eq!(file.path, "empty.bin");
        assert_eq!(file.encoding, "base64");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&file.content)
            .unwrap();
        assert!(decoded.is_empty());
    }

    /// Bucket writes always use XET and commit via NDJSON batch — this code
    /// path is not covered by behavior tests unless run with a bucket config.
    /// Requires HF_OPENDAL_BUCKET and HF_OPENDAL_TOKEN.
    #[tokio::test]
    #[ignore]
    async fn test_bucket_write_roundtrip() {
        let op = super::super::backend::test_utils::testing_bucket_operator();
        let path = "tests/bucket-roundtrip.bin";
        let content = b"Binary content for bucket roundtrip test";

        op.write(path, content.as_slice())
            .await
            .expect("bucket write should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), content);

        let _ = op.delete(path).await;
    }
}
