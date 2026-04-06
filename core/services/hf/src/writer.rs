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

use super::core::{BucketOperation, HfCore, LfsFile};
use opendal_core::raw::*;
use opendal_core::*;
use xet::xet_session::{Sha256Policy, XetFileInfo, XetStreamUpload, XetUploadCommit};

/// Writer that always uses the XET protocol for uploads.
pub struct HfWriter {
    core: Arc<HfCore>,
    path: String,
    xet_commit: XetUploadCommit,
    xet_stream: Option<XetStreamUpload>,
}

impl HfWriter {
    /// Create a new writer for the given path.
    ///
    /// All uploads go through the XET protocol regardless of file size.
    /// An XET upload commit and stream are created eagerly so that
    /// subsequent [`write`](oio::Write::write) calls can stream data
    /// directly into the XET CAS.
    pub async fn try_new(core: Arc<HfCore>, path: String) -> Result<Self> {
        let commit = core.xet_upload_commit().await?;
        let stream = commit
            .upload_stream(None, Sha256Policy::Compute)
            .await
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "failed to start xet upload stream")
                    .set_source(err)
            })?;
        Ok(HfWriter {
            core,
            path,
            xet_commit: commit,
            xet_stream: Some(stream),
        })
    }

    /// Finalize the XET upload and register the file with HF.
    ///
    /// 1. Commit data to XET CAS (may already be done by `stream.finish()`)
    /// 2. Register the file via git LFS commit or bucket batch API
    ///
    /// Retries on transient CAS propagation delays.
    async fn commit(&mut self, file_info: &XetFileInfo) -> Result<Metadata> {
        // Finalize the XET CAS upload. May return "Already completed"
        // if stream.finish() already committed internally.
        match self.xet_commit.commit().await {
            Ok(_) => {}
            Err(e) if e.to_string().contains("Already completed") => {}
            Err(e) => {
                return Err(
                    Error::new(ErrorKind::Unexpected, "failed to commit xet upload")
                        .set_source(e),
                );
            }
        }

        let content_length = file_info
            .file_size()
            .expect("file_size must be set after finish()");
        let meta = Metadata::default().with_content_length(content_length);

        let repo_path = self.core.repo_path(&self.path);
        if self.core.is_bucket() {
            let xet_hash = file_info.hash().to_string();
            self.core
                .commit_bucket(vec![BucketOperation::AddFile {
                    path: repo_path,
                    xet_hash,
                }])
                .await?;
        } else {
            let sha256 = file_info.sha256().ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "xet upload returned no sha256")
            })?;
            let lfs_file = LfsFile {
                path: repo_path,
                oid: sha256.to_string(),
                algo: "sha256".to_string(),
                size: content_length,
            };
            self.core.commit_git(vec![], vec![lfs_file], vec![]).await?;
        }

        Ok(meta)
    }
}

impl oio::Write for HfWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let stream = self.xet_stream.as_ref().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;
        stream.write(bs.to_bytes()).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to write xet chunk").set_source(err)
        })
    }

    async fn close(&mut self) -> Result<Metadata> {
        let stream = self.xet_stream.take().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        let file_meta = stream.finish().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to finish xet upload").set_source(err)
        })?;

        self.commit(&file_meta.xet_info).await
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(stream) = self.xet_stream.take() {
            stream.abort();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::test_utils::testing_bucket_operator;

    /// Bucket writes always use XET and commit via NDJSON batch — this code
    /// path is not covered by behavior tests unless run with a bucket config.
    /// Requires HF_OPENDAL_BUCKET and HF_OPENDAL_TOKEN.
    #[tokio::test]
    #[ignore]
    async fn test_bucket_write_roundtrip() {
        let op = testing_bucket_operator();
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
