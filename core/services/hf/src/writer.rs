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
use super::uri::RepoType;
use opendal_core::raw::*;
use opendal_core::*;
use xet::xet_session::{Sha256Policy, XetFileInfo, XetStreamUpload};

/// Tracks the close() progress so retries can resume from the last successful step.
enum CloseState {
    /// Stream is still being written to.
    Streaming(XetStreamUpload),
    /// stream.finish() succeeded; commit.commit() is next.
    Finished(XetFileInfo),
    /// CAS commit succeeded; the final registration (LFS or bucket_batch) is next.
    Committed(XetFileInfo),
    /// All done.
    Done,
}

/// Writer that always uses the XET protocol for uploads.
pub struct HfWriter {
    core: Arc<HfCore>,
    path: String,
    upload_commit: xet::xet_session::XetUploadCommit,
    state: CloseState,
}

impl HfWriter {
    pub async fn try_new(core: Arc<HfCore>, path: String) -> Result<Self> {
        let xet_session = core.new_xet_session()?;
        let refresh_url = core.repo.xet_token_url(&core.endpoint, "write");
        let refresh_headers = core.xet_token_refresh_headers();

        let upload_commit = xet_session
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
        let stream = upload_commit
            .upload_stream(None, Sha256Policy::Compute)
            .await
            .map_err(|err| {
                Error::new(ErrorKind::Unexpected, "failed to start xet upload stream")
                    .set_source(err)
            })?;
        Ok(HfWriter {
            core,
            path,
            upload_commit,
            state: CloseState::Streaming(stream),
        })
    }

    /// Register the uploaded file with HF (LFS commit or bucket batch).
    async fn register_file(&self, file_info: &XetFileInfo) -> Result<Metadata> {
        let content_length = file_info
            .file_size()
            .expect("file_size must be set after finish()");
        let meta = Metadata::default().with_content_length(content_length);

        let repo_path = self.core.repo_path(&self.path);
        if self.core.repo.repo_type == RepoType::Bucket {
            let xet_hash = file_info.hash().to_string();
            self.core
                .bucket_batch(vec![BucketOperation::AddFile {
                    path: repo_path,
                    xet_hash,
                }])
                .await?;
            Ok(meta)
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
            let resp = self.core.commit_files(vec![], vec![lfs_file], vec![]).await?;
            Ok(if let Some(oid) = resp.commit_oid {
                meta.with_version(oid)
            } else {
                meta
            })
        }
    }
}

impl oio::Write for HfWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let CloseState::Streaming(stream) = &self.state else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "cannot write: upload stream is no longer active",
            ));
        };
        stream.write(bs.to_bytes()).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to write xet chunk").set_source(err)
        })
    }

    async fn close(&mut self) -> Result<Metadata> {
        // Step 1: finish the upload stream.
        if let CloseState::Streaming(stream) = &self.state {
            let file_meta = stream.finish().await.map_err(|err| {
                Error::new(ErrorKind::Unexpected, "failed to finish xet upload").set_source(err)
            })?;
            self.state = CloseState::Finished(file_meta.xet_info);
        }

        // Step 2: commit data to CAS. Ignore "Already completed" on retry.
        if let CloseState::Finished(file_info) = &self.state {
            let file_info = file_info.clone();
            match self.upload_commit.commit().await {
                Ok(_) => {}
                Err(e) if e.to_string().contains("Already completed") => {}
                Err(e) => {
                    return Err(
                        Error::new(ErrorKind::Unexpected, "failed to commit xet upload")
                            .set_source(e),
                    );
                }
            }
            self.state = CloseState::Committed(file_info);
        }

        // Step 3: register the file with HF (LFS commit or bucket batch).
        if let CloseState::Committed(file_info) = &self.state {
            let file_info = file_info.clone();
            let meta = self.register_file(&file_info).await?;
            self.state = CloseState::Done;
            return Ok(meta);
        }

        Err(Error::new(
            ErrorKind::Unexpected,
            "close called on already-completed writer",
        ))
    }

    async fn abort(&mut self) -> Result<()> {
        if let CloseState::Streaming(stream) = &self.state {
            stream.abort();
        }
        self.state = CloseState::Done;
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
