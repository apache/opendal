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
use xet::xet_session::{Sha256Policy, XetFileInfo, XetStreamUpload, XetUploadCommit};

/// CAS propagation can lag behind `commit.commit()`. Retry the HF
/// registration call a few times with exponential backoff.
async fn retry_on_cas_delay<T, F, Fut>(op: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let delays = [1000, 2000, 4000, 8000];
    let mut last_err = None;
    for (i, delay_ms) in std::iter::once(&0).chain(delays.iter()).enumerate() {
        if i > 0 {
            std::thread::sleep(std::time::Duration::from_millis(*delay_ms));
        }
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("not found in Xet storage")
                    || msg.contains("LFS pointer pointed to a file that does not exist")
                    || msg.contains("Invalid file change")
                {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }
    Err(last_err.unwrap())
}

/// Writer that always uses the XET protocol for uploads.
pub struct HfWriter {
    core: Arc<HfCore>,
    path: String,
    commit: XetUploadCommit,
    stream: Option<XetStreamUpload>,
}

impl HfWriter {
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
            commit,
            stream: Some(stream),
        })
    }

    /// Register the uploaded file with HF (LFS commit or bucket batch).
    ///
    /// Retries on transient CAS propagation failures where the file
    /// data hasn't propagated yet after `commit.commit()`.
    async fn register_file(&self, file_info: &XetFileInfo) -> Result<Metadata> {
        let content_length = file_info
            .file_size()
            .expect("file_size must be set after finish()");
        let meta = Metadata::default().with_content_length(content_length);

        let repo_path = self.core.repo_path(&self.path);
        if self.core.repo.repo_type == RepoType::Bucket {
            let xet_hash = file_info.hash().to_string();
            let op = || async {
                self.core
                    .bucket_batch(vec![BucketOperation::AddFile {
                        path: repo_path.clone(),
                        xet_hash: xet_hash.clone(),
                    }])
                    .await
            };
            retry_on_cas_delay(op).await?;
            Ok(meta)
        } else {
            let sha256 = file_info.sha256().ok_or_else(|| {
                Error::new(ErrorKind::Unexpected, "xet upload returned no sha256")
            })?;
            let op = || async {
                let lfs_file = LfsFile {
                    path: repo_path.clone(),
                    oid: sha256.to_string(),
                    algo: "sha256".to_string(),
                    size: content_length,
                };
                self.core
                    .commit_files(vec![], vec![lfs_file], vec![])
                    .await
            };
            retry_on_cas_delay(op).await?;
            Ok(meta)
        }
    }
}

impl oio::Write for HfWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let stream = self.stream.as_ref().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;
        stream.write(bs.to_bytes()).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to write xet chunk").set_source(err)
        })
    }

    async fn close(&mut self) -> Result<Metadata> {
        let stream = self.stream.take().ok_or_else(|| {
            Error::new(ErrorKind::Unexpected, "writer has been closed or aborted")
        })?;

        let file_meta = stream.finish().await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "failed to finish xet upload").set_source(err)
        })?;

        // commit.commit() may return "Already completed" if finish()
        // already committed internally — ignore that error.
        match self.commit.commit().await {
            Ok(_) => {}
            Err(e) if e.to_string().contains("Already completed") => {}
            Err(e) => {
                return Err(
                    Error::new(ErrorKind::Unexpected, "failed to commit xet upload")
                        .set_source(e),
                );
            }
        }

        self.register_file(&file_meta.xet_info).await
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(stream) = self.stream.take() {
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
