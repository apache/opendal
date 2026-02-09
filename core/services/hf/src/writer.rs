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
use http::StatusCode;
use sha2::{Digest, Sha256};

use super::core::{CommitFile, HfCore, LfsFile, PreuploadFile};
#[cfg(feature = "xet")]
use super::core::{XetTokenRefresher, map_xet_error};
use opendal_core::raw::*;
use opendal_core::*;

pub struct HfWriter {
    core: Arc<HfCore>,
    #[allow(dead_code)]
    op: OpWrite,
    path: String,
}

impl HfWriter {
    /// Create a writer.
    pub fn new(core: &Arc<HfCore>, path: &str, op: OpWrite) -> Self {
        Self {
            core: core.clone(),
            op,
            path: path.to_string(),
        }
    }

    /// Determine upload mode via preupload API.
    async fn determine_upload_mode(core: &HfCore, path: &str, body: &Buffer) -> Result<String> {
        let bytes = body.to_bytes();
        let size = bytes.len() as u64;

        // Compute SHA256 hash
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let sha256_hash = format!("{:x}", hasher.finalize());

        // Get sample (first 512 bytes, base64 encoded)
        let sample_size = std::cmp::min(512, bytes.len());
        let sample = base64::engine::general_purpose::STANDARD.encode(&bytes[..sample_size]);

        // Call preupload endpoint
        let preupload_files = vec![PreuploadFile {
            path: path.to_string(),
            size,
            sample,
            sha256: sha256_hash,
        }];

        let preupload_resp = core.preupload_files(preupload_files).await?;

        // Get upload mode from response
        let mode = preupload_resp
            .files
            .first()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "no files in preupload response"))?
            .upload_mode
            .clone();

        Ok(mode)
    }

    /// Prepare file content for HTTP storage (base64 encode for regular upload).
    async fn upload_http(path: &str, body: Buffer) -> Result<CommitFile> {
        let bytes = body.to_bytes();
        let content = base64::engine::general_purpose::STANDARD.encode(bytes);
        Ok(CommitFile {
            path: path.to_string(),
            content,
            encoding: "base64".to_string(),
        })
    }

    /// Upload file content to XET storage.
    #[cfg(feature = "xet")]
    async fn upload_xet(
        core: &HfCore,
        path: &str,
        body: Buffer,
    ) -> Result<LfsFile> {
        let bytes = body.to_bytes();
        let size = bytes.len() as u64;

        // Compute SHA256 hash for LFS OID
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let sha256_hash = format!("{:x}", hasher.finalize());

        // Upload to XET storage
        let token = core.get_xet_token("write").await?;
        let refresher = Arc::new(XetTokenRefresher::new(core, "write"));

        let file_contents = vec![bytes.to_vec()];

        let results = xet_data::data_client::upload_bytes_async(
            file_contents,
            Some(token.cas_url),
            Some((token.access_token, token.exp)),
            Some(refresher),
            None,
            "opendal/1.0".to_string(),
        )
        .await
        .map_err(map_xet_error)?;

        let _file_info = results.first().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "No file info returned from XET upload",
            )
        })?;

        Ok(LfsFile {
            path: path.to_string(),
            oid: sha256_hash,
            algo: "sha256".to_string(),
            size,
        })
    }

    /// Upload file and commit based on determined mode.
    ///
    /// Retries on commit conflicts (HTTP 412) and transient server errors
    /// (HTTP 5xx), matching the behavior of the official HuggingFace Hub
    /// client.
    async fn upload_and_commit(&self, body: Buffer) -> Result<Metadata> {
        const MAX_RETRIES: usize = 3;

        let mut last_err = None;
        for _ in 0..MAX_RETRIES {
            match self.try_upload_and_commit(body.clone()).await {
                Ok(meta) => return Ok(meta),
                Err(err)
                    if err.kind() == ErrorKind::ConditionNotMatch || err.is_temporary() =>
                {
                    last_err = Some(err);
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
        Err(last_err.unwrap())
    }

    async fn try_upload_and_commit(&self, body: Buffer) -> Result<Metadata> {
        #[cfg_attr(not(feature = "xet"), allow(unused_variables))]
        let mode = Self::determine_upload_mode(&self.core, &self.path, &body).await?;

        // Prepare file based on mode
        let (commit_file, lfs_file) = {
            #[cfg(feature = "xet")]
            {
                if self.core.xet_enabled && mode == "xet" {
                    let lfs = Self::upload_xet(&self.core, &self.path, body).await?;
                    (None, Some(lfs))
                } else {
                    let commit = Self::upload_http(&self.path, body).await?;
                    (Some(commit), None)
                }
            }
            #[cfg(not(feature = "xet"))]
            {
                let commit = Self::upload_http(&self.path, body).await?;
                (Some(commit), None)
            }
        };

        // Commit the files
        let regular_files: Vec<_> = commit_file.into_iter().collect();
        let lfs_files: Vec<_> = lfs_file.into_iter().collect();
        let resp = self.core.commit_files(regular_files, lfs_files).await?;

        match resp.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(Metadata::default()),
            _ => Err(super::error::parse_error(resp)),
        }
    }
}

impl oio::OneShotWrite for HfWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        self.upload_and_commit(bs).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::core::HfCore;
    use super::super::uri::{HfRepo, RepoType};
    use super::*;
    use oio::OneShotWrite;

    fn testing_core(_xet: bool) -> HfCore {
        let repo_id = std::env::var("HF_OPENDAL_DATASET").expect("HF_OPENDAL_DATASET must be set");

        let info = AccessorInfo::default();
        info.set_scheme("huggingface")
            .set_native_capability(Capability {
                write: true,
                ..Default::default()
            });

        HfCore {
            info: info.into(),
            repo: HfRepo::new(RepoType::Dataset, repo_id, Some("main".to_string())),
            root: "/".to_string(),
            token: std::env::var("HF_OPENDAL_TOKEN").ok(),
            endpoint: "https://huggingface.co".to_string(),
            #[cfg(feature = "xet")]
            xet_enabled: _xet,
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_upload_http() {
        let core = testing_core(false);

        let test_data = b"Hello, HuggingFace!";
        let buffer = Buffer::from(test_data.as_slice());

        let commit_file = HfWriter::upload_http("test-file.txt", buffer)
            .await
            .expect("upload should succeed");

        let resp = core
            .commit_files(vec![commit_file], vec![])
            .await
            .expect("commit should succeed");

        assert!(
            resp.status() == StatusCode::OK || resp.status() == StatusCode::CREATED,
            "expected OK or CREATED status, got {}",
            resp.status()
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_write_once_http() {
        let core = Arc::new(testing_core(false));

        let test_data = b"Test content for write_once";
        let buffer = Buffer::from(test_data.as_slice());

        let writer = HfWriter::new(&core, "write-once-test.txt", OpWrite::default());
        let result = writer.write_once(buffer).await;

        assert!(result.is_ok(), "write_once should succeed: {:?}", result);
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore]
    async fn test_upload_xet() {
        let core = testing_core(true);

        let test_data = b"Binary data for XET test";
        let buffer = Buffer::from(test_data.as_slice());

        let result = HfWriter::upload_xet(&core, "test-xet.bin", buffer).await;
        assert!(result.is_ok(), "xet upload should succeed: {:?}", result);
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore]
    async fn test_upload_and_commit_xet() {
        let core = testing_core(true);

        let test_data = b"Binary data for XET commit test";
        let buffer = Buffer::from(test_data.as_slice());

        let lfs_file = HfWriter::upload_xet(&core, "test-xet.bin", buffer)
            .await
            .expect("xet upload should succeed");

        let resp = core
            .commit_files(vec![], vec![lfs_file])
            .await
            .expect("commit should succeed");

        assert!(
            resp.status() == StatusCode::OK || resp.status() == StatusCode::CREATED,
            "expected OK or CREATED status, got {}",
            resp.status()
        );
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore]
    async fn test_write_once_dispatches_to_xet() {
        let core = Arc::new(testing_core(true));

        let test_data = b"Binary content for XET dispatch";
        let buffer = Buffer::from(test_data.as_slice());

        let writer = HfWriter::new(&core, "test-file.bin", OpWrite::default());
        let result = writer.write_once(buffer).await;

        assert!(
            result.is_ok(),
            "write_once with binary file should use xet: {:?}",
            result
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_upload_with_content_type() {
        let core = Arc::new(testing_core(false));

        let test_data = br#"{"test": "data"}"#;
        let buffer = Buffer::from(test_data.as_slice());

        let mut op = OpWrite::default();
        op = op.with_content_type("application/json");

        let writer = HfWriter::new(&core, "test.json", op);
        let result = writer.write_once(buffer).await;

        assert!(
            result.is_ok(),
            "upload with content type should succeed: {:?}",
            result
        );
    }
}
