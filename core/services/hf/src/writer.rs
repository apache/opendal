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

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine;
use http::Request;
use http::header;
use sha2::{Digest, Sha256};

#[cfg(feature = "xet")]
use super::core::XetTokenRefresher;
use super::core::{CommitFile, CommitResponse, HfCore, LfsFile};
use opendal_core::raw::*;
use opendal_core::*;

#[derive(serde::Serialize)]
struct PreuploadFile {
    path: String,
    size: u64,
    sample: String,
    #[serde(rename = "sha256")]
    sha256: String,
}

#[derive(serde::Serialize)]
struct PreuploadRequest {
    files: Vec<PreuploadFile>,
}

#[derive(serde::Deserialize, Debug)]
struct PreuploadFileResponse {
    #[allow(dead_code)]
    path: String,
    #[serde(rename = "uploadMode")]
    upload_mode: String,
}

#[derive(serde::Deserialize, Debug)]
struct PreuploadResponse {
    files: Vec<PreuploadFileResponse>,
}

#[derive(serde::Serialize)]
struct LfsBatchRequest {
    operation: String,
    transfers: Vec<String>,
    objects: Vec<LfsBatchRequestObject>,
    hash_algo: String,
}

#[derive(serde::Serialize)]
struct LfsBatchRequestObject {
    oid: String,
    size: u64,
}

#[derive(serde::Deserialize)]
struct LfsBatchResponse {
    transfer: Option<String>,
    #[serde(default)]
    objects: Vec<LfsBatchResponseObject>,
}

#[derive(serde::Deserialize)]
struct LfsBatchResponseObject {
    actions: Option<LfsBatchActions>,
    error: Option<LfsBatchError>,
}

#[derive(serde::Deserialize)]
struct LfsBatchActions {
    upload: LfsBatchAction,
    verify: Option<LfsBatchAction>,
}

#[derive(serde::Deserialize)]
struct LfsBatchAction {
    href: String,
    #[serde(default)]
    header: HashMap<String, serde_json::Value>,
}

#[derive(serde::Deserialize)]
struct LfsBatchError {
    message: String,
}

#[derive(serde::Serialize)]
struct LfsVerifyRequest {
    oid: String,
    size: u64,
}

/// Resolved upload strategy after consulting the preupload and LFS batch APIs.
enum UploadMode {
    /// Small file: base64 encode inline in commit payload.
    Regular,
    /// File already exists in LFS storage, just commit pointer.
    LfsExists,
    /// Single-part LFS upload: PUT entire body to pre-signed URL.
    LfsSinglepart {
        upload: LfsBatchAction,
        verify: Option<LfsBatchAction>,
    },
    /// Multi-part LFS upload: PUT chunks to numbered pre-signed URLs.
    LfsMultipart {
        upload: LfsBatchAction,
        verify: Option<LfsBatchAction>,
        chunk_size: usize,
    },
    /// XET transfer protocol.
    #[cfg(feature = "xet")]
    Xet,
}

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

    /// Determine the upload strategy for a file.
    ///
    /// Follows the HuggingFace Hub upload protocol:
    /// 1. Compute SHA256 hash and a content sample for the preupload API.
    /// 2. Call the preupload API to determine if the file should be uploaded
    ///    as "regular" (base64 inline in commit) or "lfs" (Git LFS).
    /// 3. For LFS files, negotiate the transfer adapter with the LFS batch
    ///    API which returns pre-signed upload URLs or Xet.
    ///
    /// Returns the resolved upload mode and the SHA256 OID.
    async fn determine_upload_mode(&self, body: &Buffer) -> Result<(UploadMode, String)> {
        let bytes = body.to_bytes();
        let size = bytes.len() as u64;

        // Step 1: compute SHA256 and content sample.
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        let oid = format!("{:x}", hasher.finalize());

        let sample_size = std::cmp::min(512, bytes.len());
        let sample = base64::engine::general_purpose::STANDARD.encode(&bytes[..sample_size]);

        // Step 2: call preupload API to get "regular" or "lfs".
        let uri = self.core.uri(&self.path);
        let preupload_url = uri.preupload_url(&self.core.endpoint);

        let preupload_payload = PreuploadRequest {
            files: vec![PreuploadFile {
                path: self.path.clone(),
                size,
                sample,
                sha256: oid.clone(),
            }],
        };
        let json_body = serde_json::to_vec(&preupload_payload).map_err(new_json_serialize_error)?;

        let req = self
            .core
            .request(http::Method::POST, &preupload_url, Operation::Write)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        let (_, preupload_resp): (_, PreuploadResponse) = self.core.send_parse(req).await?;

        let mode = preupload_resp
            .files
            .first()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "no files in preupload response"))?
            .upload_mode
            .clone();

        if mode != "lfs" {
            return Ok((UploadMode::Regular, oid));
        }

        // Step 3: negotiate transfer adapter with the LFS batch API.
        let url = self.core.repo.lfs_batch_url(&self.core.endpoint);

        #[allow(unused_mut)]
        let mut transfers = vec!["basic".to_string(), "multipart".to_string()];
        #[cfg(feature = "xet")]
        if self.core.xet_enabled {
            transfers.push("xet".to_string());
        }

        let payload = LfsBatchRequest {
            operation: "upload".to_string(),
            transfers,
            objects: vec![LfsBatchRequestObject {
                oid: oid.clone(),
                size,
            }],
            hash_algo: "sha256".to_string(),
        };
        let json_body = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;

        let req = self
            .core
            .request(http::Method::POST, &url, Operation::Write)
            .header(header::ACCEPT, "application/vnd.git-lfs+json")
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(Buffer::from(json_body))
            .map_err(new_request_build_error)?;

        let (_, batch_resp): (_, LfsBatchResponse) = self.core.send_parse(req).await?;

        #[cfg_attr(not(feature = "xet"), allow(unused_variables))]
        let chosen_transfer = batch_resp.transfer;

        let obj = batch_resp
            .objects
            .into_iter()
            .next()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "empty LFS batch response"))?;

        if let Some(err) = obj.error {
            return Err(Error::new(ErrorKind::Unexpected, err.message));
        }

        // No actions means the file already exists on the server.
        let Some(actions) = obj.actions else {
            return Ok((UploadMode::LfsExists, oid));
        };

        // If the server chose XET transfer, delegate to the XET protocol.
        #[cfg(feature = "xet")]
        if self.core.xet_enabled && chosen_transfer.as_deref() == Some("xet") {
            return Ok((UploadMode::Xet, oid));
        }

        // Decide singlepart vs multipart based on whether the server
        // provided a chunk_size in the upload action headers (matches
        // the huggingface_hub Python client detection logic).
        let chunk_size = actions.upload.header.get("chunk_size").and_then(|v| {
            v.as_u64()
                .map(|n| n as usize)
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        });

        let mode = if let Some(chunk_size) = chunk_size {
            UploadMode::LfsMultipart {
                upload: actions.upload,
                verify: actions.verify,
                chunk_size,
            }
        } else {
            UploadMode::LfsSinglepart {
                upload: actions.upload,
                verify: actions.verify,
            }
        };

        Ok((mode, oid))
    }

    /// Prepare file content for regular HTTP commit (base64 encoded inline).
    fn prepare_commit_file(path: &str, body: &Buffer) -> CommitFile {
        let content = base64::engine::general_purpose::STANDARD.encode(body.to_bytes());
        CommitFile {
            path: path.to_string(),
            content,
            encoding: "base64".to_string(),
        }
    }

    /// Singlepart LFS upload: PUT entire body to the upload URL.
    async fn lfs_upload_singlepart(&self, upload: &LfsBatchAction, body: Buffer) -> Result<()> {
        let req = Request::builder()
            .method(http::Method::PUT)
            .uri(&upload.href)
            .extension(Operation::Write)
            .body(body)
            .map_err(new_request_build_error)?;

        self.core.send(req).await?;
        Ok(())
    }

    /// Multi-part LFS upload: PUT chunks to numbered part URLs, then POST completion.
    async fn lfs_upload_multipart(
        &self,
        upload: &LfsBatchAction,
        oid: &str,
        body: Buffer,
        chunk_size: usize,
    ) -> Result<()> {
        let bytes = body.to_bytes();
        let total_parts = bytes.len().div_ceil(chunk_size);

        // Collect presigned part URLs from the upload header. The server
        // stores them as digit-only keys (e.g. "1", "2", "3"). We collect
        // all such keys, sort by numeric value, and use them in order —
        // matching the huggingface_hub Python client's `_get_sorted_parts_urls`.
        let mut part_urls: Vec<(usize, String)> = upload
            .header
            .iter()
            .filter_map(|(k, v)| {
                let num: usize = k.parse().ok()?;
                let url = v.as_str()?;
                Some((num, url.to_string()))
            })
            .collect();
        part_urls.sort_by_key(|(num, _)| *num);

        let part_urls: Vec<String> = part_urls.into_iter().map(|(_, url)| url).collect();
        if part_urls.len() != total_parts {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!(
                    "expected {} part URLs but server returned {} \
                     (file size: {}, chunk size: {}, header keys: {:?})",
                    total_parts,
                    part_urls.len(),
                    bytes.len(),
                    chunk_size,
                    upload.header.keys().collect::<Vec<_>>(),
                ),
            ));
        }

        let mut etags = Vec::with_capacity(total_parts);

        for (part_num, part_url) in part_urls.iter().enumerate() {
            let start = part_num * chunk_size;
            let end = std::cmp::min(start + chunk_size, bytes.len());
            let chunk = bytes.slice(start..end);

            let req = Request::builder()
                .method(http::Method::PUT)
                .uri(part_url.as_str())
                .extension(Operation::Write)
                .body(Buffer::from(chunk))
                .map_err(new_request_build_error)?;

            let parts = self.core.send(req).await?.into_parts().0;
            let etag = parts
                .headers
                .get(header::ETAG)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string();
            etags.push(etag);
        }

        let parts: Vec<_> = etags
            .into_iter()
            .enumerate()
            .map(|(i, etag)| {
                serde_json::json!({
                    "partNumber": i + 1,
                    "etag": etag,
                })
            })
            .collect();

        let completion = serde_json::json!({
            "oid": oid,
            "parts": parts,
        });
        let completion_body = serde_json::to_vec(&completion).map_err(new_json_serialize_error)?;

        let req = self
            .core
            .request(http::Method::POST, &upload.href, Operation::Write)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Buffer::from(completion_body))
            .map_err(new_request_build_error)?;

        self.core.send(req).await?;
        Ok(())
    }

    /// Verify an LFS upload if the server requested verification.
    async fn lfs_verify(
        &self,
        verify: &Option<LfsBatchAction>,
        oid: &str,
        size: u64,
    ) -> Result<()> {
        let Some(verify) = verify else {
            return Ok(());
        };

        let payload = LfsVerifyRequest {
            oid: oid.to_string(),
            size,
        };
        let body = serde_json::to_vec(&payload).map_err(new_json_serialize_error)?;

        let req = self
            .core
            .request(http::Method::POST, &verify.href, Operation::Write)
            .header(header::CONTENT_TYPE, "application/vnd.git-lfs+json")
            .body(Buffer::from(body))
            .map_err(new_request_build_error)?;

        self.core.send(req).await?;
        Ok(())
    }

    /// Upload file content to XET storage.
    #[cfg(feature = "xet")]
    async fn xet_upload(&self, body: Buffer) -> Result<()> {
        use super::core::map_xet_error;

        let bytes = body.to_bytes();

        let token = self.core.get_xet_token("write").await?;
        let refresher = Arc::new(XetTokenRefresher::new(&self.core, "write"));

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

        results.first().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "No file info returned from XET upload",
            )
        })?;

        Ok(())
    }

    async fn upload_and_commit(&self, body: Buffer) -> Result<CommitResponse> {
        let (mode, oid) = self.determine_upload_mode(&body).await?;
        let size = body.len() as u64;

        match mode {
            UploadMode::Regular => {
                let file = Self::prepare_commit_file(&self.path, &body);
                return self.core.commit_files(vec![file], vec![], vec![]).await;
            }
            UploadMode::LfsExists => {}
            UploadMode::LfsSinglepart { upload, verify } => {
                self.lfs_upload_singlepart(&upload, body).await?;
                self.lfs_verify(&verify, &oid, size).await?;
            }
            UploadMode::LfsMultipart {
                upload,
                verify,
                chunk_size,
            } => {
                self.lfs_upload_multipart(&upload, &oid, body, chunk_size)
                    .await?;
                self.lfs_verify(&verify, &oid, size).await?;
            }
            #[cfg(feature = "xet")]
            UploadMode::Xet => {
                self.xet_upload(body).await?;
            }
        }

        let lfs_file = LfsFile {
            path: self.path.clone(),
            oid,
            algo: "sha256".to_string(),
            size,
        };
        self.core.commit_files(vec![], vec![lfs_file], vec![]).await
    }
}

impl oio::OneShotWrite for HfWriter {
    async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
        let size = bs.len() as u64;
        let resp = self.upload_and_commit(bs).await?;

        let mut meta = Metadata::default().with_content_length(size);
        if let Some(oid) = resp.commit_oid {
            meta = meta.with_version(oid);
        }
        Ok(meta)
    }
}

#[cfg(test)]
mod tests {
    use super::super::backend::test_utils::testing_operator;
    #[cfg(feature = "xet")]
    use super::super::backend::test_utils::testing_xet_operator;
    use super::*;
    use base64::Engine;

    // --- Unit tests (no network required) ---

    #[test]
    fn test_prepare_commit_file() {
        let content = b"Hello, World!";
        let buf = Buffer::from(content.to_vec());
        let file = HfWriter::prepare_commit_file("data/test.txt", &buf);

        assert_eq!(file.path, "data/test.txt");
        assert_eq!(file.encoding, "base64");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&file.content)
            .unwrap();
        assert_eq!(decoded, content);
    }

    #[test]
    fn test_prepare_commit_file_empty() {
        let buf = Buffer::from(Vec::<u8>::new());
        let file = HfWriter::prepare_commit_file("empty.bin", &buf);

        assert_eq!(file.path, "empty.bin");
        assert_eq!(file.encoding, "base64");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&file.content)
            .unwrap();
        assert!(decoded.is_empty());
    }

    // --- Integration tests (require HF_OPENDAL_DATASET and HF_OPENDAL_TOKEN) ---

    #[tokio::test]
    #[ignore]
    async fn test_write_http() {
        let op = testing_operator();
        op.write("test-file.txt", b"Hello, HuggingFace!".as_slice())
            .await
            .expect("write should succeed");
    }

    #[tokio::test]
    #[ignore]
    async fn test_write_http_with_content_type() {
        let op = testing_operator();
        op.write_with("test.json", br#"{"test": "data"}"#.as_slice())
            .content_type("application/json")
            .await
            .expect("write with content type should succeed");
    }

    #[cfg(feature = "xet")]
    #[tokio::test]
    #[ignore]
    async fn test_write_xet() {
        let op = testing_xet_operator();
        op.write("test-xet.bin", b"Binary data for XET test".as_slice())
            .await
            .expect("xet write should succeed");
    }

    /// Write a small text file (should use Regular upload mode — base64 inline
    /// in commit) and verify the content roundtrips correctly.
    #[tokio::test]
    #[ignore]
    async fn test_write_regular_roundtrip() {
        let op = testing_operator();
        let path = "tests/regular-roundtrip.txt";
        let content = b"Small text file for regular upload.";

        op.write(path, content.as_slice())
            .await
            .expect("write should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), content);

        // Cleanup is best-effort; transient 500s from concurrent ops are OK.
        let _ = op.delete(path).await;
    }

    /// Write a 1 MB binary file with XET disabled. The preupload API should
    /// classify this as LFS, and the LFS batch API should choose basic
    /// (singlepart) transfer since the file is below the multipart threshold.
    #[tokio::test]
    #[ignore]
    async fn test_write_lfs_singlepart_roundtrip() {
        let op = testing_operator();
        let path = "tests/lfs-singlepart.bin";
        let content: Vec<u8> = (0..1_048_576u32).map(|i| (i % 256) as u8).collect();

        op.write(path, content.clone())
            .await
            .expect("LFS singlepart write should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), content.as_slice());

        let _ = op.delete(path).await;
    }

    /// Write a large binary file with XET disabled. The server decides
    /// whether to use singlepart or multipart LFS transfer based on size.
    #[tokio::test]
    #[ignore]
    async fn test_write_lfs_large_roundtrip() {
        let op = testing_operator();
        let path = "tests/lfs-large.bin";
        // 12 MB of patterned data — above the ~10 MB multipart threshold.
        let content: Vec<u8> = (0..12_000_000u32).map(|i| (i % 251) as u8).collect();

        op.write(path, content.clone())
            .await
            .expect("LFS large write should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().len(), content.len());
        assert_eq!(data.to_bytes().as_ref(), content.as_slice());

        let _ = op.delete(path).await;
    }

    /// Verify stat returns correct metadata after writing.
    #[tokio::test]
    #[ignore]
    async fn test_write_and_stat() {
        let op = testing_operator();
        let path = "tests/stat-after-write.txt";
        let content = b"Content for stat verification.";

        op.write(path, content.as_slice())
            .await
            .expect("write should succeed");

        let meta = op.stat(path).await.expect("stat should succeed");
        assert_eq!(meta.content_length(), content.len() as u64);

        let _ = op.delete(path).await;
    }

    /// Overwriting an existing file should replace its content.
    #[tokio::test]
    #[ignore]
    async fn test_write_overwrite() {
        let op = testing_operator();
        let path = "tests/overwrite-test.txt";

        op.write(path, b"first version".as_slice())
            .await
            .expect("first write should succeed");

        op.write(path, b"second version".as_slice())
            .await
            .expect("overwrite should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), b"second version");

        let _ = op.delete(path).await;
    }

    /// Full lifecycle: write → stat → read → delete → confirm gone.
    #[tokio::test]
    #[ignore]
    async fn test_write_delete_lifecycle() {
        let op = testing_operator();
        let path = "tests/lifecycle-test.txt";

        op.write(path, b"temporary file".as_slice())
            .await
            .expect("write should succeed");

        assert!(op.stat(path).await.is_ok());

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), b"temporary file");

        op.delete(path).await.expect("delete should succeed");
        assert!(op.stat(path).await.is_err());
    }

    /// Write an empty (0-byte) file and verify roundtrip.
    #[tokio::test]
    #[ignore]
    async fn test_write_empty_file_roundtrip() {
        let op = testing_operator();
        let path = "tests/empty-file.txt";

        op.write(path, Vec::<u8>::new())
            .await
            .expect("write empty file should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert!(data.to_bytes().is_empty());

        let meta = op.stat(path).await.expect("stat should succeed");
        assert_eq!(meta.content_length(), 0);

        let _ = op.delete(path).await;
    }

    /// Write a file in a deeply nested directory structure.
    /// HuggingFace creates intermediate directories implicitly.
    #[tokio::test]
    #[ignore]
    async fn test_write_nested_directory() {
        let op = testing_operator();
        let path = "tests/deep/nested/dir/file.txt";
        let content = b"nested directory test";

        op.write(path, content.as_slice())
            .await
            .expect("write to nested path should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), content);

        let _ = op.delete(path).await;
    }

    /// Write a file with special characters in the path.
    #[tokio::test]
    #[ignore]
    async fn test_write_special_characters_in_path() {
        let op = testing_operator();
        let path = "tests/special chars (1).txt";
        let content = b"special character path test";

        op.write(path, content.as_slice())
            .await
            .expect("write with special chars should succeed");

        let data = op.read(path).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), content);

        let _ = op.delete(path).await;
    }

    /// Upload identical LFS content to two different paths. The second
    /// write should hit the LfsExists code path (LFS batch returns no
    /// actions because the object already exists in storage).
    #[tokio::test]
    #[ignore]
    async fn test_write_lfs_reupload() {
        let op = testing_operator();
        let path1 = "tests/lfs-reupload-1.bin";
        let path2 = "tests/lfs-reupload-2.bin";
        let content: Vec<u8> = (0..1_048_576u32).map(|i| (i % 256) as u8).collect();

        // First upload — should use LFS singlepart.
        op.write(path1, content.clone())
            .await
            .expect("first LFS write should succeed");

        // Second upload of identical content to a different path — should hit LfsExists.
        op.write(path2, content.clone())
            .await
            .expect("LFS re-upload should succeed (LfsExists path)");

        let data = op.read(path2).await.expect("read should succeed");
        assert_eq!(data.to_bytes().as_ref(), content.as_slice());

        let _ = op.delete(path1).await;
        let _ = op.delete(path2).await;
    }

    /// Delete a file and confirm read returns NotFound.
    #[tokio::test]
    #[ignore]
    async fn test_delete_then_read() {
        let op = testing_operator();
        let path = "tests/delete-then-read.txt";

        op.write(path, b"will be deleted".as_slice())
            .await
            .expect("write should succeed");

        op.delete(path).await.expect("delete should succeed");

        let err = op
            .read(path)
            .await
            .expect_err("read after delete should fail");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    /// Write multiple files, delete them all, and verify each is gone.
    #[tokio::test]
    #[ignore]
    async fn test_batch_delete() {
        let op = testing_operator();
        let paths = [
            "tests/batch-del-a.txt",
            "tests/batch-del-b.txt",
            "tests/batch-del-c.txt",
        ];

        for path in &paths {
            op.write(path, b"batch delete test".as_slice())
                .await
                .expect("write should succeed");
        }

        for path in &paths {
            op.delete(path).await.expect("delete should succeed");
        }

        for path in &paths {
            let err = op
                .stat(path)
                .await
                .expect_err("stat should fail after delete");
            assert_eq!(err.kind(), ErrorKind::NotFound);
        }
    }
}
