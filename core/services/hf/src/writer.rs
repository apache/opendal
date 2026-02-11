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
#[cfg(feature = "xet")]
use std::sync::Mutex;

use base64::Engine;

use super::core::{CommitFile, HfCore};
#[cfg(feature = "xet")]
use super::core::{LfsFile, map_xet_error};
use opendal_core::raw::*;
use opendal_core::*;
#[cfg(feature = "xet")]
use xet_data::streaming::XetWriter;

/// Writer that handles both regular (small) and XET (large) file uploads.
pub enum HfWriter {
    /// Regular writer for small files using base64 inline commit.
    Regular {
        core: Arc<HfCore>,
        path: String,
        size: u64,
        buf: Vec<Buffer>,
    },
    /// XET writer for large files using streaming protocol.
    #[cfg(feature = "xet")]
    Xet {
        core: Arc<HfCore>,
        path: String,
        size: u64,
        writer: Mutex<XetWriter>,
    },
}

impl HfWriter {
    /// Create a new writer by determining the upload mode from the API.
    pub async fn try_new(core: Arc<HfCore>, path: String) -> Result<Self> {
        let mode_str = core.determine_upload_mode(&path).await?;

        if mode_str == "lfs" {
            #[cfg(feature = "xet")]
            if core.xet_enabled {
                let client = core.xet_client("write").await?;
                let writer = client
                    .write(None, None, None)
                    .await
                    .map_err(map_xet_error)?;
                return Ok(HfWriter::Xet {
                    core,
                    path,
                    size: 0,
                    writer: Mutex::new(writer),
                });
            }
            return Err(Error::new(
                ErrorKind::Unsupported,
                "file requires LFS; enable the xet feature for large file support",
            ));
        }

        Ok(HfWriter::Regular {
            core,
            path,
            size: 0,
            buf: Vec::new(),
        })
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
            HfWriter::Regular { size, buf, .. } => {
                *size += bs.len() as u64;
                buf.push(bs);
                Ok(())
            }
            #[cfg(feature = "xet")]
            HfWriter::Xet { size, writer, .. } => {
                *size += bs.len() as u64;
                writer
                    .get_mut()
                    .unwrap()
                    .write(bs.to_bytes())
                    .await
                    .map_err(map_xet_error)
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match self {
            HfWriter::Regular {
                core,
                path,
                size,
                buf,
                ..
            } => {
                let content_length = *size;

                // Flatten buffer
                let mut data = Vec::new();
                for buf in std::mem::take(buf) {
                    data.extend_from_slice(&buf.to_bytes());
                }

                let file = Self::prepare_commit_file(path, &data);
                let resp = core.commit_files(vec![file], vec![], vec![]).await?;

                let mut meta = Metadata::default().with_content_length(content_length);
                if let Some(commit_oid) = resp.commit_oid {
                    meta = meta.with_version(commit_oid);
                }
                Ok(meta)
            }
            #[cfg(feature = "xet")]
            HfWriter::Xet {
                core,
                path,
                size,
                writer,
            } => {
                let content_length = *size;
                let file_info = writer
                    .get_mut()
                    .unwrap()
                    .close()
                    .await
                    .map_err(map_xet_error)?;
                let sha256 = file_info.sha256().ok_or_else(|| {
                    Error::new(
                        ErrorKind::Unexpected,
                        "xet upload did not return sha256 hash",
                    )
                })?;
                let lfs_file = LfsFile {
                    path: path.clone(),
                    oid: sha256.to_string(),
                    algo: "sha256".to_string(),
                    size: content_length,
                };
                let resp = core.commit_files(vec![], vec![lfs_file], vec![]).await?;

                let mut meta = Metadata::default().with_content_length(content_length);
                if let Some(commit_oid) = resp.commit_oid {
                    meta = meta.with_version(commit_oid);
                }
                Ok(meta)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match self {
            HfWriter::Regular { buf, .. } => {
                buf.clear();
            }
            #[cfg(feature = "xet")]
            HfWriter::Xet { writer, .. } => {
                let _ = writer.get_mut().unwrap().abort().await;
            }
        }
        Ok(())
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
}
