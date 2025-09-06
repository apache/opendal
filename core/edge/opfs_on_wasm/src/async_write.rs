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

mod tests {

    use anyhow::Result;
    use bytes::Bytes;
    use futures::io::BufReader;
    use futures::io::Cursor;
    use futures::stream;
    use futures::AsyncWriteExt;
    use futures::SinkExt;
    use futures::StreamExt;
    use opendal::*;
    use sha2::Digest;
    use sha2::Sha256;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    use crate::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// Write a single file and test with stat.
    #[wasm_bindgen_test]
    pub async fn test_write_only() -> Result<()> {
        let op = operator();
        let (path, content, size) = TEST_FIXTURE.new_file(op.clone());

        op.write(&path, content).await?;

        let meta = op.stat(&path).await.expect("stat must succeed");
        assert_eq!(meta.content_length(), size as u64);

        Ok(())
    }

    /// Write a file with empty content.
    #[wasm_bindgen_test]
    pub async fn test_write_with_empty_content() -> Result<()> {
        let op = operator();
        if !op.info().full_capability().write_can_empty {
            return Ok(());
        }

        let path = TEST_FIXTURE.new_file_path();

        let bs: Vec<u8> = vec![];
        op.write(&path, bs).await?;

        let meta = op.stat(&path).await.expect("stat must succeed");
        assert_eq!(meta.content_length(), 0);
        Ok(())
    }

    /// Write file with dir path should return an error
    #[wasm_bindgen_test]
    pub async fn test_write_with_dir_path() -> Result<()> {
        let op = operator();
        let path = TEST_FIXTURE.new_dir_path();

        let result = op.write(&path, vec![1]).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

        Ok(())
    }

    /// Write a single file with special chars should succeed.
    #[wasm_bindgen_test]
    pub async fn test_write_with_special_chars() -> Result<()> {
        let op = operator();
        // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
        if op.info().scheme() == opendal::Scheme::Atomicserver {
            return Ok(());
        }
        // Ignore test for vercel blob https://github.com/apache/opendal/pull/4103.
        if op.info().scheme() == opendal::Scheme::VercelBlob {
            return Ok(());
        }

        let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
        let (path, content, size) = TEST_FIXTURE.new_file_with_path(op.clone(), &path);

        op.write(&path, content).await?;

        let meta = op.stat(&path).await.expect("stat must succeed");
        assert_eq!(meta.content_length(), size as u64);

        Ok(())
    }

    #[wasm_bindgen_test]
    pub async fn test_write_returns_metadata() -> Result<()> {
        let op = operator();
        let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

        let meta = op.write(&path, content).await?;
        let stat_meta = op.stat(&path).await?;

        assert_metadata(stat_meta, meta);

        Ok(())
    }

    /// Delete existing file should succeed.
    #[wasm_bindgen_test]
    pub async fn test_writer_abort() -> Result<()> {
        let op = operator();
        let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

        let mut writer = match op.writer(&path).await {
            Ok(writer) => writer,
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::Unsupported);
                return Ok(());
            }
        };

        if let Err(e) = writer.write(content).await {
            assert_eq!(e.kind(), ErrorKind::Unsupported);
            return Ok(());
        }

        if let Err(e) = writer.abort().await {
            assert_eq!(e.kind(), ErrorKind::Unsupported);
            return Ok(());
        }

        // Aborted writer should not write actual file.
        assert!(!op.exists(&path).await?);
        Ok(())
    }

    /// Append data into writer
    #[wasm_bindgen_test]
    pub async fn test_writer_write() -> Result<()> {
        let op = operator();
        if !(op.info().full_capability().write_can_multi) {
            return Ok(());
        }

        let path = TEST_FIXTURE.new_file_path();
        let size = 5 * 1024 * 1024; // write file with 5 MiB
        let content_a = gen_fixed_bytes(size);
        let content_b = gen_fixed_bytes(size);

        let mut w = op.writer(&path).await?;
        w.write(content_a.clone()).await?;
        w.write(content_b.clone()).await?;
        w.close().await?;

        let meta = op.stat(&path).await.expect("stat must succeed");
        assert_eq!(meta.content_length(), (size * 2) as u64);

        let bs = op.read(&path).await?.to_bytes();
        assert_eq!(bs.len(), size * 2, "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[..size])),
            format!("{:x}", Sha256::digest(content_a)),
            "read content a"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[size..])),
            format!("{:x}", Sha256::digest(content_b)),
            "read content b"
        );

        Ok(())
    }

    /// Streaming data into writer
    #[wasm_bindgen_test]
    pub async fn test_writer_sink() -> Result<()> {
        let op = operator();
        let cap = op.info().full_capability();
        if !(cap.write && cap.write_can_multi) {
            return Ok(());
        }

        let path = TEST_FIXTURE.new_file_path();
        let size = 5 * 1024 * 1024; // write file with 5 MiB
        let content_a = gen_fixed_bytes(size);
        let content_b = gen_fixed_bytes(size);
        let mut stream = stream::iter(vec![
            Bytes::from(content_a.clone()),
            Bytes::from(content_b.clone()),
        ])
        .map(Ok);

        let mut w = op
            .writer_with(&path)
            .chunk(4 * 1024 * 1024)
            .await?
            .into_bytes_sink();
        w.send_all(&mut stream).await?;
        w.close().await?;

        let meta = op.stat(&path).await.expect("stat must succeed");
        assert_eq!(meta.content_length(), (size * 2) as u64);

        let bs = op.read(&path).await?.to_bytes();
        assert_eq!(bs.len(), size * 2, "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[..size])),
            format!("{:x}", Sha256::digest(content_a)),
            "read content a"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[size..])),
            format!("{:x}", Sha256::digest(content_b)),
            "read content b"
        );

        Ok(())
    }

    /// Copy data from reader to writer
    #[wasm_bindgen_test]
    pub async fn test_writer_futures_copy() -> Result<()> {
        let op = operator();
        if !(op.info().full_capability().write_can_multi) {
            return Ok(());
        }

        let path = TEST_FIXTURE.new_file_path();
        let (content, size): (Vec<u8>, usize) =
            gen_bytes_with_range(10 * 1024 * 1024..20 * 1024 * 1024);

        let mut w = op
            .writer_with(&path)
            .chunk(8 * 1024 * 1024)
            .await?
            .into_futures_async_write();

        // Wrap a buf reader here to make sure content is read in 1MiB chunks.
        let mut cursor = BufReader::with_capacity(1024 * 1024, Cursor::new(content.clone()));
        futures::io::copy_buf(&mut cursor, &mut w).await?;
        w.close().await?;

        let meta = op.stat(&path).await.expect("stat must succeed");
        assert_eq!(meta.content_length(), size as u64);

        let bs = op.read(&path).await?.to_bytes();
        assert_eq!(bs.len(), size, "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[..size])),
            format!("{:x}", Sha256::digest(content)),
            "read content"
        );

        Ok(())
    }

    #[wasm_bindgen_test]
    pub async fn test_writer_return_metadata() -> Result<()> {
        let op = operator();
        let cap = op.info().full_capability();
        if !cap.write_can_multi {
            return Ok(());
        }

        let path = TEST_FIXTURE.new_file_path();
        let size = 5 * 1024 * 1024; // write file with 5 MiB
        let content_a = gen_fixed_bytes(size);
        let content_b = gen_fixed_bytes(size);

        let mut w = op.writer(&path).await?;
        w.write(content_a.clone()).await?;
        w.write(content_b.clone()).await?;
        let meta = w.close().await?;

        let stat_meta = op.stat(&path).await.expect("stat must succeed");

        assert_metadata(stat_meta, meta);

        Ok(())
    }

    /// Test append to a file must success.
    #[wasm_bindgen_test]
    pub async fn test_write_with_append() -> Result<()> {
        let op = operator();
        let path = TEST_FIXTURE.new_file_path();
        let (content_one, size_one) = gen_bytes(op.info().full_capability());
        let (content_two, size_two) = gen_bytes(op.info().full_capability());

        op.write_with(&path, content_one.clone())
            .append(true)
            .await
            .expect("append file first time must success");

        let meta = op.stat(&path).await?;
        assert_eq!(meta.content_length(), size_one as u64);

        op.write_with(&path, content_two.clone())
            .append(true)
            .await
            .expect("append to an existing file must success");

        let bs = op
            .read(&path)
            .await
            .expect("read file must success")
            .to_bytes();

        assert_eq!(bs.len(), size_one + size_two);
        assert_eq!(bs[..size_one], content_one);
        assert_eq!(bs[size_one..], content_two);

        Ok(())
    }

    #[wasm_bindgen_test]
    pub async fn test_write_with_append_returns_metadata() -> Result<()> {
        let op = operator();
        let cap = op.info().full_capability();

        let path = TEST_FIXTURE.new_file_path();
        let (content_one, _) = gen_bytes(cap);
        let (content_two, _) = gen_bytes(cap);

        op.write_with(&path, content_one.clone())
            .append(true)
            .await
            .expect("append file first time must success");

        let meta = op
            .write_with(&path, content_two.clone())
            .append(true)
            .await
            .expect("append to an existing file must success");

        let stat_meta = op.stat(&path).await.expect("stat must succeed");
        assert_metadata(stat_meta, meta);

        Ok(())
    }

    fn assert_metadata(stat_meta: Metadata, meta: Metadata) {
        assert_eq!(stat_meta.content_length(), meta.content_length());
        if meta.etag().is_some() {
            assert_eq!(stat_meta.etag(), meta.etag());
        }
        if meta.last_modified().is_some() {
            assert_eq!(stat_meta.last_modified(), meta.last_modified());
        }
        if meta.version().is_some() {
            assert_eq!(stat_meta.version(), meta.version());
        }
        if meta.content_md5().is_some() {
            assert_eq!(stat_meta.content_md5(), meta.content_md5());
        }
        if meta.content_type().is_some() {
            assert_eq!(stat_meta.content_type(), meta.content_type());
        }
        if meta.content_encoding().is_some() {
            assert_eq!(stat_meta.content_encoding(), meta.content_encoding());
        }
        if meta.content_disposition().is_some() {
            assert_eq!(stat_meta.content_disposition(), meta.content_disposition());
        }
    }

    /// Copy data from reader to writer
    #[wasm_bindgen_test]
    pub async fn test_writer_with_append() -> Result<()> {
        let op = operator();
        let path = uuid::Uuid::new_v4().to_string();
        let (content, size): (Vec<u8>, usize) =
            gen_bytes_with_range(10 * 1024 * 1024..20 * 1024 * 1024);

        let mut a = op
            .writer_with(&path)
            .append(true)
            .await?
            .into_futures_async_write();

        // Wrap a buf reader here to make sure content is read in 1MiB chunks.
        let mut cursor = BufReader::with_capacity(1024 * 1024, Cursor::new(content.clone()));
        futures::io::copy_buf(&mut cursor, &mut a).await?;
        a.close().await?;

        let meta = op.stat(&path).await.expect("stat must succeed");
        assert_eq!(meta.content_length(), size as u64);

        let bs = op.read(&path).await?.to_bytes();
        assert_eq!(bs.len(), size, "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs[..size])),
            format!("{:x}", Sha256::digest(content)),
            "read content"
        );

        op.delete(&path).await.expect("delete must succeed");
        Ok(())
    }

    #[wasm_bindgen_test]
    pub async fn test_writer_write_with_overwrite() -> Result<()> {
        let op = operator();
        // ghac does not support overwrite
        if op.info().scheme() == Scheme::Ghac {
            return Ok(());
        }

        let path = uuid::Uuid::new_v4().to_string();
        let (content_one, _) = gen_bytes(op.info().full_capability());
        let (content_two, _) = gen_bytes(op.info().full_capability());

        op.write(&path, content_one.clone()).await?;
        let bs = op.read(&path).await?.to_bytes();
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content_one)),
            "read content_one"
        );
        op.write(&path, content_two.clone())
            .await
            .expect("write overwrite must succeed");
        let bs = op.read(&path).await?.to_bytes();
        assert_ne!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content_one)),
            "content_one must be overwrote"
        );
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content_two)),
            "read content_two"
        );

        op.delete(&path).await.expect("delete must succeed");
        Ok(())
    }
}
