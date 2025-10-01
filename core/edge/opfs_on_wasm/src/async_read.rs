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

#[cfg(test)]
mod tests {

    use futures::AsyncReadExt;
    use futures::TryStreamExt;
    use opendal::*;
    use sha2::Digest;
    use sha2::Sha256;
    use wasm_bindgen_test::console_log;
    use wasm_bindgen_test::wasm_bindgen_test;
    use wasm_bindgen_test::wasm_bindgen_test_configure;

    use crate::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// Read full content should match.
    #[wasm_bindgen_test]
    pub async fn test_read_full() -> anyhow::Result<()> {
        let op = operator();
        let (path, content, size) = TEST_FIXTURE.new_file(op.clone());

        op.write(&path, content.clone())
            .await
            .expect("write must succeed");

        let bs = op.read(&path).await?.to_bytes();
        assert_eq!(size, bs.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content)),
            "read content"
        );

        Ok(())
    }

    /// Read range content should match.
    #[wasm_bindgen_test]
    pub async fn test_read_range() -> anyhow::Result<()> {
        let op = operator();
        let (path, content, size) = TEST_FIXTURE.new_file(op.clone());
        let (offset, length) = gen_offset_length(size);

        op.write(&path, content.clone())
            .await
            .expect("write must succeed");

        let bs = op
            .read_with(&path)
            .range(offset..offset + length)
            .await?
            .to_bytes();
        assert_eq!(bs.len() as u64, length, "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!(
                "{:x}",
                Sha256::digest(&content[offset as usize..(offset + length) as usize])
            ),
            "read content"
        );

        Ok(())
    }

    /// Read full content should match.
    #[wasm_bindgen_test]
    pub async fn test_reader() -> anyhow::Result<()> {
        let op = operator();
        let (path, content, size) = TEST_FIXTURE.new_file(op.clone());

        op.write(&path, content.clone())
            .await
            .expect("write must succeed");

        // Reader.
        let bs = op.reader(&path).await?.read(..).await?.to_bytes();
        assert_eq!(size, bs.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content)),
            "read content"
        );

        // Bytes Stream
        let bs = op
            .reader(&path)
            .await?
            .into_bytes_stream(..)
            .await?
            .try_fold(Vec::new(), |mut acc, chunk| {
                acc.extend_from_slice(&chunk);
                async { Ok(acc) }
            })
            .await?;
        assert_eq!(size, bs.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content)),
            "read content"
        );

        // Futures Reader
        let mut futures_reader = op
            .reader(&path)
            .await?
            .into_futures_async_read(0..size as u64)
            .await?;
        let mut bs = Vec::new();
        futures_reader.read_to_end(&mut bs).await?;
        assert_eq!(size, bs.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content)),
            "read content"
        );

        Ok(())
    }

    /// Read not exist file should return NotFound
    #[wasm_bindgen_test]
    pub async fn test_read_not_exist() -> anyhow::Result<()> {
        let op = operator();
        let path = uuid::Uuid::new_v4().to_string();

        let bs = op.read(&path).await;
        assert!(bs.is_err());
        assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

        Ok(())
    }

    /// Read with dir path should return an error.
    #[wasm_bindgen_test]
    pub async fn test_read_with_dir_path() -> anyhow::Result<()> {
        let op = operator();
        if !op.info().full_capability().create_dir {
            return Ok(());
        }

        let path = TEST_FIXTURE.new_dir_path();

        op.create_dir(&path).await.expect("write must succeed");

        let result = op.read(&path).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

        Ok(())
    }

    /// Read file with special chars should succeed.
    #[wasm_bindgen_test]
    pub async fn test_read_with_special_chars() -> anyhow::Result<()> {
        let op = operator();
        // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
        if op.info().scheme() == opendal::Scheme::Atomicserver {
            console_log!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
            return Ok(());
        }

        let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
        let (path, content, size) = TEST_FIXTURE.new_file_with_path(op.clone(), &path);

        op.write(&path, content.clone())
            .await
            .expect("write must succeed");

        let bs = op.read(&path).await?.to_bytes();
        assert_eq!(size, bs.len(), "read size");
        assert_eq!(
            format!("{:x}", Sha256::digest(&bs)),
            format!("{:x}", Sha256::digest(&content)),
            "read content"
        );

        Ok(())
    }
}
