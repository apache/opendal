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

    use anyhow::Result;
    use opendal::raw::Access;
    use opendal::*;
    use wasm_bindgen_test::wasm_bindgen_test;
    use wasm_bindgen_test::wasm_bindgen_test_configure;

    use crate::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// Delete existing file should succeed.
    #[wasm_bindgen_test]
    pub async fn test_delete_file() -> Result<()> {
        let op = operator();
        let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

        op.write(&path, content).await.expect("write must succeed");

        op.delete(&path).await?;

        // Stat it again to check.
        assert!(!op.exists(&path).await?);

        Ok(())
    }

    /// Delete empty dir should succeed.
    #[wasm_bindgen_test]
    pub async fn test_delete_empty_dir() -> Result<()> {
        let op = operator();
        if !op.info().full_capability().create_dir {
            return Ok(());
        }

        let path = TEST_FIXTURE.new_dir_path();

        op.create_dir(&path).await.expect("create must succeed");

        op.delete(&path).await?;

        Ok(())
    }

    /// Delete file with special chars should succeed.
    #[wasm_bindgen_test]
    pub async fn test_delete_with_special_chars() -> Result<()> {
        let op = operator();
        // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
        if op.info().scheme() == opendal::Scheme::Atomicserver {
            return Ok(());
        }

        let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
        let (path, content, _) = TEST_FIXTURE.new_file_with_path(op.clone(), &path);

        op.write(&path, content).await.expect("write must succeed");

        op.delete(&path).await?;

        // Stat it again to check.
        assert!(!op.exists(&path).await?);

        Ok(())
    }

    /// Delete not existing file should also succeed.
    #[wasm_bindgen_test]
    pub async fn test_delete_not_existing() -> Result<()> {
        let op = operator();
        let path = uuid::Uuid::new_v4().to_string();

        op.delete(&path).await?;

        Ok(())
    }

    /// Remove one file
    #[wasm_bindgen_test]
    pub async fn test_remove_one_file() -> Result<()> {
        let op = operator();
        let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

        op.write(&path, content.clone())
            .await
            .expect("write must succeed");

        op.delete_iter(vec![path.clone()]).await?;

        // Stat it again to check.
        assert!(!op.exists(&path).await?);

        op.write(&format!("/{path}"), content)
            .await
            .expect("write must succeed");

        op.delete_iter(vec![path.clone()]).await?;

        // Stat it again to check.
        assert!(!op.exists(&path).await?);

        Ok(())
    }

    /// Delete via stream.
    #[wasm_bindgen_test]
    pub async fn test_delete_stream() -> Result<()> {
        let op = operator();
        if !op.info().full_capability().create_dir {
            return Ok(());
        }
        // Gdrive think that this test is an abuse of their service and redirect us
        // to an infinite loop. Let's ignore this test for gdrive.
        if op.info().scheme() == Scheme::Gdrive {
            return Ok(());
        }

        let dir = uuid::Uuid::new_v4().to_string();
        op.create_dir(&format!("{dir}/"))
            .await
            .expect("create must succeed");

        let expected: Vec<_> = (0..100).collect();
        for path in expected.iter() {
            op.write(&format!("{dir}/{path}"), "delete_stream").await?;
        }

        let mut deleter = op.deleter().await?;
        deleter
            .delete_iter(expected.iter().map(|v| format!("{dir}/{v}")))
            .await?;
        deleter.close().await?;

        // Stat it again to check.
        for path in expected.iter() {
            assert!(
                !op.exists(&format!("{dir}/{path}")).await?,
                "{path} should be removed"
            )
        }

        Ok(())
    }

    #[wasm_bindgen_test]
    pub async fn test_batch_delete() -> Result<()> {
        let op = operator();
        let mut cap = op.info().full_capability();
        if cap.delete_max_size.unwrap_or(1) <= 1 {
            return Ok(());
        }

        cap.delete_max_size = Some(2);
        op.inner().info().update_full_capability(|_| cap);

        let mut files = Vec::new();
        for _ in 0..5 {
            let (path, content, _) = TEST_FIXTURE.new_file(op.clone());
            op.write(path.as_str(), content)
                .await
                .expect("write must succeed");
            files.push(path);
        }

        op.delete_iter(files.clone())
            .await
            .expect("batch delete must succeed");

        for path in files {
            let stat = op.stat(path.as_str()).await;
            assert!(stat.is_err());
            assert_eq!(stat.unwrap_err().kind(), ErrorKind::NotFound);
        }

        Ok(())
    }
}
