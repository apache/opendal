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

    use std::collections::HashMap;

    use anyhow::Result;
    use futures::TryStreamExt;
    use opendal::*;
    use wasm_bindgen_test::console_log;
    use wasm_bindgen_test::wasm_bindgen_test;
    use wasm_bindgen_test::wasm_bindgen_test_configure;

    use crate::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// Check should be OK.
    #[wasm_bindgen_test]
    pub async fn test_check() -> Result<()> {
        let op = operator();
        op.check().await.expect("operator check is ok");

        Ok(())
    }

    /// List dir should return newly created file.
    #[wasm_bindgen_test]
    pub async fn test_list_dir() -> Result<()> {
        let op = operator();
        let parent = uuid::Uuid::new_v4().to_string();
        let path = format!("{parent}/{}", uuid::Uuid::new_v4());
        console_log!("Generate a random file: {}", &path);
        let (content, size) = gen_bytes(op.info().full_capability());

        op.write(&path, content).await.expect("write must succeed");

        let mut obs = op.lister(&format!("{parent}/")).await?;
        let mut found = false;
        while let Some(de) = obs.try_next().await? {
            let meta = op.stat(de.path()).await?;
            if de.path() == path {
                assert_eq!(meta.mode(), EntryMode::FILE);

                assert_eq!(meta.content_length(), size as u64);

                found = true
            }
        }
        assert!(found, "file should be found in list");

        op.delete(&path).await.expect("delete must succeed");
        Ok(())
    }

    /// List prefix should return newly created file.
    #[wasm_bindgen_test]
    pub async fn test_list_prefix() -> Result<()> {
        let op = operator();
        let path = uuid::Uuid::new_v4().to_string();
        console_log!("Generate a random file: {}", &path);
        let (content, _) = gen_bytes(op.info().full_capability());

        op.write(&path, content).await.expect("write must succeed");

        let obs = op.list(&path).await?;
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].path(), path);
        assert_eq!(obs[0].metadata().mode(), EntryMode::FILE);

        op.delete(&path).await.expect("delete must succeed");
        Ok(())
    }

    /// listing a directory, which contains more objects than a single page can take.
    #[wasm_bindgen_test]
    pub async fn test_list_rich_dir() -> Result<()> {
        let op = operator();
        // Gdrive think that this test is an abuse of their service and redirect us
        // to an infinite loop. Let's ignore this test for gdrive.
        if op.info().scheme() == Scheme::Gdrive {
            return Ok(());
        }

        let parent = "test_list_rich_dir/";
        op.create_dir(parent).await?;

        let mut expected: Vec<String> = (0..=10).map(|num| format!("{parent}file-{num}")).collect();
        for path in expected.iter() {
            op.write(path, "test_list_rich_dir").await?;
        }
        expected.push(parent.to_string());

        let mut objects = op.lister_with(parent).limit(5).await?;
        let mut actual = vec![];
        while let Some(o) = objects.try_next().await? {
            let path = o.path().to_string();
            actual.push(path)
        }
        expected.sort_unstable();
        actual.sort_unstable();

        assert_eq!(actual, expected);

        op.remove_all(parent).await?;
        Ok(())
    }

    /// List empty dir should return itself.
    #[wasm_bindgen_test]
    pub async fn test_list_empty_dir() -> Result<()> {
        let op = operator();
        let dir = format!("{}/", uuid::Uuid::new_v4());

        op.create_dir(&dir).await.expect("write must succeed");

        // List "dir/" should return "dir/".
        let mut obs = op.lister(&dir).await?;
        let mut objects = HashMap::new();
        while let Some(de) = obs.try_next().await? {
            objects.insert(de.path().to_string(), de);
        }
        assert_eq!(
            objects.len(),
            1,
            "only return the dir itself, but found: {objects:?}"
        );
        assert_eq!(
            objects[&dir].metadata().mode(),
            EntryMode::DIR,
            "given dir should exist and must be dir, but found: {objects:?}"
        );

        // List "dir" should return "dir/".
        let mut obs = op.lister(dir.trim_end_matches('/')).await?;
        let mut objects = HashMap::new();
        while let Some(de) = obs.try_next().await? {
            objects.insert(de.path().to_string(), de);
        }
        assert_eq!(
            objects.len(),
            1,
            "only return the dir itself, but found: {objects:?}"
        );
        assert_eq!(
            objects[&dir].metadata().mode(),
            EntryMode::DIR,
            "given dir should exist and must be dir, but found: {objects:?}"
        );

        // List "dir/" recursively should return "dir/".
        let mut obs = op.lister_with(&dir).recursive(true).await?;
        let mut objects = HashMap::new();
        while let Some(de) = obs.try_next().await? {
            objects.insert(de.path().to_string(), de);
        }
        assert_eq!(
            objects.len(),
            1,
            "only return the dir itself, but found: {objects:?}"
        );
        assert_eq!(
            objects[&dir].metadata().mode(),
            EntryMode::DIR,
            "given dir should exist and must be dir, but found: {objects:?}"
        );

        // List "dir" recursively should return "dir/".
        let mut obs = op
            .lister_with(dir.trim_end_matches('/'))
            .recursive(true)
            .await?;
        let mut objects = HashMap::new();
        while let Some(de) = obs.try_next().await? {
            objects.insert(de.path().to_string(), de);
        }
        assert_eq!(objects.len(), 1, "only return the dir itself");
        assert_eq!(
            objects[&dir].metadata().mode(),
            EntryMode::DIR,
            "given dir should exist and must be dir"
        );

        op.delete(&dir).await.expect("delete must succeed");
        Ok(())
    }

    /// List non exist dir should return nothing.
    #[wasm_bindgen_test]
    pub async fn test_list_non_exist_dir() -> Result<()> {
        let op = operator();
        let dir = format!("{}/", uuid::Uuid::new_v4());

        let mut obs = op.lister(&dir).await?;
        let mut objects = HashMap::new();
        while let Some(de) = obs.try_next().await? {
            objects.insert(de.path().to_string(), de);
        }
        console_log!("got objects: {objects:?}");

        assert_eq!(objects.len(), 0, "dir should only return empty");
        Ok(())
    }

    /// List dir should return correct sub dir.
    #[wasm_bindgen_test]
    pub async fn test_list_sub_dir() -> Result<()> {
        let op = operator();
        let path = format!("{}/", uuid::Uuid::new_v4());

        op.create_dir(&path).await.expect("create must succeed");

        let mut obs = op.lister("/").await?;
        let mut found = false;
        let mut entries = vec![];
        while let Some(de) = obs.try_next().await? {
            if de.path() == path {
                let meta = op.stat(&path).await?;
                assert_eq!(meta.mode(), EntryMode::DIR);
                assert_eq!(de.name(), path);

                found = true
            }
            entries.push(de)
        }
        assert!(
            found,
            "dir should be found in list, but only got: {entries:?}"
        );

        op.delete(&path).await.expect("delete must succeed");
        Ok(())
    }

    /// List dir should also to list nested dir.
    #[wasm_bindgen_test]
    pub async fn test_list_nested_dir() -> Result<()> {
        let op = operator();
        let parent = format!("{}/", uuid::Uuid::new_v4());
        op.create_dir(&parent)
            .await
            .expect("create dir must succeed");

        let dir = format!("{parent}{}/", uuid::Uuid::new_v4());
        op.create_dir(&dir).await.expect("create must succeed");

        let file_name = uuid::Uuid::new_v4().to_string();
        let file_path = format!("{dir}{file_name}");
        op.write(&file_path, "test_list_nested_dir")
            .await
            .expect("create must succeed");

        let dir_name = format!("{}/", uuid::Uuid::new_v4());
        let dir_path = format!("{dir}{dir_name}");
        op.create_dir(&dir_path).await.expect("create must succeed");

        let obs = op.list(&parent).await?;
        assert_eq!(obs.len(), 2, "parent should got 2 entry");
        let objects: HashMap<&str, &Entry> = obs.iter().map(|e| (e.path(), e)).collect();
        assert_eq!(
            objects
                .get(parent.as_str())
                .expect("parent should be found in list")
                .metadata()
                .mode(),
            EntryMode::DIR
        );
        assert_eq!(
            objects
                .get(dir.as_str())
                .expect("dir should be found in list")
                .metadata()
                .mode(),
            EntryMode::DIR
        );

        let mut obs = op.lister(&dir).await?;
        let mut objects = HashMap::new();

        while let Some(de) = obs.try_next().await? {
            objects.insert(de.path().to_string(), de);
        }
        console_log!("got objects: {objects:?}");

        assert_eq!(objects.len(), 3, "dir should only got 3 objects");

        // Check file
        let meta = op
            .stat(
                objects
                    .get(&file_path)
                    .expect("file should be found in list")
                    .path(),
            )
            .await?;
        assert_eq!(meta.mode(), EntryMode::FILE);
        assert_eq!(meta.content_length(), 20);

        // Check dir
        let meta = op
            .stat(
                objects
                    .get(&dir_path)
                    .expect("file should be found in list")
                    .path(),
            )
            .await?;
        assert_eq!(meta.mode(), EntryMode::DIR);

        op.delete(&file_path).await.expect("delete must succeed");
        op.delete(&dir_path).await.expect("delete must succeed");
        op.delete(&dir).await.expect("delete must succeed");
        Ok(())
    }

    /// List with path file should auto add / suffix.
    #[wasm_bindgen_test]
    pub async fn test_list_dir_with_file_path() -> Result<()> {
        let op = operator();
        let parent = uuid::Uuid::new_v4().to_string();
        let file = format!("{parent}/{}", uuid::Uuid::new_v4());

        let (content, _) = gen_bytes(op.info().full_capability());
        op.write(&file, content).await?;

        let obs = op.list(&parent).await?;
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].path(), format!("{parent}/"));
        assert_eq!(obs[0].metadata().mode(), EntryMode::DIR);

        op.delete(&file).await?;

        Ok(())
    }
}
