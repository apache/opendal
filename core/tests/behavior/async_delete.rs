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

use anyhow::Result;
use futures::TryStreamExt;
use opendal::raw::Access;
use opendal::raw::OpDelete;

use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.stat && cap.delete && cap.write {
        tests.extend(async_trials!(
            op,
            test_delete_file,
            test_delete_empty_dir,
            test_delete_with_special_chars,
            test_delete_not_existing,
            test_delete_stream,
            test_remove_one_file,
            test_delete_with_version,
            test_delete_with_not_existing_version,
            test_batch_delete,
            test_batch_delete_with_version
        ));
        if cap.delete_with_recursive {
            tests.extend(async_trials!(op, test_delete_with_recursive_basic));
        }
        if cap.list_with_recursive {
            tests.extend(async_trials!(op, test_remove_all_basic));
            if !cap.create_dir {
                tests.extend(async_trials!(op, test_remove_all_with_prefix_exists));
            }
        }
    }

    if cap.undelete && cap.stat && cap.delete && cap.write {
        tests.extend(async_trials!(op, test_undelete_file));
    }
}

/// Delete existing file should succeed.
pub async fn test_delete_file(op: Operator) -> Result<()> {
    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content).await.expect("write must succeed");

    op.delete(&path).await?;

    // Stat it again to check.
    assert!(!op.exists(&path).await?);

    Ok(())
}

/// Delete empty dir should succeed.
pub async fn test_delete_empty_dir(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = TEST_FIXTURE.new_dir_path();

    op.create_dir(&path).await.expect("create must succeed");

    op.delete(&path).await?;

    Ok(())
}

/// Delete file with special chars should succeed.
pub async fn test_delete_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    let (path, content, _) = TEST_FIXTURE.new_file_with_path(op.clone(), &path);

    op.write(&path, content).await.expect("write must succeed");

    op.delete(&path).await?;

    // Stat it again to check.
    assert!(!op.exists(&path).await?);

    Ok(())
}

/// Delete not existing file should also succeed.
pub async fn test_delete_not_existing(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    op.delete(&path).await?;

    Ok(())
}

/// Remove one file
pub async fn test_remove_one_file(op: Operator) -> Result<()> {
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
pub async fn test_delete_stream(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }
    // Gdrive think that this test is an abuse of their service and redirect us
    // to an infinite loop. Let's ignore this test for gdrive.
    #[cfg(feature = "services-gdrive")]
    if op.info().scheme() == services::GDRIVE_SCHEME {
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

async fn test_blocking_remove_all_with_objects(
    op: Operator,
    parent: String,
    paths: impl IntoIterator<Item = &'static str>,
) -> Result<()> {
    for path in paths {
        let path = format!("{parent}/{path}");
        let (content, _) = gen_bytes(op.info().full_capability());
        op.write(&path, content).await.expect("write must succeed");
    }

    op.delete_with(&parent).recursive(true).await?;

    let found = op
        .lister_with(&format!("{parent}/"))
        .recursive(true)
        .await
        .expect("list must succeed")
        .try_next()
        .await
        .expect("list must succeed")
        .is_some();

    assert!(!found, "all objects should be removed");

    Ok(())
}

/// Remove all under a prefix
pub async fn test_remove_all_basic(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    test_blocking_remove_all_with_objects(op, parent, ["a/b", "a/c", "a/d/e"]).await
}

/// Remove all under a prefix, while the prefix itself is also an object
pub async fn test_remove_all_with_prefix_exists(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());
    op.write(&parent, content)
        .await
        .expect("write must succeed");
    test_blocking_remove_all_with_objects(op, parent, ["a", "a/b", "a/c", "a/b/e"]).await
}

pub async fn test_delete_with_version(op: Operator) -> Result<()> {
    if !op.info().full_capability().delete_with_version {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(path.as_str(), content)
        .await
        .expect("write must success");
    let meta = op.stat(path.as_str()).await.expect("stat must success");
    let version = meta.version().expect("must have version");

    op.delete(path.as_str()).await.expect("delete must success");
    assert!(!op.exists(path.as_str()).await?);

    // After a simple delete, the data can still be accessed using its version.
    let meta = op
        .stat_with(path.as_str())
        .version(version)
        .await
        .expect("stat must success");
    assert_eq!(version, meta.version().expect("must have version"));

    // After deleting with the version, the data is removed permanently
    op.delete_with(path.as_str())
        .version(version)
        .await
        .expect("delete must success");
    let ret = op.stat_with(path.as_str()).version(version).await;
    assert!(ret.is_err());
    assert_eq!(ret.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

pub async fn test_delete_with_not_existing_version(op: Operator) -> Result<()> {
    if !op.info().full_capability().delete_with_version {
        return Ok(());
    }

    // retrieve a valid version
    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(path.as_str(), content)
        .await
        .expect("write must success");
    let version = op
        .stat(path.as_str())
        .await
        .expect("stat must success")
        .version()
        .expect("must have stat")
        .to_string();

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(path.as_str(), content)
        .await
        .expect("write must success");
    let ret = op
        .delete_with(path.as_str())
        .version(version.as_str())
        .await;
    assert!(ret.is_ok());

    Ok(())
}

pub async fn test_delete_with_recursive_basic(op: Operator) -> Result<()> {
    if !op.info().full_capability().delete_with_recursive {
        return Ok(());
    }

    let base = format!("delete_recursive_{}/", uuid::Uuid::new_v4());

    let files = [
        format!("{base}file.txt"),
        format!("{base}dir1/file1.txt"),
        format!("{base}dir1/dir2/file2.txt"),
    ];

    for path in &files {
        op.write(path, "delete recursive").await?;
    }

    op.delete_with(&base).recursive(true).await?;

    let mut l = op.lister_with(&base).recursive(true).await?;
    assert!(
        l.try_next().await?.is_none(),
        "all entries should be removed"
    );

    for path in &files {
        assert!(!op.exists(path).await?, "{path} should be removed");
    }

    Ok(())
}

pub async fn test_batch_delete(op: Operator) -> Result<()> {
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

pub async fn test_batch_delete_with_version(op: Operator) -> Result<()> {
    let mut cap = op.info().full_capability();
    if !cap.delete_with_version {
        return Ok(());
    }
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
        let meta = op.stat(path.as_str()).await.expect("stat must succeed");
        let version = meta.version().expect("must have version");
        let op_args = OpDelete::new().with_version(version);
        files.push((path, op_args));
    }

    op.delete_iter(files.clone())
        .await
        .expect("batch delete must succeed");

    for (path, args) in files {
        let stat = op
            .stat_with(path.as_str())
            .version(args.version().unwrap())
            .await;
        assert!(stat.is_err());
        assert_eq!(stat.unwrap_err().kind(), ErrorKind::NotFound);
    }

    Ok(())
}

/// Undelete a soft-deleted file should succeed.
pub async fn test_undelete_file(op: Operator) -> Result<()> {
    if !op.info().full_capability().undelete {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    // Create a file
    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    // Verify file exists
    assert!(op.exists(&path).await?, "file should exist after creation");

    // Delete the file
    op.delete(&path).await?;

    // Verify file is deleted
    assert!(
        !op.exists(&path).await?,
        "file should not exist after deletion"
    );

    // Undelete the file
    op.undelete(&path).await?;

    // Verify file is restored
    assert!(op.exists(&path).await?, "file should exist after undelete");

    // Verify content is intact
    let restored_content = op.read(&path).await?;
    assert_eq!(
        restored_content.to_vec(),
        content,
        "restored file should have the same content"
    );

    Ok(())
}
