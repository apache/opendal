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
use futures::StreamExt;
use futures::TryStreamExt;
use log::warn;

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
            test_delete_with_not_existing_version
        ));
        if cap.list_with_recursive {
            tests.extend(async_trials!(op, test_remove_all_basic));
            if !cap.create_dir {
                tests.extend(async_trials!(op, test_remove_all_with_prefix_exists));
            }
        }
    }
}

/// Delete existing file should succeed.
pub async fn test_delete_file(op: Operator) -> Result<()> {
    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content).await.expect("write must succeed");

    op.delete(&path).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

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
    // Ignore test for supabase until https://github.com/apache/opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/opendal/issues/2194 is resolved");
        return Ok(());
    }
    // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
    if op.info().scheme() == opendal::Scheme::Atomicserver {
        warn!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
        return Ok(());
    }

    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    let (path, content, _) = TEST_FIXTURE.new_file_with_path(op.clone(), &path);

    op.write(&path, content).await.expect("write must succeed");

    op.delete(&path).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

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

    op.remove(vec![path.clone()]).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

    op.write(&format!("/{path}"), content)
        .await
        .expect("write must succeed");

    op.remove(vec![path.clone()]).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

    Ok(())
}

/// Delete via stream.
pub async fn test_delete_stream(op: Operator) -> Result<()> {
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

    op.with_limit(30)
        .remove_via(futures::stream::iter(expected.clone()).map(|v| format!("{dir}/{v}")))
        .await?;

    // Stat it again to check.
    for path in expected.iter() {
        assert!(
            !op.is_exist(&format!("{dir}/{path}")).await?,
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

    op.remove_all(&parent).await?;

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
    assert!(!op.is_exist(path.as_str()).await?);

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
