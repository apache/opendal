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
use log::warn;

use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.delete && cap.write {
        tests.extend(async_trials!(
            op,
            test_delete_file,
            test_delete_empty_dir,
            test_delete_with_special_chars,
            test_delete_not_existing,
            test_delete_stream,
            test_remove_one_file
        ))
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
    // Ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 is resolved");
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

    let dir = uuid::Uuid::new_v4().to_string();
    op.create_dir(&format!("{dir}/"))
        .await
        .expect("creat must succeed");

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
