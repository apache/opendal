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
use opendal::ErrorKind;
use opendal::Operator;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_rename
macro_rules! behavior_rename_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            $(
                #[tokio::test]
                $(
                    #[$meta]
                )*
                async fn [<rename_ $test >]() -> anyhow::Result<()> {
                    match OPERATOR.as_ref() {
                        Some(op) if op.info().can_read()
                            && op.info().can_write()
                            && op.info().can_rename() => $crate::rename::$test(op.clone()).await,
                        Some(_) => {
                            log::warn!("service {} doesn't support rename, ignored", opendal::Scheme::$service);
                            Ok(())
                        },
                        None => {
                            log::warn!("service {} not initiated, ignored", opendal::Scheme::$service);
                            Ok(())
                        }
                    }
                }
            )*
        }
    };
}

#[macro_export]
macro_rules! behavior_rename_tests {
     ($($service:ident),*) => {
        $(
            behavior_rename_test!(
                $service,

                test_rename,
                test_rename_non_existing_source,
                test_rename_source_dir,
                test_rename_target_dir,
                test_rename_self,
                test_rename_nested,
                test_rename_overwrite,

            );
        )*
    };
}

/// Rename a file and test with stat.
pub async fn test_rename(op: Operator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (source_content, _) = gen_bytes();

    op.write(&source_path, source_content.clone()).await?;

    let target_path = uuid::Uuid::new_v4().to_string();

    op.rename(&source_path, &target_path).await?;

    let err = op.stat(&source_path).await.expect_err("stat must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let target_content = op.read(&target_path).await.expect("read must succeed");
    assert_eq!(target_content, source_content);

    op.delete(&source_path).await.expect("delete must succeed");
    op.delete(&target_path).await.expect("delete must succeed");
    Ok(())
}

/// Rename a nonexistent source should return an error.
pub async fn test_rename_non_existing_source(op: Operator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let target_path = uuid::Uuid::new_v4().to_string();

    let err = op
        .rename(&source_path, &target_path)
        .await
        .expect_err("rename must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

/// Rename a dir as source should return an error.
pub async fn test_rename_source_dir(op: Operator) -> Result<()> {
    let source_path = format!("{}/", uuid::Uuid::new_v4());
    let target_path = uuid::Uuid::new_v4().to_string();

    op.create_dir(&source_path).await?;

    let err = op
        .rename(&source_path, &target_path)
        .await
        .expect_err("rename must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);
    Ok(())
}

/// Rename to a dir should return an error.
pub async fn test_rename_target_dir(op: Operator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    op.write(&source_path, content).await?;

    let target_path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&target_path).await?;

    let err = op
        .rename(&source_path, &target_path)
        .await
        .expect_err("rename must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);

    op.delete(&source_path).await.expect("delete must succeed");
    op.delete(&target_path).await.expect("delete must succeed");
    Ok(())
}

/// Rename a file to self should return an error.
pub async fn test_rename_self(op: Operator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    op.write(&source_path, content).await?;

    let err = op
        .rename(&source_path, &source_path)
        .await
        .expect_err("rename must fail");
    assert_eq!(err.kind(), ErrorKind::IsSameFile);

    op.delete(&source_path).await.expect("delete must succeed");
    Ok(())
}

/// Rename to a nested path, parent path should be created successfully.
pub async fn test_rename_nested(op: Operator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (source_content, _) = gen_bytes();

    op.write(&source_path, source_content.clone()).await?;

    let target_path = format!(
        "{}/{}/{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );

    op.rename(&source_path, &target_path).await?;

    let err = op.stat(&source_path).await.expect_err("stat must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let target_content = op.read(&target_path).await.expect("read must succeed");
    assert_eq!(target_content, source_content);

    op.delete(&source_path).await.expect("delete must succeed");
    op.delete(&target_path).await.expect("delete must succeed");
    Ok(())
}

/// Rename to a exist path should overwrite successfully.
pub async fn test_rename_overwrite(op: Operator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (source_content, _) = gen_bytes();

    op.write(&source_path, source_content.clone()).await?;

    let target_path = uuid::Uuid::new_v4().to_string();
    let (target_content, _) = gen_bytes();
    assert_ne!(source_content, target_content);

    op.write(&target_path, target_content).await?;

    op.rename(&source_path, &target_path).await?;

    let err = op.stat(&source_path).await.expect_err("stat must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let target_content = op.read(&target_path).await.expect("read must succeed");
    assert_eq!(target_content, source_content);

    op.delete(&source_path).await.expect("delete must succeed");
    op.delete(&target_path).await.expect("delete must succeed");
    Ok(())
}
