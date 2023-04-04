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
/// - can_copy
macro_rules! behavior_copy_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _copy>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op) if op.info().can_read()
                              && op.info().can_write()
                              && op.info().can_copy() => $crate::copy::$test(op).await,
                            Some(_) => {
                                log::warn!("service {} doesn't support copy, ignored", opendal::Scheme::$service);
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
        }
    };
}

#[macro_export]
macro_rules! behavior_copy_tests {
     ($($service:ident),*) => {
        $(
            behavior_copy_test!(
                $service,

                test_copy,
                test_non_existing_source,
                test_copy_source_dir,
                test_copy_target_dir,
                test_copy_self,
                test_copy_nested,
                test_copy_overwrite,

            );
        )*
    };
}

// Copy a file and test with stat.
pub async fn test_copy(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.write(&path, content).await?;

    let path2 = uuid::Uuid::new_v4().to_string();

    op.copy(&path, &path2).await?;

    let meta = op.stat(&path2).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");
    op.delete(&path2).await.expect("delete must succeed");
    Ok(())
}

// Copy a nonexistent source should return an error.
pub async fn test_non_existing_source(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let path2 = uuid::Uuid::new_v4().to_string();

    let err = op.copy(&path, &path2).await.expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

// Copy a dir as source should return an error.
pub async fn test_copy_source_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let path2 = uuid::Uuid::new_v4().to_string();

    op.create_dir(&path).await?;

    let err = op.copy(&path, &path2).await.expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);
    Ok(())
}

// Copy to a dir should return an error.
pub async fn test_copy_target_dir(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _size) = gen_bytes();

    op.write(&path, content).await?;

    let path2 = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path2).await?;

    let err = op.copy(&path, &path2).await.expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);

    op.delete(&path).await.expect("delete must succeed");
    op.delete(&path2).await.expect("delete must succeed");
    Ok(())
}

// Copy a file to self should return an error.
pub async fn test_copy_self(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _size) = gen_bytes();

    op.write(&path, content).await?;

    let err = op.copy(&path, &path).await.expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsSameFile);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

// Copy to a nested path, parent path should be created successfully.
pub async fn test_copy_nested(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.write(&path, content).await?;

    let path2 = format!("{}/{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());

    op.copy(&path, &path2).await?;

    let meta = op.stat(&path2).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");
    op.delete(&path2).await.expect("delete must succeed");
    Ok(())
}

// Copy to a exist path should overwrite successfully.
pub async fn test_copy_overwrite(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.write(&path, content.clone()).await?;

    let path2 = uuid::Uuid::new_v4().to_string();
    let (content2, _size2) = gen_bytes();

    op.write(&path2, content2).await?;

    op.copy(&path, &path2).await?;

    let data2 = op.read(&path2).await.expect("read must succeed");
    assert_eq!(data2.len(), size);
    assert_eq!(data2, content);

    op.delete(&path).await.expect("delete must succeed");
    op.delete(&path2).await.expect("delete must succeed");
    Ok(())
}
