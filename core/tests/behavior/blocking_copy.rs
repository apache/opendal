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
use opendal::BlockingOperator;
use opendal::ErrorKind;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_blocking
macro_rules! behavior_blocking_copy_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _blocking_copy>] {
                $(
                    #[test]
                    $(
                        #[$meta]
                    )*
                    fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op) if op.info().can_read()
                                && op.info().can_write()
                                && op.info().can_copy()
                                && op.info().can_blocking() => $crate::blocking_copy::$test(op.blocking()),
                            Some(_) => {
                                log::warn!("service {} doesn't support blocking_copy, ignored", opendal::Scheme::$service);
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
macro_rules! behavior_blocking_copy_tests {
     ($($service:ident),*) => {
        $(
            behavior_blocking_copy_test!(
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
pub fn test_copy(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.write(&path, content)?;

    let path2 = uuid::Uuid::new_v4().to_string();

    op.copy(&path, &path2)?;

    let meta = op.stat(&path2).expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).expect("delete must succeed");
    op.delete(&path2).expect("delete must succeed");
    Ok(())
}

// Copy a nonexistent source should return an error.
pub fn test_non_existing_source(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let path2 = uuid::Uuid::new_v4().to_string();

    let err = op.copy(&path, &path2).expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

// Copy a dir as source should return an error.
pub fn test_copy_source_dir(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let path2 = uuid::Uuid::new_v4().to_string();

    op.create_dir(&path)?;

    let err = op.copy(&path, &path2).expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);
    Ok(())
}

// Copy to a dir should return an error.
pub fn test_copy_target_dir(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _size) = gen_bytes();

    op.write(&path, content)?;

    let path2 = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path2)?;

    let err = op.copy(&path, &path2).expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);

    op.delete(&path).expect("delete must succeed");
    op.delete(&path2).expect("delete must succeed");
    Ok(())
}

// Copy a file to self should return an error.
pub fn test_copy_self(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _size) = gen_bytes();

    op.write(&path, content)?;

    let err = op.copy(&path, &path).expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsSameFile);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

// Copy to a nested path, parent path should be created successfully.
pub fn test_copy_nested(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.write(&path, content)?;

    let path2 = format!("{}/{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());

    op.copy(&path, &path2)?;

    let meta = op.stat(&path2).expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).expect("delete must succeed");
    op.delete(&path2).expect("delete must succeed");
    Ok(())
}

// Copy to a exist path should overwrite successfully.
pub fn test_copy_overwrite(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.write(&path, content.clone())?;

    let path2 = uuid::Uuid::new_v4().to_string();
    let (content2, _size2) = gen_bytes();

    op.write(&path2, content2)?;

    op.copy(&path, &path2)?;

    let data2 = op.read(&path2).expect("read must succeed");
    assert_eq!(data2.len(), size);
    assert_eq!(data2, content);

    op.delete(&path).expect("delete must succeed");
    op.delete(&path2).expect("delete must succeed");
    Ok(())
}
