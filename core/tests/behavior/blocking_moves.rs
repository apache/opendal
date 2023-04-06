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
/// - can_moves
/// - can_blocking
macro_rules! behavior_blocking_moves_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _blocking_moves>] {
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
                              && op.info().can_moves()
                              && op.info().can_blocking()
                              => $crate::blocking_moves::$test(op.blocking()),
                            Some(_) => {
                                log::warn!("service {} doesn't support blocking_moves, ignored", opendal::Scheme::$service);
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
macro_rules! behavior_blocking_moves_tests {
     ($($service:ident),*) => {
        $(
            behavior_blocking_moves_test!(
                $service,

                test_moves_file,
                test_moves_dir,
                test_moves_non_existing_source,
                test_moves_file_to_directory,
                test_moves_dir_to_dir,
                test_moves_self,
                test_moves_nested,
                test_moves_overwrite,

            );
        )*
    };
}

// moves file and test with stat.
pub fn test_moves_file(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (source_content, _) = gen_bytes();

    op.write(&source_path, source_content.clone())?;

    let target_path = uuid::Uuid::new_v4().to_string();

    op.moves(&source_path, &target_path)?;

    let read_source_err = op.stat(&source_path).expect_err("read source must fail");
    assert_eq!(read_source_err.kind(), ErrorKind::NotFound);

    let target_content = op.read(&target_path).expect("read must succeed");
    assert_eq!(target_content, source_content);

    op.delete(&target_path).expect("delete must succeed");
    Ok(())
}

// Moving directory and test with stat.
pub fn test_moves_dir(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string() + "/";
    let sub_file = format!("{}{}", source_path, uuid::Uuid::new_v4());
    let (source_content, _) = gen_bytes();

    op.write(&sub_file, source_content.clone())?;

    let target_path = uuid::Uuid::new_v4().to_string();

    op.moves(&source_path, &target_path)?;

    let read_source_err = op
        .stat(&sub_file)
        .expect_err("read source sub file must fail");
    assert_eq!(read_source_err.kind(), ErrorKind::NotFound);

    let target_sub_file = format!("{}/{}", target_path, sub_file);
    let target_content = op.read(&target_sub_file).expect("read must succeed");
    assert_eq!(target_content, source_content);

    op.delete(&target_path).expect("delete must succeed");
    Ok(())
}

// Moving a nonexistent source should return an error.
pub fn test_moves_non_existing_source(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let target_path = uuid::Uuid::new_v4().to_string();

    let err = op
        .moves(&source_path, &target_path)
        .expect_err("moves must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

// Moving file to a dir should return an error.
pub fn test_moves_file_to_directory(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    op.write(&source_path, content)?;

    let target_path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&target_path)?;

    let err = op
        .moves(&source_path, &target_path)
        .expect_err("moves must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);

    op.delete(&target_path).expect("delete must succeed");
    Ok(())
}

// Moving directory to a dir
pub fn test_moves_dir_to_dir(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string() + "/";
    op.create_dir(&source_path)?;

    let target_path = format!("{}/", uuid::Uuid::new_v4());
    op.create_dir(&target_path)?;

    let occupy = format!("{}{}", target_path, uuid::Uuid::new_v4());
    op.write(
        &occupy,
        b"this file is used for occupying the directory, and making move fail".to_vec(),
    )?;

    let err = op
        .moves(&source_path, &target_path)
        .expect_err("moves directory to non-empty directory must fail");

    assert_eq!(err.kind(), ErrorKind::AlreadyExists);

    op.delete(&occupy).expect("delete must succeed");

    let _ = op
        .moves(&source_path, &target_path)
        .expect("moves directory to empty directory should success");

    op.delete(&target_path).expect("delete must succeed");
    Ok(())
}

// Moving to self should be allowed
pub fn test_moves_self(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    op.write(&source_path, content)?;

    let _ = op
        .moves(&source_path, &source_path)
        .expect("move must success");

    op.delete(&source_path).expect("delete must succeed");
    Ok(())
}

// Move to a nested path, parent path should be created successfully.
pub fn test_moves_nested(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (source_content, _) = gen_bytes();

    op.write(&source_path, source_content.clone())?;

    let target_path = format!("{}/{}", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());

    op.moves(&source_path, &target_path)?;

    let target_content = op.read(&target_path).expect("read must succeed");
    assert_eq!(target_content, source_content);

    op.delete(&target_path).expect("delete must succeed");
    Ok(())
}

// Copy to a exist path should overwrite successfully.
pub fn test_moves_overwrite(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let (source_content, _) = gen_bytes();

    op.write(&source_path, source_content.clone())?;

    let target_path = uuid::Uuid::new_v4().to_string();
    let (target_content, _) = gen_bytes();
    assert_ne!(source_content, target_content);

    op.write(&target_path, target_content)?;

    op.moves(&source_path, &target_path)?;

    let target_content = op.read(&target_path).expect("read must succeed");
    assert_eq!(target_content, source_content);

    op.delete(&target_path).expect("delete must succeed");
    Ok(())
}
