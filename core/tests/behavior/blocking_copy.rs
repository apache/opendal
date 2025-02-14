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
use sha2::Digest;
use sha2::Sha256;

use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.read && cap.write && cap.copy && cap.blocking {
        tests.extend(blocking_trials!(
            op,
            test_blocking_copy_file,
            test_blocking_copy_non_existing_source,
            test_blocking_copy_source_dir,
            test_blocking_copy_target_dir,
            test_blocking_copy_self,
            test_blocking_copy_nested,
            test_blocking_copy_overwrite
        ))
    }
}

/// Copy a file and test with stat.
pub fn test_blocking_copy_file(op: BlockingOperator) -> Result<()> {
    let (source_path, source_content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&source_path, source_content.clone())?;

    let target_path = TEST_FIXTURE.new_file_path();

    op.copy(&source_path, &target_path)?;

    let target_content = op.read(&target_path).expect("read must succeed").to_bytes();
    assert_eq!(
        format!("{:x}", Sha256::digest(target_content)),
        format!("{:x}", Sha256::digest(&source_content)),
    );

    Ok(())
}

/// Copy a nonexistent source should return an error.
pub fn test_blocking_copy_non_existing_source(op: BlockingOperator) -> Result<()> {
    let source_path = uuid::Uuid::new_v4().to_string();
    let target_path = uuid::Uuid::new_v4().to_string();

    let err = op
        .copy(&source_path, &target_path)
        .expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}

/// Copy a dir as source should return an error.
pub fn test_blocking_copy_source_dir(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let source_path = format!("{}/", uuid::Uuid::new_v4());
    let target_path = uuid::Uuid::new_v4().to_string();

    op.create_dir(&source_path)?;

    let err = op
        .copy(&source_path, &target_path)
        .expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);
    Ok(())
}

/// Copy to a dir should return an error.
pub fn test_blocking_copy_target_dir(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let (source_path, source_content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&source_path, source_content)?;

    let target_path = TEST_FIXTURE.new_dir_path();

    op.create_dir(&target_path)?;

    let err = op
        .copy(&source_path, &target_path)
        .expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsADirectory);
    Ok(())
}

/// Copy a file to self should return an error.
pub fn test_blocking_copy_self(op: BlockingOperator) -> Result<()> {
    let (source_path, source_content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&source_path, source_content)?;

    let err = op
        .copy(&source_path, &source_path)
        .expect_err("copy must fail");
    assert_eq!(err.kind(), ErrorKind::IsSameFile);
    Ok(())
}

/// Copy to a nested path, parent path should be created successfully.
pub fn test_blocking_copy_nested(op: BlockingOperator) -> Result<()> {
    let (source_path, source_content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(&source_path, source_content.clone())?;

    let target_path = format!(
        "{}/{}/{}",
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4(),
        uuid::Uuid::new_v4()
    );
    TEST_FIXTURE.add_path(target_path.clone());

    op.copy(&source_path, &target_path)?;

    let target_content = op.read(&target_path).expect("read must succeed").to_bytes();
    assert_eq!(
        format!("{:x}", Sha256::digest(target_content)),
        format!("{:x}", Sha256::digest(&source_content)),
    );
    Ok(())
}

/// Copy to a exist path should overwrite successfully.
pub fn test_blocking_copy_overwrite(op: BlockingOperator) -> Result<()> {
    let (source_path, source_content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(&source_path, source_content.clone())?;

    let (target_path, target_content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(&target_path, target_content)?;

    op.copy(&source_path, &target_path)?;

    let target_content = op.read(&target_path).expect("read must succeed").to_bytes();
    assert_eq!(
        format!("{:x}", Sha256::digest(target_content)),
        format!("{:x}", Sha256::digest(&source_content)),
    );
    Ok(())
}
