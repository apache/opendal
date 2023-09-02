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

pub fn behavior_blocking_read_only_tests(op: &Operator) -> Vec<Trial> {
    let cap = op.info().full_capability();

    if !(cap.read && !cap.write && cap.blocking) {
        return vec![];
    }

    blocking_trials!(
        op,
        test_blocking_read_only_stat_file_and_dir,
        test_blocking_read_only_stat_special_chars,
        test_blocking_read_only_stat_not_exist,
        test_blocking_read_only_read_full,
        test_blocking_read_only_read_with_range,
        test_blocking_read_only_read_not_exist
    )
}

/// Stat normal file and dir should return metadata
pub fn test_blocking_read_only_stat_file_and_dir(op: BlockingOperator) -> Result<()> {
    let meta = op.stat("normal_file.txt")?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 30482);

    let meta = op.stat("normal_dir/")?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    Ok(())
}

/// Stat special file and dir should return metadata
pub fn test_blocking_read_only_stat_special_chars(op: BlockingOperator) -> Result<()> {
    let meta = op.stat("special_file  !@#$%^&()_+-=;',.txt")?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 30482);

    let meta = op.stat("special_dir  !@#$%^&()_+-=;',/")?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    Ok(())
}

/// Stat not exist file should return NotFound
pub fn test_blocking_read_only_stat_not_exist(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.stat(&path);
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

/// Read full content should match.
pub fn test_blocking_read_only_read_full(op: BlockingOperator) -> Result<()> {
    let bs = op.read("normal_file.txt")?;
    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "6aa0c53aec3cea11712191b090928660b36d89cb586b21e23f3a9834231c0600",
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub fn test_blocking_read_only_read_with_range(op: BlockingOperator) -> Result<()> {
    let bs = op.read_with("normal_file.txt").range(1024..2048).call()?;
    assert_eq!(bs.len(), 1024, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "330c6d57fdc1119d6021b37714ca5ad0ede12edd484f66be799a5cff59667034",
        "read content"
    );

    Ok(())
}

/// Read not exist file should return NotFound
pub fn test_blocking_read_only_read_not_exist(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.read(&path);
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}
