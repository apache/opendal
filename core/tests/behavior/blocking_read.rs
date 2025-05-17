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

    if cap.read && cap.write && cap.blocking {
        tests.extend(blocking_trials!(
            op,
            test_blocking_read_full,
            test_blocking_read_range,
            test_blocking_read_not_exist
        ))
    }

    if cap.read && !cap.write && cap.blocking {
        tests.extend(blocking_trials!(
            op,
            test_blocking_read_only_stat_file_and_dir,
            test_blocking_read_only_stat_special_chars,
            test_blocking_read_only_stat_not_exist,
            test_blocking_read_only_read_full,
            test_blocking_read_only_read_with_range,
            test_blocking_read_only_read_not_exist
        ))
    }
}

/// Read full content should match.
pub fn test_blocking_read_full(op: BlockingOperator) -> Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .expect("write must succeed");

    let bs = op.read(&path)?.to_bytes();
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    Ok(())
}

/// Read range content should match.
pub fn test_blocking_read_range(op: BlockingOperator) -> Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(op.clone());
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .expect("write must succeed");

    let bs = op
        .read_with(&path)
        .range(offset..offset + length)
        .call()?
        .to_bytes();
    assert_eq!(bs.len() as u64, length, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!(
            "{:x}",
            Sha256::digest(&content[offset as usize..(offset + length) as usize])
        ),
        "read content"
    );
    Ok(())
}

/// Read not exist file should return NotFound
pub fn test_blocking_read_not_exist(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.read(&path);
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
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
    let bs = op.read("normal_file.txt")?.to_bytes();
    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5",
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub fn test_blocking_read_only_read_with_range(op: BlockingOperator) -> Result<()> {
    let bs = op
        .read_with("normal_file.txt")
        .range(1024..2048)
        .call()?
        .to_bytes();
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
