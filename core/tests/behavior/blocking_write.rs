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

use std::io::Read;
use std::io::Seek;

use anyhow::Result;
use log::debug;
use opendal::BlockingOperator;
use opendal::EntryMode;
use opendal::ErrorKind;
use sha2::Digest;
use sha2::Sha256;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_blocking
macro_rules! behavior_blocking_write_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _blocking_write>] {
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
                                && op.info().can_blocking() => $crate::blocking_write::$test(op.blocking()),
                            Some(_) => {
                                log::warn!("service {} doesn't support blocking_write, ignored", opendal::Scheme::$service);
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
macro_rules! behavior_blocking_write_tests {
     ($($service:ident),*) => {
        $(
            behavior_blocking_write_test!(
                $service,

                test_create_dir,
                test_create_dir_existing,
                test_write,
                test_write_with_dir_path,
                test_write_with_special_chars,
                test_stat,
                test_stat_dir,
                test_stat_with_special_chars,
                test_stat_not_exist,
                test_read_full,
                test_read_range,
                test_read_large_range,
                test_read_not_exist,
                test_fuzz_range_reader,
                test_fuzz_offset_reader,
                test_fuzz_part_reader,
                test_delete,
            );
        )*
    };
}

/// Create dir with dir path should succeed.
pub fn test_create_dir(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path)?;

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Create dir on existing dir should succeed.
pub fn test_create_dir_existing(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path)?;

    op.create_dir(&path)?;

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Write a single file and test with stat.
pub fn test_write(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.write(&path, content)?;

    let meta = op.stat(&path).expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Write file with dir path should return an error
pub fn test_write_with_dir_path(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes();

    let result = op.write(&path, content);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}

/// Write a single file with special chars should succeed.
pub fn test_write_with_special_chars(op: BlockingOperator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.write(&path, content)?;

    let meta = op.stat(&path).expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
pub fn test_stat(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.write(&path, content).expect("write must succeed");

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
pub fn test_stat_dir(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).expect("write must succeed");

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Stat existing file with special chars should return metadata
pub fn test_stat_with_special_chars(op: BlockingOperator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.write(&path, content).expect("write must succeed");

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Stat not exist file should return NotFound
pub fn test_stat_not_exist(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.stat(&path);
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

/// Read full content should match.
pub fn test_read_full(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.write(&path, content.clone())
        .expect("write must succeed");

    let bs = op.read(&path)?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Read range content should match.
pub fn test_read_range(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .expect("write must succeed");

    let bs = op.range_read(&path, offset..offset + length)?;
    assert_eq!(bs.len() as u64, length, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!(
            "{:x}",
            Sha256::digest(&content[offset as usize..(offset + length) as usize])
        ),
        "read content"
    );

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Read large range content should match.
pub fn test_read_large_range(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, _) = gen_offset_length(size);

    op.write(&path, content.clone())
        .expect("write must succeed");

    let bs = op.range_read(&path, offset..u32::MAX as u64)?;
    assert_eq!(
        bs.len() as u64,
        size as u64 - offset,
        "read size with large range"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content[offset as usize..])),
        "read content with large range"
    );

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Read not exist file should return NotFound
pub fn test_read_not_exist(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.read(&path);
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

pub fn test_fuzz_range_reader(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.write(&path, content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op.range_reader(&path, 0..content.len() as u64)?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectReaderAction::Read(size) => {
                let mut bs = vec![0; size];
                let n = o.read(&mut bs)?;
                fuzzer.check_read(n, &bs[..n])
            }
            ObjectReaderAction::Seek(input_pos) => {
                let actual_pos = o.seek(input_pos)?;
                fuzzer.check_seek(input_pos, actual_pos)
            }
            ObjectReaderAction::Next => {
                let actual_bs = o.next().map(|v| v.expect("next should not return error"));
                fuzzer.check_next(actual_bs)
            }
        }
    }

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

pub fn test_fuzz_offset_reader(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.write(&path, content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op.range_reader(&path, 0..)?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectReaderAction::Read(size) => {
                let mut bs = vec![0; size];
                let n = o.read(&mut bs)?;
                fuzzer.check_read(n, &bs[..n])
            }
            ObjectReaderAction::Seek(input_pos) => {
                let actual_pos = o.seek(input_pos)?;
                fuzzer.check_seek(input_pos, actual_pos)
            }
            ObjectReaderAction::Next => {
                let actual_bs = o.next().map(|v| v.expect("next should not return error"));
                fuzzer.check_next(actual_bs)
            }
        }
    }

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

pub fn test_fuzz_part_reader(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content, offset as usize, length as usize);
    let mut o = op.range_reader(&path, offset..offset + length)?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectReaderAction::Read(size) => {
                let mut bs = vec![0; size];
                let n = o.read(&mut bs)?;
                fuzzer.check_read(n, &bs[..n])
            }
            ObjectReaderAction::Seek(input_pos) => {
                let actual_pos = o.seek(input_pos)?;
                fuzzer.check_seek(input_pos, actual_pos)
            }
            ObjectReaderAction::Next => {
                let actual_bs = o.next().map(|v| v.expect("next should not return error"));
                fuzzer.check_next(actual_bs)
            }
        }
    }

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

// Delete existing file should succeed.
pub fn test_delete(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.write(&path, content).expect("write must succeed");

    op.delete(&path)?;

    // Stat it again to check.
    assert!(!op.is_exist(&path)?);

    Ok(())
}
