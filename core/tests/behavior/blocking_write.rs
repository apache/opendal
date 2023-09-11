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
use log::warn;
use sha2::Digest;
use sha2::Sha256;

use crate::*;

pub fn behavior_blocking_write_tests(op: &Operator) -> Vec<Trial> {
    let cap = op.info().full_capability();

    if !(cap.read && cap.write && cap.blocking) {
        return vec![];
    }

    blocking_trials!(
        op,
        test_blocking_create_dir,
        test_blocking_create_dir_existing,
        test_blocking_write_file,
        test_blocking_write_with_dir_path,
        test_blocking_write_with_special_chars,
        test_blocking_stat_file,
        test_blocking_stat_dir,
        test_blocking_stat_with_special_chars,
        test_blocking_stat_not_exist,
        test_blocking_read_full,
        test_blocking_read_range,
        test_blocking_read_large_range,
        test_blocking_read_not_exist,
        test_blocking_fuzz_range_reader,
        test_blocking_fuzz_offset_reader,
        test_blocking_fuzz_part_reader,
        test_blocking_delete_file,
        test_blocking_remove_one_file
    )
}

/// Create dir with dir path should succeed.
pub fn test_blocking_create_dir(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path)?;

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Create dir on existing dir should succeed.
pub fn test_blocking_create_dir_existing(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path)?;

    op.create_dir(&path)?;

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Write a single file and test with stat.
pub fn test_blocking_write_file(op: BlockingOperator) -> Result<()> {
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
pub fn test_blocking_write_with_dir_path(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes();

    let result = op.write(&path, content);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}

/// Write a single file with special chars should succeed.
pub fn test_blocking_write_with_special_chars(op: BlockingOperator) -> Result<()> {
    // Ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 is resolved");
        return Ok(());
    }
    // Ignore test for atomicdata until TODO-Issue Link addressed.
    if op.info().scheme() == opendal::Scheme::Atomicdata {
        warn!("ignore test for atomicdata until TODO-Issue Link is resolved");
        return Ok(());
    }

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
pub fn test_blocking_stat_file(op: BlockingOperator) -> Result<()> {
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
pub fn test_blocking_stat_dir(op: BlockingOperator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).expect("write must succeed");

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Stat existing file with special chars should return metadata
pub fn test_blocking_stat_with_special_chars(op: BlockingOperator) -> Result<()> {
    // Ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 is resolved");
        return Ok(());
    }
    // Ignore test for atomicdata until TODO-Issue Link addressed.
    if op.info().scheme() == opendal::Scheme::Atomicdata {
        warn!("ignore test for atomicdata until TODO-Issue Link is resolved");
        return Ok(());
    }

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
pub fn test_blocking_stat_not_exist(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.stat(&path);
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

/// Read full content should match.
pub fn test_blocking_read_full(op: BlockingOperator) -> Result<()> {
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
pub fn test_blocking_read_range(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .expect("write must succeed");

    let bs = op.read_with(&path).range(offset..offset + length).call()?;
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
pub fn test_blocking_read_large_range(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, _) = gen_offset_length(size);

    op.write(&path, content.clone())
        .expect("write must succeed");

    let bs = op.read_with(&path).range(offset..u32::MAX as u64).call()?;
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
pub fn test_blocking_read_not_exist(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.read(&path);
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

pub fn test_blocking_fuzz_range_reader(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.write(&path, content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op
        .reader_with(&path)
        .range(0..content.len() as u64)
        .call()?;

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

pub fn test_blocking_fuzz_offset_reader(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.write(&path, content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op.reader_with(&path).range(0..).call()?;

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

pub fn test_blocking_fuzz_part_reader(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content, offset as usize, length as usize);
    let mut o = op
        .reader_with(&path)
        .range(offset..offset + length)
        .call()?;

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
pub fn test_blocking_delete_file(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.write(&path, content).expect("write must succeed");

    op.delete(&path)?;

    // Stat it again to check.
    assert!(!op.is_exist(&path)?);

    Ok(())
}

/// Remove one file
pub fn test_blocking_remove_one_file(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    op.write(&path, content).expect("write must succeed");

    op.remove(vec![path.clone()])?;

    // Stat it again to check.
    assert!(!op.is_exist(&path)?);

    Ok(())
}
