// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Read;
use std::io::Seek;

use anyhow::Result;
use log::debug;
use opendal::ErrorKind;
use opendal::ObjectMode;
use opendal::Operator;
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
                            Some(op) if op.metadata().can_read()
                                && op.metadata().can_write()
                                && op.metadata().can_blocking() => $crate::blocking_write::$test(op),
                            Some(_) => {
                                log::warn!("service {} doesn't support read, ignored", opendal::Scheme::$service);
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

                test_create_file,
                test_create_file_existing,
                test_create_file_with_special_chars,
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

/// Create file with file path should succeed.
pub fn test_create_file(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let o = op.object(&path);

    o.blocking_create()?;

    let meta = o.blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Create file on existing file path should succeed.
pub fn test_create_file_existing(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let o = op.object(&path);

    o.blocking_create()?;

    o.blocking_create()?;

    let meta = o.blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Create file with special chars should succeed.
pub fn test_create_file_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());

    let o = op.object(&path);
    debug!("{o:?}");

    o.blocking_create()?;

    let meta = o.blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Create dir with dir path should succeed.
pub fn test_create_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let o = op.object(&path);

    o.blocking_create()?;

    let meta = o.blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Create dir on existing dir should succeed.
pub fn test_create_dir_existing(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let o = op.object(&path);

    o.blocking_create()?;

    o.blocking_create()?;

    let meta = o.blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Write a single file and test with stat.
pub fn test_write(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path).blocking_write(content)?;

    let meta = op.object(&path).blocking_stat().expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Write file with dir path should return an error
pub fn test_write_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes();

    let result = op.object(&path).blocking_write(content);
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::ObjectIsADirectory);

    Ok(())
}

/// Write a single file with special chars should succeed.
pub fn test_write_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path).blocking_write(content)?;

    let meta = op.object(&path).blocking_stat().expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
pub fn test_stat(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .blocking_write(content)
        .expect("write must succeed");

    let meta = op.object(&path).blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
pub fn test_stat_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.object(&path)
        .blocking_create()
        .expect("write must succeed");

    let meta = op.object(&path).blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file with special chars should return metadata
pub fn test_stat_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .blocking_write(content)
        .expect("write must succeed");

    let meta = op.object(&path).blocking_stat()?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Stat not exist file should return NotFound
pub fn test_stat_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.object(&path).blocking_stat();
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), ErrorKind::ObjectNotFound);

    Ok(())
}

/// Read full content should match.
pub fn test_read_full(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .blocking_write(content.clone())
        .expect("write must succeed");

    let bs = op.object(&path).blocking_read()?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Read range content should match.
pub fn test_read_range(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.object(&path)
        .blocking_write(content.clone())
        .expect("write must succeed");

    let bs = op
        .object(&path)
        .blocking_range_read(offset..offset + length)?;
    assert_eq!(bs.len() as u64, length, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!(
            "{:x}",
            Sha256::digest(&content[offset as usize..(offset + length) as usize])
        ),
        "read content"
    );

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Read large range content should match.
pub fn test_read_large_range(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, _) = gen_offset_length(size);

    op.object(&path)
        .blocking_write(content.clone())
        .expect("write must succeed");

    let bs = op
        .object(&path)
        .blocking_range_read(offset..u32::MAX as u64)?;
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

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Read not exist file should return NotFound
pub fn test_read_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.object(&path).blocking_read();
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::ObjectNotFound);

    Ok(())
}

pub fn test_fuzz_range_reader(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.object(&path)
        .blocking_write(content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op
        .object(&path)
        .blocking_range_reader(0..content.len() as u64)?;

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

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

pub fn test_fuzz_offset_reader(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.object(&path)
        .blocking_write(content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op.object(&path).blocking_range_reader(0..)?;

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

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

pub fn test_fuzz_part_reader(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.object(&path)
        .blocking_write(content.clone())
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content, offset as usize, length as usize);
    let mut o = op
        .object(&path)
        .blocking_range_reader(offset..offset + length)?;

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

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

// Delete existing file should succeed.
pub fn test_delete(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.object(&path)
        .blocking_write(content)
        .expect("write must succeed");

    op.object(&path).blocking_delete()?;

    // Stat it again to check.
    assert!(!op.object(&path).blocking_is_exist()?);

    Ok(())
}
