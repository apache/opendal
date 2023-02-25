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

use anyhow::Result;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::StreamExt;
use log::debug;
use log::warn;
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
macro_rules! behavior_write_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _write>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op) if op.metadata().can_read() && op.metadata().can_write() => $crate::write::$test(op).await,
                            Some(_) => {
                                log::warn!("service {} doesn't support write, ignored", opendal::Scheme::$service);
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
macro_rules! behavior_write_tests {
     ($($service:ident),*) => {
        $(
            behavior_write_test!(
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
                test_stat_not_cleaned_path,
                test_stat_not_exist,
                test_stat_root,
                test_read_full,
                test_read_range,
                test_read_large_range,
                test_reader_range,
                test_reader_from,
                test_reader_tail,
                test_read_not_exist,
                test_fuzz_range_reader,
                test_fuzz_offset_reader,
                test_fuzz_part_reader,
                test_read_with_dir_path,
                test_read_with_special_chars,
                test_delete,
                test_delete_empty_dir,
                test_delete_with_special_chars,
                test_delete_not_existing,
                test_delete_stream,
            );
        )*
    };
}

/// Create file with file path should succeed.
pub async fn test_create_file(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let o = op.object(&path);

    o.create().await?;

    let meta = o.stat().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Create file on existing file path should succeed.
pub async fn test_create_file_existing(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let o = op.object(&path);

    o.create().await?;

    o.create().await?;

    let meta = o.stat().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Create file with special chars should succeed.
pub async fn test_create_file_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());

    let o = op.object(&path);

    o.create().await?;

    let meta = o.stat().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Create dir with dir path should succeed.
pub async fn test_create_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let o = op.object(&path);

    o.create().await?;

    let meta = o.stat().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Create dir on existing dir should succeed.
pub async fn test_create_dir_existing(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let o = op.object(&path);

    o.create().await?;

    o.create().await?;

    let meta = o.stat().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Write a single file and test with stat.
pub async fn test_write(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.object(&path).write(content).await?;

    let meta = op.object(&path).stat().await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Write file with dir path should return an error
pub async fn test_write_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes();

    let result = op.object(&path).write(content).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::ObjectIsADirectory);

    Ok(())
}

/// Write a single file with special chars should succeed.
pub async fn test_write_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    let (content, size) = gen_bytes();

    op.object(&path).write(content).await?;

    let meta = op.object(&path).stat().await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
pub async fn test_stat(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content)
        .await
        .expect("write must succeed");

    let meta = op.object(&path).stat().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
pub async fn test_stat_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.object(&path).create().await.expect("write must succeed");

    let meta = op.object(&path).stat().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file with special chars should return metadata
pub async fn test_stat_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content)
        .await
        .expect("write must succeed");

    let meta = op.object(&path).stat().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat not cleaned path should also succeed.
pub async fn test_stat_not_cleaned_path(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content)
        .await
        .expect("write must succeed");

    let meta = op.object(&format!("//{}", &path)).stat().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat not exist file should return NotFound
pub async fn test_stat_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.object(&path).stat().await;
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), ErrorKind::ObjectNotFound);

    Ok(())
}

/// Root should be able to stat and returns DIR.
pub async fn test_stat_root(op: Operator) -> Result<()> {
    let meta = op.object("").stat().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    let meta = op.object("/").stat().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    Ok(())
}

/// Read full content should match.
pub async fn test_read_full(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let bs = op.object(&path).read().await?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read range content should match.
pub async fn test_read_range(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let bs = op.object(&path).range_read(offset..offset + length).await?;
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
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read large range content should match.
pub async fn test_read_large_range(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, _) = gen_offset_length(size);

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let bs = op.object(&path).range_read(offset..u32::MAX as u64).await?;
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
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read range content should match.
pub async fn test_reader_range(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let mut r = op
        .object(&path)
        .range_reader(offset..offset + length)
        .await?;

    let mut bs = Vec::new();
    r.read_to_end(&mut bs).await?;

    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!(
            "{:x}",
            Sha256::digest(&content[offset as usize..(offset + length) as usize])
        ),
        "read content"
    );

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read range from should match.
pub async fn test_reader_from(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, _) = gen_offset_length(size);

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let mut r = op.object(&path).range_reader(offset..).await?;

    let mut bs = Vec::new();
    r.read_to_end(&mut bs).await?;

    assert_eq!(bs.len(), size - offset as usize, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content[offset as usize..])),
        "read content"
    );

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read range tail should match.
pub async fn test_reader_tail(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (_, length) = gen_offset_length(size);

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let mut r = match op.object(&path).range_reader(..length).await {
        Ok(r) => r,
        // Not all services support range with tail range, let's tolerate this.
        Err(err) if err.kind() == ErrorKind::Unsupported => {
            warn!("service doesn't support range with tail");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    let mut bs = Vec::new();
    r.read_to_end(&mut bs).await?;

    assert_eq!(bs.len(), length as usize, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content[size - length as usize..])),
        "read content"
    );

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read not exist file should return NotFound
pub async fn test_read_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.object(&path).read().await;
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::ObjectNotFound);

    Ok(())
}

pub async fn test_fuzz_range_reader(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op
        .object(&path)
        .range_reader(0..content.len() as u64)
        .await?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectReaderAction::Read(size) => {
                let mut bs = vec![0; size];
                let n = o.read(&mut bs).await?;
                fuzzer.check_read(n, &bs[..n])
            }
            ObjectReaderAction::Seek(input_pos) => {
                let actual_pos = o.seek(input_pos).await?;
                fuzzer.check_seek(input_pos, actual_pos)
            }
            ObjectReaderAction::Next => {
                let actual_bs = o
                    .next()
                    .await
                    .map(|v| v.expect("next should not return error"));
                fuzzer.check_next(actual_bs)
            }
        }
    }

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

pub async fn test_fuzz_offset_reader(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op.object(&path).range_reader(0..).await?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectReaderAction::Read(size) => {
                let mut bs = vec![0; size];
                let n = o.read(&mut bs).await?;
                fuzzer.check_read(n, &bs[..n])
            }
            ObjectReaderAction::Seek(input_pos) => {
                let actual_pos = o.seek(input_pos).await?;
                fuzzer.check_seek(input_pos, actual_pos)
            }
            ObjectReaderAction::Next => {
                let actual_bs = o
                    .next()
                    .await
                    .map(|v| v.expect("next should not return error"));
                fuzzer.check_next(actual_bs)
            }
        }
    }

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

pub async fn test_fuzz_part_reader(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size);

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let mut fuzzer =
        ObjectReaderFuzzer::new(&path, content.clone(), offset as usize, length as usize);
    let mut o = op
        .object(&path)
        .range_reader(offset..offset + length)
        .await?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectReaderAction::Read(size) => {
                let mut bs = vec![0; size];
                let n = o.read(&mut bs).await?;
                fuzzer.check_read(n, &bs[..n])
            }
            ObjectReaderAction::Seek(input_pos) => {
                let actual_pos = o.seek(input_pos).await?;
                fuzzer.check_seek(input_pos, actual_pos)
            }
            ObjectReaderAction::Next => {
                let actual_bs = o
                    .next()
                    .await
                    .map(|v| v.expect("next should not return error"));
                fuzzer.check_next(actual_bs)
            }
        }
    }

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read with dir path should return an error.
pub async fn test_read_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.object(&path).create().await.expect("write must succeed");

    let result = op.object(&path).read().await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::ObjectIsADirectory);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Read file with special chars should succeed.
pub async fn test_read_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let bs = op.object(&path).read().await?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

// Delete existing file should succeed.
pub async fn test_delete(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    op.object(&path)
        .write(content)
        .await
        .expect("write must succeed");

    op.object(&path).delete().await?;

    // Stat it again to check.
    assert!(!op.object(&path).is_exist().await?);

    Ok(())
}

// Delete empty dir should succeed.
pub async fn test_delete_empty_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.object(&path)
        .create()
        .await
        .expect("create must succeed");

    op.object(&path).delete().await?;

    Ok(())
}

// Delete file with special chars should succeed.
pub async fn test_delete_with_special_chars(op: Operator) -> Result<()> {
    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    op.object(&path)
        .write(content)
        .await
        .expect("write must succeed");

    op.object(&path).delete().await?;

    // Stat it again to check.
    assert!(!op.object(&path).is_exist().await?);

    Ok(())
}

// Delete not existing file should also succeed.
pub async fn test_delete_not_existing(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    op.object(&path).delete().await?;

    Ok(())
}

// Delete via stream.
pub async fn test_delete_stream(op: Operator) -> Result<()> {
    let dir = uuid::Uuid::new_v4().to_string();
    op.object(&format!("{dir}/"))
        .create()
        .await
        .expect("creat must succeed");

    let expected: Vec<_> = (0..100).collect();
    for path in expected.iter() {
        op.object(&format!("{dir}/{path}")).create().await?;
    }

    op.batch()
        .with_limit(30)
        .remove_via(futures::stream::iter(expected.clone()).map(|v| format!("{dir}/{v}")))
        .await?;

    // Stat it again to check.
    for path in expected.iter() {
        assert!(
            !op.object(&format!("{dir}/{path}")).is_exist().await?,
            "{path} should be removed"
        )
    }

    Ok(())
}
