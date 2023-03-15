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
use futures::AsyncReadExt;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;

/// Test services that meet the following capability:
///
/// - can_read
/// - !can_write
macro_rules! behavior_read_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _read>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(false);
                        match op {
                            Some(op) if op.info().can_read() && !op.info().can_write() => $crate::read_only::$test(op).await,
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
macro_rules! behavior_read_tests {
     ($($service:ident),*) => {
        $(
            behavior_read_test!(
                $service,

                test_stat,
                test_stat_special_chars,
                test_stat_not_cleaned_path,
                test_stat_not_exist,
                test_stat_root,
                test_read_full,
                test_read_full_with_special_chars,
                test_read_range,
                test_reader_range,
                test_reader_from,
                test_reader_tail,
                test_read_not_exist,
                test_read_with_dir_path,
            );
        )*
    };
}

/// Stat normal file and dir should return metadata
pub async fn test_stat(op: Operator) -> Result<()> {
    let meta = op.stat("normal_file").await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 262144);

    let meta = op.stat("normal_dir/").await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    Ok(())
}

/// Stat special file and dir should return metadata
pub async fn test_stat_special_chars(op: Operator) -> Result<()> {
    let meta = op.stat("special_file  !@#$%^&()_+-=;',").await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 262144);

    let meta = op.stat("special_dir  !@#$%^&()_+-=;',/").await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    Ok(())
}

/// Stat not cleaned path should also succeed.
pub async fn test_stat_not_cleaned_path(op: Operator) -> Result<()> {
    let meta = op.stat("//normal_file").await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 262144);

    Ok(())
}

/// Stat not exist file should return NotFound
pub async fn test_stat_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.stat(&path).await;
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

/// Root should be able to stat and returns DIR.
pub async fn test_stat_root(op: Operator) -> Result<()> {
    let meta = op.stat("").await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    let meta = op.stat("/").await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    Ok(())
}

/// Read full content should match.
pub async fn test_read_full(op: Operator) -> Result<()> {
    let bs = op.read("normal_file").await?;
    assert_eq!(bs.len(), 262144, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "e7541d0f50d2d5c79dc41f28ccba8e0cdfbbc8c4b1aa1a0110184ef0ef67689f",
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub async fn test_read_full_with_special_chars(op: Operator) -> Result<()> {
    let bs = op.read("special_file  !@#$%^&()_+-=;',").await?;
    assert_eq!(bs.len(), 262144, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "e7541d0f50d2d5c79dc41f28ccba8e0cdfbbc8c4b1aa1a0110184ef0ef67689f",
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub async fn test_read_range(op: Operator) -> Result<()> {
    let bs = op.range_read("normal_file", 1024..2048).await?;
    assert_eq!(bs.len(), 1024, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "28786fb63abfe5545479e4f50da853652d1d67b88be5553c265ede4022774913",
        "read content"
    );

    Ok(())
}

/// Read range should match.
pub async fn test_reader_range(op: Operator) -> Result<()> {
    let mut r = op.range_reader("normal_file", 1024..2048).await?;

    let mut bs = Vec::new();
    r.read_to_end(&mut bs).await?;

    assert_eq!(bs.len(), 1024, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "28786fb63abfe5545479e4f50da853652d1d67b88be5553c265ede4022774913",
        "read content"
    );

    Ok(())
}

/// Read from should match.
pub async fn test_reader_from(op: Operator) -> Result<()> {
    let mut r = op.range_reader("normal_file", 261120..).await?;

    let mut bs = Vec::new();
    r.read_to_end(&mut bs).await?;

    assert_eq!(bs.len(), 1024, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "81fa400e85baa2a5c7006d77d4320b73d36222974b923e03ed9891580f989e2a",
        "read content"
    );

    Ok(())
}

/// Read tail should match.
pub async fn test_reader_tail(op: Operator) -> Result<()> {
    let mut r = op.range_reader("normal_file", ..1024).await?;

    let mut bs = Vec::new();
    r.read_to_end(&mut bs).await?;

    assert_eq!(bs.len(), 1024, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "81fa400e85baa2a5c7006d77d4320b73d36222974b923e03ed9891580f989e2a",
        "read content"
    );

    Ok(())
}

/// Read not exist file should return NotFound
pub async fn test_read_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.read(&path).await;
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

/// Read with dir path should return an error.
pub async fn test_read_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let result = op.read(&path).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}
