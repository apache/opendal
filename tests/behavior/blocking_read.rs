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
use opendal::BlockingOperator;
use opendal::EntryMode;
use opendal::ErrorKind;
use sha2::Digest;
use sha2::Sha256;

/// Test services that meet the following capability:
///
/// - can_read
/// - !can_write
/// - can_blocking
macro_rules! behavior_blocking_read_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _blocking_read>] {
                $(
                    #[test]
                    $(
                        #[$meta]
                    )*
                    fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op) if op.info().can_read()
                                && !op.info().can_write()
                                && op.info().can_blocking() => $crate::blocking_read::$test(op.blocking()),
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
macro_rules! behavior_blocking_read_tests {
     ($($service:ident),*) => {
        $(
            behavior_blocking_read_test!(
                $service,

                test_stat,
                test_stat_special_chars,
                test_stat_not_exist,
                test_read_full,
                test_read_range,
                test_read_not_exist,
            );
        )*
    };
}

/// Stat normal file and dir should return metadata
pub fn test_stat(op: BlockingOperator) -> Result<()> {
    let meta = op.stat("normal_file")?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 262144);

    let meta = op.stat("normal_dir/")?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    Ok(())
}

/// Stat special file and dir should return metadata
pub fn test_stat_special_chars(op: BlockingOperator) -> Result<()> {
    let meta = op.stat("special_file  !@#$%^&()_+-=;',")?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 262144);

    let meta = op.stat("special_dir  !@#$%^&()_+-=;',/")?;
    assert_eq!(meta.mode(), EntryMode::DIR);

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
    let bs = op.read("normal_file")?;
    assert_eq!(bs.len(), 262144, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "e7541d0f50d2d5c79dc41f28ccba8e0cdfbbc8c4b1aa1a0110184ef0ef67689f",
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub fn test_read_range(op: BlockingOperator) -> Result<()> {
    let bs = op.range_read("normal_file", 1024..2048)?;
    assert_eq!(bs.len(), 1024, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "28786fb63abfe5545479e4f50da853652d1d67b88be5553c265ede4022774913",
        "read content"
    );

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
