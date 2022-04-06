// Copyright 2022 Datafuse Labs.
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

//! `behavior` intents to provide behavior tests for all storage services.
//!
//! # Note
//!
//! `behavior` requires most of the logic is correct, especially `write` and `delete`. We will not depends service specific functions to prepare the fixtures.
//!
//! For examples, we depends `write` to create a file before testing `read`. If `write` doesn't works well, we can't test `read` correctly too.

use std::io;

use anyhow::Result;
use futures::StreamExt;
use log::debug;
use opendal::ObjectMode;
use opendal::Operator;
use opendal_test::services;
use sha2::Digest;
use sha2::Sha256;

use super::init_logger;
use super::utils::*;

/// Generate real test cases.
/// Update function list while changed.
macro_rules! behavior_tests {
    ($($service:ident),*) => {
        $(
            behavior_test!(
                $service,

                test_create_file,
                test_create_file_with_dir_path,
                test_create_dir,
                test_create_dir_with_file_path,

                test_write,
                test_write_with_dir_path,

                test_read_full,
                test_read_range,
                test_read_not_exist,
                test_read_with_dir_path,

                test_stat,
                test_stat_dir,
                test_stat_not_cleaned_path,
                test_stat_not_exist,
                test_stat_root,

                test_list_dir,
                test_list_dir_with_file_path,

                test_delete,
                test_delete_not_existing,
                test_delete_empty_dir,
            );
        )*
    };
}

macro_rules! behavior_test {
    ($service:ident, $($test:ident),*,) => {
        paste::item! {
            mod [<$service>] {
                use super::*;

                $(
                    #[tokio::test]
                    async fn [< $test >]() -> Result<()> {
                        init_logger();

                        let acc = super::services::$service::new().await?;
                        if acc.is_none() {
                            return Ok(())
                        }
                        super::$test(Operator::new(acc.unwrap())).await
                    }
                )*
            }
        }
    };
}

behavior_tests!(azblob, fs, memory, s3);

/// Create file with file path should succeed.
async fn test_create_file(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.object(&path).create_file().await?;
    assert_eq!(meta.path(), &path);
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Create file with dir path should return an error.
async fn test_create_file_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let meta = op.object(&path).create_file().await;
    assert!(meta.is_err());
    assert!(meta.unwrap_err().to_string().contains("is a directory"));

    Ok(())
}

/// Create dir with dir path should succeed.
async fn test_create_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let meta = op.object(&path).create_dir().await?;
    assert_eq!(meta.path(), &path);
    assert_eq!(meta.mode(), ObjectMode::DIR);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Create dir with file path should also succeed.
async fn test_create_dir_with_file_path(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.object(&path).create_dir().await?;
    assert_eq!(meta.path(), &path);
    assert_eq!(meta.mode(), ObjectMode::DIR);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Write a single file and test with stat.
async fn test_write(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    let _ = op.object(&path).write(&content).await?;

    let meta = op
        .object(&path)
        .metadata()
        .await
        .expect("stat must succeed");
    assert_eq!(meta.path(), path);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Write file with dir path should return an error
async fn test_write_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes();

    let result = op.object(&path).write(&content).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("is a directory"));

    Ok(())
}

/// Stat existing file should return metadata
async fn test_stat(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    let _ = op
        .object(&path)
        .write(&content)
        .await
        .expect("write must succeed");

    let meta = op.object(&path).metadata().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
async fn test_stat_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let _ = op
        .object(&path)
        .create_dir()
        .await
        .expect("write must succeed");

    let meta = op.object(&path).metadata().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);
    assert_eq!(meta.content_length(), 0);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat not cleaned path should also succeed.
async fn test_stat_not_cleaned_path(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    let _ = op
        .object(&path)
        .write(&content)
        .await
        .expect("write must succeed");

    let meta = op.object(&format!("//{}", &path)).metadata().await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat not exist file should return NotFound
async fn test_stat_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let meta = op.object(&path).metadata().await;
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), io::ErrorKind::NotFound);

    Ok(())
}

/// Root should be able to stat and returns DIR.
async fn test_stat_root(op: Operator) -> Result<()> {
    let meta = op.object("").metadata().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    let meta = op.object("/").metadata().await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    Ok(())
}

/// Read full content should match.
async fn test_read_full(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    let _ = op
        .object(&path)
        .write(&content)
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
async fn test_read_range(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, length) = gen_offset_length(size as usize);

    let _ = op
        .object(&path)
        .write(&content)
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

/// Read not exist file should return NotFound
async fn test_read_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.object(&path).read().await;
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), io::ErrorKind::NotFound);

    Ok(())
}

/// Read with dir path should return an error.
async fn test_read_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let _ = op
        .object(&path)
        .create_dir()
        .await
        .expect("write must succeed");

    let result = op.object(&path).read().await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("is a directory"));

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// List dir should return newly created file.
async fn test_list_dir(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    let _ = op
        .object(&path)
        .write(&content)
        .await
        .expect("write must succeed");

    let mut obs = op.objects("/").await?;
    let mut found = false;
    while let Some(o) = obs.next().await {
        let meta = o?.metadata().await?;
        if meta.path() == path {
            assert_eq!(meta.mode(), ObjectMode::FILE);
            assert_eq!(meta.content_length(), size as u64);

            found = true
        }
    }
    assert!(found, "file should be found in list");

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// List with path file should auto add / suffix.
async fn test_list_dir_with_file_path(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    let path = uuid::Uuid::new_v4().to_string();
    let full_path = format!("{}/{}", &parent, &path);
    let (content, size) = gen_bytes();

    let _ = op
        .object(&full_path)
        .write(&content)
        .await
        .expect("write must succeed");

    let mut obs = op.objects(&parent).await?;
    let mut found = false;
    while let Some(o) = obs.next().await {
        let meta = o?.metadata().await?;
        if meta.path() == full_path {
            assert_eq!(meta.mode(), ObjectMode::FILE);
            assert_eq!(meta.content_length(), size as u64);

            found = true
        }
    }
    assert!(found, "file should be found in list");

    op.object(&full_path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

// Delete existing file should succeed.
async fn test_delete(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes();

    let _ = op
        .object(&path)
        .write(&content)
        .await
        .expect("write must succeed");

    let _ = op.object(&path).delete().await?;

    // Stat it again to check.
    assert!(!op.object(&path).is_exist().await?);

    Ok(())
}

// Delete empty dir should succeed.
async fn test_delete_empty_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let _ = op
        .object(&path)
        .create_dir()
        .await
        .expect("create_dir must succeed");

    let _ = op.object(&path).delete().await?;

    // Stat it again to check.
    assert!(!op.object(&path).is_exist().await?);

    Ok(())
}

// Delete not existing file should also succeed.
async fn test_delete_not_existing(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let _ = op.object(&path).delete().await?;

    Ok(())
}
