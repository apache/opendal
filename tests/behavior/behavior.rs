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

use std::io::ErrorKind;

use super::init_logger;
use super::utils::*;
use anyhow::Result;
use futures::AsyncReadExt;
use futures::StreamExt;
use opendal::ObjectMode;
use opendal::Operator;
use opendal_test::services;
use sha2::Digest;
use sha2::Sha256;

/// Generate real test cases.
/// Update function list while changed.
macro_rules! behavior_tests {
    ($($service:ident),*) => {
        $(
            behavior_test!($service, test_normal, test_stat_root, test_stat_non_exist);
        )*
    };
}

macro_rules! behavior_test {
    ($service:ident, $($test:ident),*) => {
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

/// This case is use to test service's normal behavior.
async fn test_normal(op: Operator) -> Result<()> {
    // Step 1: Generate a random file with random size (under 4 MB).
    let path = uuid::Uuid::new_v4().to_string();
    println!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    // Step 2: Write this file
    let _ = op.object(&path).write(&content).await?;

    // Step 3: Stat this file
    let meta = op.object(&path).metadata().await?;
    assert_eq!(meta.content_length(), size as u64, "stat file");

    // Step 3.1: Stat this file start with "//" should works
    let meta = op.object(&format!("//{}", &path)).metadata().await?;
    assert_eq!(meta.content_length(), size as u64, "stat file");

    // Step 4: Read this file's content
    // Step 4.1: Read the whole file.
    let mut buf = Vec::new();
    let mut r = op.object(&path).reader().await?;
    let n = r.read_to_end(&mut buf).await.expect("read to end");
    assert_eq!(n, size as usize, "check size in read whole file");
    assert_eq!(
        format!("{:x}", Sha256::digest(&buf)),
        format!("{:x}", Sha256::digest(&content)),
        "check hash in read whole file"
    );

    // Step 4.2: Read the file with random offset and length.
    let (offset, length) = gen_offset_length(size as usize);
    let mut buf: Vec<u8> = vec![0; length as usize];
    let mut r = op.object(&path).range_reader(offset..).await?;
    r.read_exact(&mut buf).await?;
    assert_eq!(
        format!("{:x}", Sha256::digest(&buf)),
        format!(
            "{:x}",
            Sha256::digest(&content[offset as usize..(offset + length) as usize])
        ),
        "read part file"
    );

    // Step 5: List this dir, we should get this file.
    let mut obs = op.objects("").await?.map(|o| o.expect("list object"));
    let mut found = false;
    while let Some(o) = obs.next().await {
        let meta = o.metadata().await?;
        if meta.path() == path {
            let mode = meta.mode();
            assert_eq!(mode, ObjectMode::FILE);

            found = true
        }
    }
    assert!(found, "file should be found in iterator");

    // Step 6: Delete this file
    let result = op.object(&path).delete().await;
    assert!(result.is_ok(), "delete file: {}", result.unwrap_err());

    // Step 7: Stat this file again to check if it's deleted
    let o = op.object(&path);
    let exist = o.is_exist().await?;
    assert!(!exist, "stat file again");
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

/// Stat non exist object should return ObjectNotExist.
async fn test_stat_non_exist(op: Operator) -> Result<()> {
    let meta = op
        .object(&uuid::Uuid::new_v4().to_string())
        .metadata()
        .await;
    assert!(meta.is_err());
    let err = meta.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    Ok(())
}
