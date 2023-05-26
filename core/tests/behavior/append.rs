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
use log::warn;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_append
macro_rules! behavior_append_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            $(
                #[test]
                $(
                    #[$meta]
                )*
                fn [<append_ $test >]() -> anyhow::Result<()> {
                    match OPERATOR.as_ref() {
                        Some(op) if op.info().can_read()
                            && op.info().can_write()
                            && op.info().can_append() => RUNTIME.block_on($crate::append::$test(op.clone())),
                        Some(_) => {
                            log::warn!("service {} doesn't support append, ignored", opendal::Scheme::$service);
                            Ok(())
                        },
                        None => {
                            Ok(())
                        }
                    }
                }
            )*
        }
    };
}

#[macro_export]
macro_rules! behavior_append_tests {
     ($($service:ident),*) => {
        $(
            behavior_append_test!(
                $service,

                test_append,
                test_append_with_dir_path,
                test_append_with_cache_control,
                test_append_with_content_type,
                test_append_with_content_disposition,

                test_append_to_normal,

                test_appender_futures_copy,
                test_fuzz_appender,
            );
        )*
    };
}

/// Test append to a file must success.
pub async fn test_append(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content_one, size_one) = gen_bytes();
    let (content_two, size_two) = gen_bytes();

    op.append(&path, content_one.clone())
        .await
        .expect("append file first time must success");

    op.append(&path, content_two.clone())
        .await
        .expect("append to an existing file must success");

    let bs = op.read(&path).await.expect("read file must success");

    assert_eq!(bs.len(), size_one + size_two);
    assert_eq!(bs[..size_one], content_one);
    assert_eq!(bs[size_one..], content_two);

    op.delete(&path).await.expect("delete file must success");

    Ok(())
}

/// Test append to a directory path must fail.
pub async fn test_append_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes();

    let res = op.append(&path, content).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}

/// Test append with cache control must success.
pub async fn test_append_with_cache_control(op: Operator) -> Result<()> {
    if !op.info().capability().append_with_cache_control {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    let target_cache_control = "no-cache, no-store, max-age=300";
    op.append_with(&path, content)
        .cache_control(target_cache_control)
        .await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(
        meta.cache_control().expect("cache control must exist"),
        target_cache_control
    );

    op.delete(&path).await.expect("delete must succeed");

    Ok(())
}

/// Test append with content type must success.
pub async fn test_append_with_content_type(op: Operator) -> Result<()> {
    if !op.info().capability().append_with_content_type {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    let target_content_type = "application/json";
    op.append_with(&path, content)
        .content_type(target_content_type)
        .await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(
        meta.content_type().expect("content type must exist"),
        target_content_type
    );
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");

    Ok(())
}

/// Write a single file with content disposition should succeed.
pub async fn test_append_with_content_disposition(op: Operator) -> Result<()> {
    if !op.info().capability().append_with_content_disposition {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes();

    let target_content_disposition = "attachment; filename=\"filename.jpg\"";
    op.append_with(&path, content)
        .content_disposition(target_content_disposition)
        .await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(
        meta.content_disposition().expect("content type must exist"),
        target_content_disposition
    );
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");

    Ok(())
}

/// If operator can't append to a file not created by append, it must fail.
pub async fn test_append_to_normal(op: Operator) -> Result<()> {
    if op.info().capability().append_normal {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes();

    op.write(&path, content.clone()).await?;

    let res = op.append(&path, content).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::Unexpected);

    op.delete(&path).await.expect("delete file must success");

    Ok(())
}

/// Copy data from reader to writer
pub async fn test_appender_futures_copy(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size): (Vec<u8>, usize) =
        gen_bytes_with_range(10 * 1024 * 1024..20 * 1024 * 1024);

    let mut a = match op.appender(&path).await {
        Ok(a) => a,
        Err(err) if err.kind() == ErrorKind::Unsupported => {
            warn!("service doesn't support write with append");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };

    futures::io::copy(&mut content.as_slice(), &mut a).await?;
    a.close().await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    let bs = op.read(&path).await?;
    assert_eq!(bs.len(), size, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[..size])),
        format!("{:x}", Sha256::digest(content)),
        "read content"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Test for fuzzing appender.
pub async fn test_fuzz_appender(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let mut fuzzer = ObjectWriterFuzzer::new(&path, None);

    let mut a = op.appender(&path).await?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectWriterAction::Write(bs) => {
                a.append(bs).await?;
            }
        }
    }
    a.close().await?;

    let content = op.read(&path).await?;
    fuzzer.check(&content);

    op.delete(&path).await.expect("delete file must success");

    Ok(())
}
