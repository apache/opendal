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

use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use bytes::Buf;
use bytes::Bytes;
use futures::io::BufReader;
use futures::io::Cursor;
use futures::stream;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use futures::StreamExt;
use http::StatusCode;
use log::debug;
use log::warn;
use reqwest::Url;
use sha2::Digest;
use sha2::Sha256;

use crate::*;

pub fn behavior_write_tests(op: &Operator) -> Vec<Trial> {
    let cap = op.info().full_capability();

    if !(cap.read && cap.write) {
        return vec![];
    }

    async_trials!(
        op,
        test_create_dir,
        test_create_dir_existing,
        test_write_only,
        test_write_with_empty_content,
        test_write_with_dir_path,
        test_write_with_special_chars,
        test_write_with_cache_control,
        test_write_with_content_type,
        test_write_with_content_disposition,
        test_stat_file,
        test_stat_dir,
        test_stat_nested_parent_dir,
        test_stat_with_special_chars,
        test_stat_not_cleaned_path,
        test_stat_not_exist,
        test_stat_with_if_match,
        test_stat_with_if_none_match,
        test_stat_with_override_cache_control,
        test_stat_with_override_content_disposition,
        test_stat_with_override_content_type,
        test_stat_root,
        test_read_full,
        test_read_range,
        test_read_large_range,
        test_reader_range,
        test_reader_from,
        test_reader_tail,
        test_read_not_exist,
        test_read_with_if_match,
        test_read_with_if_none_match,
        test_fuzz_reader_with_range,
        test_fuzz_offset_reader,
        test_fuzz_part_reader,
        test_read_with_dir_path,
        test_read_with_special_chars,
        test_read_with_override_cache_control,
        test_read_with_override_content_disposition,
        test_read_with_override_content_type,
        test_delete_file,
        test_delete_empty_dir,
        test_delete_with_special_chars,
        test_delete_not_existing,
        test_delete_stream,
        test_remove_one_file,
        test_writer_write,
        test_writer_sink,
        test_writer_copy,
        test_writer_abort,
        test_writer_futures_copy,
        test_fuzz_unsized_writer,
        test_invalid_reader_seek
    )
}

/// Create dir with dir path should succeed.
pub async fn test_create_dir(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).await?;

    let meta = op.stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Create dir on existing dir should succeed.
pub async fn test_create_dir_existing(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).await?;

    op.create_dir(&path).await?;

    let meta = op.stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Write a single file and test with stat.
pub async fn test_write_only(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content).await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Write a file with empty content.
pub async fn test_write_with_empty_content(op: Operator) -> Result<()> {
    if !op.info().full_capability().write_can_empty {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();

    op.write(&path, vec![]).await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), 0);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Write file with dir path should return an error
pub async fn test_write_with_dir_path(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes(op.info().full_capability());

    let result = op.write(&path, content).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}

/// Write a single file with special chars should succeed.
pub async fn test_write_with_special_chars(op: Operator) -> Result<()> {
    // Ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 is resolved");
        return Ok(());
    }
    // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
    if op.info().scheme() == opendal::Scheme::Atomicserver {
        warn!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
        return Ok(());
    }

    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content).await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Write a single file with cache control should succeed.
pub async fn test_write_with_cache_control(op: Operator) -> Result<()> {
    if !op.info().full_capability().write_with_cache_control {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    let target_cache_control = "no-cache, no-store, max-age=300";
    op.write_with(&path, content)
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

/// Write a single file with content type should succeed.
pub async fn test_write_with_content_type(op: Operator) -> Result<()> {
    if !op.info().full_capability().write_with_content_type {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes(op.info().full_capability());

    let target_content_type = "application/json";
    op.write_with(&path, content)
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
pub async fn test_write_with_content_disposition(op: Operator) -> Result<()> {
    if !op.info().full_capability().write_with_content_disposition {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes(op.info().full_capability());

    let target_content_disposition = "attachment; filename=\"filename.jpg\"";
    op.write_with(&path, content)
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

/// Stat existing file should return metadata
pub async fn test_stat_file(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content).await.expect("write must succeed");

    let meta = op.stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    // Stat a file with trailing slash should return `NotFound`.
    if op.info().full_capability().create_dir {
        let result = op.stat(&format!("{path}/")).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotFound);
    }

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Stat existing file should return metadata
pub async fn test_stat_dir(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).await.expect("write must succeed");

    let meta = op.stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    // Stat a dir without trailing slash could have two behavior.
    let result = op.stat(path.trim_end_matches('/')).await;
    match result {
        Ok(meta) => assert_eq!(meta.mode(), EntryMode::DIR),
        Err(err) => assert_eq!(err.kind(), ErrorKind::NotFound),
    }

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Stat the parent dir of existing dir should return metadata
pub async fn test_stat_nested_parent_dir(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let parent = format!("{}", uuid::Uuid::new_v4());
    let file = format!("{}", uuid::Uuid::new_v4());
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&format!("{parent}/{file}"), content.clone())
        .await
        .expect("write must succeed");

    let meta = op.stat(&format!("{parent}/")).await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&format!("{parent}/{file}"))
        .await
        .expect("delete must succeed");
    Ok(())
}

/// Stat existing file with special chars should return metadata
pub async fn test_stat_with_special_chars(op: Operator) -> Result<()> {
    // Ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 is resolved");
        return Ok(());
    }
    // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
    if op.info().scheme() == opendal::Scheme::Atomicserver {
        warn!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
        return Ok(());
    }

    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content).await.expect("write must succeed");

    let meta = op.stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Stat not cleaned path should also succeed.
pub async fn test_stat_not_cleaned_path(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content).await.expect("write must succeed");

    let meta = op.stat(&format!("//{}", &path)).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Stat not exist file should return NotFound
pub async fn test_stat_not_exist(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    // Stat not exist file should returns NotFound.
    let meta = op.stat(&path).await;
    assert!(meta.is_err());
    assert_eq!(meta.unwrap_err().kind(), ErrorKind::NotFound);

    // Stat not exist dir should also returns NotFound.
    if op.info().full_capability().create_dir {
        let meta = op.stat(&format!("{path}/")).await;
        assert!(meta.is_err());
        assert_eq!(meta.unwrap_err().kind(), ErrorKind::NotFound);
    }

    Ok(())
}

/// Stat with if_match should succeed, else get a ConditionNotMatch error.
pub async fn test_stat_with_if_match(op: Operator) -> Result<()> {
    if !op.info().full_capability().stat_with_if_match {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let meta = op.stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    let res = op.stat_with(&path).if_match("\"invalid_etag\"").await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let result = op
        .stat_with(&path)
        .if_match(meta.etag().expect("etag must exist"))
        .await;
    assert!(result.is_ok());

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Stat with if_none_match should succeed, else get a ConditionNotMatch.
pub async fn test_stat_with_if_none_match(op: Operator) -> Result<()> {
    if !op.info().full_capability().stat_with_if_none_match {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let meta = op.stat(&path).await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), size as u64);

    let res = op
        .stat_with(&path)
        .if_none_match(meta.etag().expect("etag must exist"))
        .await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let res = op
        .stat_with(&path)
        .if_none_match("\"invalid_etag\"")
        .await?;
    assert_eq!(res.mode(), meta.mode());
    assert_eq!(res.content_length(), meta.content_length());

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Stat file with override-cache-control should succeed.
pub async fn test_stat_with_override_cache_control(op: Operator) -> Result<()> {
    if !(op.info().full_capability().stat_with_override_cache_control
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let target_cache_control = "no-cache, no-store, must-revalidate";
    let signed_req = op
        .presign_stat_with(&path, Duration::from_secs(60))
        .override_cache_control(target_cache_control)
        .await
        .expect("sign must succeed");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }

    let resp = req.send().await.expect("send must succeed");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .expect("cache-control header must exist")
            .to_str()
            .expect("cache-control header must be string"),
        target_cache_control
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Stat file with override_content_disposition should succeed.
pub async fn test_stat_with_override_content_disposition(op: Operator) -> Result<()> {
    if !(op
        .info()
        .full_capability()
        .stat_with_override_content_disposition
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let target_content_disposition = "attachment; filename=foo.txt";

    let signed_req = op
        .presign_stat_with(&path, Duration::from_secs(60))
        .override_content_disposition(target_content_disposition)
        .await
        .expect("presign must succeed");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }

    let resp = req.send().await.expect("send must succeed");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(http::header::CONTENT_DISPOSITION)
            .expect("content-disposition header must exist")
            .to_str()
            .expect("content-disposition header must be string"),
        target_content_disposition
    );

    op.delete(&path).await.expect("delete must succeed");

    Ok(())
}

/// Stat file with override_content_type should succeed.
pub async fn test_stat_with_override_content_type(op: Operator) -> Result<()> {
    if !(op.info().full_capability().stat_with_override_content_type
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let target_content_type = "application/opendal";

    let signed_req = op
        .presign_stat_with(&path, Duration::from_secs(60))
        .override_content_type(target_content_type)
        .await
        .expect("presign must succeed");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }

    let resp = req.send().await.expect("send must succeed");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(http::header::CONTENT_TYPE)
            .expect("content-type header must exist")
            .to_str()
            .expect("content-type header must be string"),
        target_content_type
    );

    op.delete(&path).await.expect("delete must succeed");

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
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let bs = op.read(&path).await?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read range content should match.
pub async fn test_read_range(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let bs = op.read_with(&path).range(offset..offset + length).await?;
    assert_eq!(bs.len() as u64, length, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!(
            "{:x}",
            Sha256::digest(&content[offset as usize..(offset + length) as usize])
        ),
        "read content"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read large range content should match.
pub async fn test_read_large_range(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());
    let (offset, _) = gen_offset_length(size);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let bs = op.read_with(&path).range(offset..u32::MAX as u64).await?;
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

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read range content should match.
pub async fn test_reader_range(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let mut r = op.reader_with(&path).range(offset..offset + length).await?;

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

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read range from should match.
pub async fn test_reader_from(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());
    let (offset, _) = gen_offset_length(size);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let mut r = op.reader_with(&path).range(offset..).await?;

    let mut bs = Vec::new();
    r.read_to_end(&mut bs).await?;

    assert_eq!(bs.len(), size - offset as usize, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content[offset as usize..])),
        "read content"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read range tail should match.
pub async fn test_reader_tail(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());
    let (_, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let mut r = match op.reader_with(&path).range(..length).await {
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

    op.delete(&path).await.expect("delete must succeed");
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

/// Read with if_match should match, else get a ConditionNotMatch error.
pub async fn test_read_with_if_match(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_if_match {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let meta = op.stat(&path).await?;

    let res = op.read_with(&path).if_match("\"invalid_etag\"").await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let bs = op
        .read_with(&path)
        .if_match(meta.etag().expect("etag must exist"))
        .await
        .expect("read must succeed");
    assert_eq!(bs, content);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read with if_none_match should match, else get a ConditionNotMatch error.
pub async fn test_read_with_if_none_match(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_if_none_match {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let meta = op.stat(&path).await?;

    let res = op
        .read_with(&path)
        .if_none_match(meta.etag().expect("etag must exist"))
        .await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let bs = op
        .read_with(&path)
        .if_none_match("\"invalid_etag\"")
        .await
        .expect("read must succeed");
    assert_eq!(bs, content);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

pub async fn test_fuzz_reader_with_range(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op.reader_with(&path).range(0..content.len() as u64).await?;

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

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

pub async fn test_fuzz_offset_reader(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let mut fuzzer = ObjectReaderFuzzer::new(&path, content.clone(), 0, content.len());
    let mut o = op.reader_with(&path).range(0..).await?;

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

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

pub async fn test_fuzz_part_reader(op: Operator) -> Result<()> {
    if !op.info().full_capability().read_with_range {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let mut fuzzer =
        ObjectReaderFuzzer::new(&path, content.clone(), offset as usize, length as usize);
    let mut o = op.reader_with(&path).range(offset..offset + length).await?;

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

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read with dir path should return an error.
pub async fn test_read_with_dir_path(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).await.expect("write must succeed");

    let result = op.read(&path).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read file with special chars should succeed.
pub async fn test_read_with_special_chars(op: Operator) -> Result<()> {
    // Ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 is resolved");
        return Ok(());
    }
    // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
    if op.info().scheme() == opendal::Scheme::Atomicserver {
        warn!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
        return Ok(());
    }

    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let bs = op.read(&path).await?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read file with override-cache-control should succeed.
pub async fn test_read_with_override_cache_control(op: Operator) -> Result<()> {
    if !(op.info().full_capability().read_with_override_cache_control
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let target_cache_control = "no-cache, no-store, must-revalidate";
    let signed_req = op
        .presign_read_with(&path, Duration::from_secs(60))
        .override_cache_control(target_cache_control)
        .await
        .expect("sign must succeed");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }

    let resp = req.send().await.expect("send must succeed");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("cache-control")
            .expect("cache-control header must exist")
            .to_str()
            .expect("cache-control header must be string"),
        target_cache_control
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Read file with override_content_disposition should succeed.
pub async fn test_read_with_override_content_disposition(op: Operator) -> Result<()> {
    if !(op
        .info()
        .full_capability()
        .read_with_override_content_disposition
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let target_content_disposition = "attachment; filename=foo.txt";

    let signed_req = op
        .presign_read_with(&path, Duration::from_secs(60))
        .override_content_disposition(target_content_disposition)
        .await
        .expect("presign must succeed");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }

    let resp = req.send().await.expect("send must succeed");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(http::header::CONTENT_DISPOSITION)
            .expect("content-disposition header must exist")
            .to_str()
            .expect("content-disposition header must be string"),
        target_content_disposition
    );
    assert_eq!(resp.bytes().await?, content);

    op.delete(&path).await.expect("delete must succeed");

    Ok(())
}

/// Read file with override_content_type should succeed.
pub async fn test_read_with_override_content_type(op: Operator) -> Result<()> {
    if !(op.info().full_capability().read_with_override_content_type
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let target_content_type = "application/opendal";

    let signed_req = op
        .presign_read_with(&path, Duration::from_secs(60))
        .override_content_type(target_content_type)
        .await
        .expect("presign must succeed");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }

    let resp = req.send().await.expect("send must succeed");

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(http::header::CONTENT_TYPE)
            .expect("content-type header must exist")
            .to_str()
            .expect("content-type header must be string"),
        target_content_type
    );
    assert_eq!(resp.bytes().await?, content);

    op.delete(&path).await.expect("delete must succeed");

    Ok(())
}

/// Delete existing file should succeed.
pub async fn test_writer_abort(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    let mut writer = match op.writer(&path).await {
        Ok(writer) => writer,
        Err(e) => {
            assert_eq!(e.kind(), ErrorKind::Unsupported);
            return Ok(());
        }
    };

    if let Err(e) = writer.write(content).await {
        assert_eq!(e.kind(), ErrorKind::Unsupported);
        return Ok(());
    }

    if let Err(e) = writer.abort().await {
        assert_eq!(e.kind(), ErrorKind::Unsupported);
        return Ok(());
    }

    // Aborted writer should not write actual file.
    assert!(!op.is_exist(&path).await?);
    Ok(())
}

/// Delete existing file should succeed.
pub async fn test_delete_file(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content).await.expect("write must succeed");

    op.delete(&path).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

    Ok(())
}

/// Delete empty dir should succeed.
pub async fn test_delete_empty_dir(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).await.expect("create must succeed");

    op.delete(&path).await?;

    Ok(())
}

/// Delete file with special chars should succeed.
pub async fn test_delete_with_special_chars(op: Operator) -> Result<()> {
    // Ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 addressed.
    if op.info().scheme() == opendal::Scheme::Supabase {
        warn!("ignore test for supabase until https://github.com/apache/incubator-opendal/issues/2194 is resolved");
        return Ok(());
    }
    // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
    if op.info().scheme() == opendal::Scheme::Atomicserver {
        warn!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
        return Ok(());
    }

    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content).await.expect("write must succeed");

    op.delete(&path).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

    Ok(())
}

/// Delete not existing file should also succeed.
pub async fn test_delete_not_existing(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    op.delete(&path).await?;

    Ok(())
}

/// Remove one file
pub async fn test_remove_one_file(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    op.remove(vec![path.clone()]).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

    op.write(&format!("/{path}"), content)
        .await
        .expect("write must succeed");

    op.remove(vec![path.clone()]).await?;

    // Stat it again to check.
    assert!(!op.is_exist(&path).await?);

    Ok(())
}

/// Delete via stream.
pub async fn test_delete_stream(op: Operator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let dir = uuid::Uuid::new_v4().to_string();
    op.create_dir(&format!("{dir}/"))
        .await
        .expect("creat must succeed");

    let expected: Vec<_> = (0..100).collect();
    for path in expected.iter() {
        op.write(&format!("{dir}/{path}"), "delete_stream").await?;
    }

    op.with_limit(30)
        .remove_via(futures::stream::iter(expected.clone()).map(|v| format!("{dir}/{v}")))
        .await?;

    // Stat it again to check.
    for path in expected.iter() {
        assert!(
            !op.is_exist(&format!("{dir}/{path}")).await?,
            "{path} should be removed"
        )
    }

    Ok(())
}

/// Append data into writer
pub async fn test_writer_write(op: Operator) -> Result<()> {
    if !(op.info().full_capability().write_can_multi) {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let size = 5 * 1024 * 1024; // write file with 5 MiB
    let content_a = gen_fixed_bytes(size);
    let content_b = gen_fixed_bytes(size);

    let mut w = op.writer(&path).await?;
    w.write(content_a.clone()).await?;
    w.write(content_b.clone()).await?;
    w.close().await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), (size * 2) as u64);

    let bs = op.read(&path).await?;
    assert_eq!(bs.len(), size * 2, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[..size])),
        format!("{:x}", Sha256::digest(content_a)),
        "read content a"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[size..])),
        format!("{:x}", Sha256::digest(content_b)),
        "read content b"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Streaming data into writer
pub async fn test_writer_sink(op: Operator) -> Result<()> {
    let cap = op.info().full_capability();
    if !(cap.write && cap.write_can_multi) {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let size = 5 * 1024 * 1024; // write file with 5 MiB
    let content_a = gen_fixed_bytes(size);
    let content_b = gen_fixed_bytes(size);
    let stream = stream::iter(vec![content_a.clone(), content_b.clone()]).map(Ok);

    let mut w = op.writer_with(&path).buffer(5 * 1024 * 1024).await?;
    w.sink(stream).await?;
    w.close().await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), (size * 2) as u64);

    let bs = op.read(&path).await?;
    assert_eq!(bs.len(), size * 2, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[..size])),
        format!("{:x}", Sha256::digest(content_a)),
        "read content a"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[size..])),
        format!("{:x}", Sha256::digest(content_b)),
        "read content b"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Reading data into writer
pub async fn test_writer_copy(op: Operator) -> Result<()> {
    let cap = op.info().full_capability();
    if !(cap.write && cap.write_can_multi) {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let size = 5 * 1024 * 1024; // write file with 5 MiB
    let content_a = gen_fixed_bytes(size);
    let content_b = gen_fixed_bytes(size);

    let mut w = op.writer_with(&path).buffer(5 * 1024 * 1024).await?;

    let mut content = Bytes::from([content_a.clone(), content_b.clone()].concat());
    while !content.is_empty() {
        let reader = Cursor::new(content.clone());
        let n = w.copy(reader).await?;
        content.advance(n as usize);
    }
    w.close().await?;

    let meta = op.stat(&path).await.expect("stat must succeed");
    assert_eq!(meta.content_length(), (size * 2) as u64);

    let bs = op.read(&path).await?;
    assert_eq!(bs.len(), size * 2, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[..size])),
        format!("{:x}", Sha256::digest(content_a)),
        "read content a"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs[size..])),
        format!("{:x}", Sha256::digest(content_b)),
        "read content b"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// Copy data from reader to writer
pub async fn test_writer_futures_copy(op: Operator) -> Result<()> {
    if !(op.info().full_capability().write_can_multi) {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    let (content, size): (Vec<u8>, usize) =
        gen_bytes_with_range(10 * 1024 * 1024..20 * 1024 * 1024);

    let mut w = op.writer_with(&path).buffer(8 * 1024 * 1024).await?;

    // Wrap a buf reader here to make sure content is read in 1MiB chunks.
    let mut cursor = BufReader::with_capacity(1024 * 1024, Cursor::new(content.clone()));
    futures::io::copy_buf(&mut cursor, &mut w).await?;
    w.close().await?;

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

/// Add test for unsized writer
pub async fn test_fuzz_unsized_writer(op: Operator) -> Result<()> {
    if !op.info().full_capability().write_can_multi {
        warn!("{op:?} doesn't support write without content length, test skip");
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();

    let mut fuzzer = ObjectWriterFuzzer::new(&path, None);

    let mut w = op.writer_with(&path).buffer(8 * 1024 * 1024).await?;

    for _ in 0..100 {
        match fuzzer.fuzz() {
            ObjectWriterAction::Write(bs) => w.write(bs).await?,
        }
    }
    w.close().await?;

    let content = op.read(&path).await?;
    fuzzer.check(&content);

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// seeking a negative position should return a InvalidInput error
pub async fn test_invalid_reader_seek(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let mut r = op.reader(&path).await?;
    let res = r.seek(std::io::SeekFrom::Current(-1024)).await;

    assert!(res.is_err());

    assert_eq!(
        res.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput,
        "seeking a negative position should return a InvalidInput error"
    );

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}
