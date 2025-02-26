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

use crate::*;
use futures::AsyncReadExt;
use futures::TryStreamExt;
use http::StatusCode;
use log::warn;
use reqwest::Url;
use sha2::Digest;
use sha2::Sha256;
use tokio::time::sleep;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.read && cap.write {
        tests.extend(async_trials!(
            op,
            test_read_full,
            test_read_range,
            test_reader,
            test_reader_with_if_match,
            test_reader_with_if_none_match,
            test_reader_with_if_modified_since,
            test_reader_with_if_unmodified_since,
            test_read_not_exist,
            test_read_with_if_match,
            test_read_with_if_none_match,
            test_read_with_if_modified_since,
            test_read_with_if_unmodified_since,
            test_read_with_dir_path,
            test_read_with_special_chars,
            test_read_with_override_cache_control,
            test_read_with_override_content_disposition,
            test_read_with_override_content_type,
            test_read_with_version,
            test_read_with_not_existing_version
        ))
    }

    if cap.read && !cap.write {
        tests.extend(async_trials!(
            op,
            test_read_only_read_full,
            test_read_only_read_full_with_special_chars,
            test_read_only_read_with_range,
            test_read_only_read_not_exist,
            test_read_only_read_with_dir_path,
            test_read_only_read_with_if_match,
            test_read_only_read_with_if_none_match,
            test_reader_only_read_with_if_match,
            test_reader_only_read_with_if_none_match
        ))
    }
}

/// Read full content should match.
pub async fn test_read_full(op: Operator) -> anyhow::Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let bs = op.read(&path).await?.to_bytes();
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    Ok(())
}

/// Read range content should match.
pub async fn test_read_range(op: Operator) -> anyhow::Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(op.clone());
    let (offset, length) = gen_offset_length(size);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let bs = op
        .read_with(&path)
        .range(offset..offset + length)
        .await?
        .to_bytes();
    assert_eq!(bs.len() as u64, length, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!(
            "{:x}",
            Sha256::digest(&content[offset as usize..(offset + length) as usize])
        ),
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub async fn test_reader(op: Operator) -> anyhow::Result<()> {
    let (path, content, size) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    // Reader.
    let bs = op.reader(&path).await?.read(..).await?.to_bytes();
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    // Bytes Stream
    let bs = op
        .reader(&path)
        .await?
        .into_bytes_stream(..)
        .await?
        .try_fold(Vec::new(), |mut acc, chunk| {
            acc.extend_from_slice(&chunk);
            async { Ok(acc) }
        })
        .await?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    // Futures Reader
    let mut futures_reader = op
        .reader(&path)
        .await?
        .into_futures_async_read(0..size as u64)
        .await?;
    let mut bs = Vec::new();
    futures_reader.read_to_end(&mut bs).await?;
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    Ok(())
}

/// Read not exist file should return NotFound
pub async fn test_read_not_exist(op: Operator) -> anyhow::Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.read(&path).await;
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

/// Reader with if_match should match, else get a ConditionNotMatch error.
pub async fn test_reader_with_if_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_match {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let meta = op.stat(&path).await?;

    let reader = op.reader_with(&path).if_match("\"invalid_etag\"").await?;
    let res = reader.read(..).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let reader = op
        .reader_with(&path)
        .if_match(meta.etag().expect("etag must exist"))
        .await?;

    let bs = reader.read(..).await.expect("read must succeed").to_bytes();
    assert_eq!(bs, content);

    Ok(())
}

/// Read with if_match should match, else get a ConditionNotMatch error.
pub async fn test_read_with_if_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_match {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

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
        .expect("read must succeed")
        .to_bytes();
    assert_eq!(bs, content);

    Ok(())
}

/// Reader with if_none_match should match, else get a ConditionNotMatch error.
pub async fn test_reader_with_if_none_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_none_match {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let meta = op.stat(&path).await?;

    let reader = op
        .reader_with(&path)
        .if_none_match(meta.etag().expect("etag must exist"))
        .await?;
    let res = reader.read(..).await;

    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let reader = op
        .reader_with(&path)
        .if_none_match("\"invalid_etag\"")
        .await?;
    let bs = reader.read(..).await.expect("read must succeed").to_bytes();
    assert_eq!(bs, content);

    Ok(())
}

/// Reader with if_modified_since should match, otherwise, a ConditionNotMatch error will be returned.
pub async fn test_reader_with_if_modified_since(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_modified_since {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");
    let last_modified_time = op.stat(&path).await?.last_modified().unwrap();

    let since = last_modified_time - chrono::Duration::seconds(1);
    let reader = op.reader_with(&path).if_modified_since(since).await?;
    let bs = reader.read(..).await?.to_bytes();
    assert_eq!(bs, content);

    sleep(Duration::from_secs(1)).await;

    let since = last_modified_time + chrono::Duration::seconds(1);
    let reader = op.reader_with(&path).if_modified_since(since).await?;
    let res = reader.read(..).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    Ok(())
}

/// Reader with if_unmodified_since should match, otherwise, a ConditionNotMatch error will be returned.
pub async fn test_reader_with_if_unmodified_since(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_unmodified_since {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");
    let last_modified_time = op.stat(&path).await?.last_modified().unwrap();

    let since = last_modified_time - chrono::Duration::seconds(1);
    let reader = op.reader_with(&path).if_unmodified_since(since).await?;
    let res = reader.read(..).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    sleep(Duration::from_secs(1)).await;

    let since = last_modified_time + chrono::Duration::seconds(1);
    let reader = op.reader_with(&path).if_unmodified_since(since).await?;
    let bs = reader.read(..).await?.to_bytes();
    assert_eq!(bs, content);

    Ok(())
}

/// Read with if_none_match should match, else get a ConditionNotMatch error.
pub async fn test_read_with_if_none_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_none_match {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

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
        .expect("read must succeed")
        .to_bytes();
    assert_eq!(bs, content);

    Ok(())
}

/// Read with dir path should return an error.
pub async fn test_read_with_dir_path(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = TEST_FIXTURE.new_dir_path();

    op.create_dir(&path).await.expect("write must succeed");

    let result = op.read(&path).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}

/// Read file with special chars should succeed.
pub async fn test_read_with_special_chars(op: Operator) -> anyhow::Result<()> {
    // Ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 addressed.
    if op.info().scheme() == opendal::Scheme::Atomicserver {
        warn!("ignore test for atomicserver until https://github.com/atomicdata-dev/atomic-server/issues/663 is resolved");
        return Ok(());
    }

    let path = format!("{} !@#$%^&()_+-=;',.txt", uuid::Uuid::new_v4());
    let (path, content, size) = TEST_FIXTURE.new_file_with_path(op.clone(), &path);

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");

    let bs = op.read(&path).await?.to_bytes();
    assert_eq!(size, bs.len(), "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content)),
        "read content"
    );

    Ok(())
}

/// Read file with override-cache-control should succeed.
pub async fn test_read_with_override_cache_control(op: Operator) -> anyhow::Result<()> {
    if !(op.info().full_capability().read_with_override_cache_control
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

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

    Ok(())
}

/// Read file with override_content_disposition should succeed.
pub async fn test_read_with_override_content_disposition(op: Operator) -> anyhow::Result<()> {
    if !(op
        .info()
        .full_capability()
        .read_with_override_content_disposition
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

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

    Ok(())
}

/// Read file with override_content_type should succeed.
pub async fn test_read_with_override_content_type(op: Operator) -> anyhow::Result<()> {
    if !(op.info().full_capability().read_with_override_content_type
        && op.info().full_capability().presign)
    {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

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

    Ok(())
}

/// Read with if_modified_since should match, otherwise, a ConditionNotMatch error will be returned.
pub async fn test_read_with_if_modified_since(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_modified_since {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");
    let last_modified_time = op.stat(&path).await?.last_modified().unwrap();

    let since = last_modified_time - chrono::Duration::seconds(1);
    let bs = op
        .read_with(&path)
        .if_modified_since(since)
        .await?
        .to_bytes();
    assert_eq!(bs, content);

    sleep(Duration::from_secs(1)).await;

    let since = last_modified_time + chrono::Duration::seconds(1);
    let res = op.read_with(&path).if_modified_since(since).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    Ok(())
}

/// Read with if_unmodified_since should match, otherwise, a ConditionNotMatch error will be returned.
pub async fn test_read_with_if_unmodified_since(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_unmodified_since {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());

    op.write(&path, content.clone())
        .await
        .expect("write must succeed");
    let last_modified = op.stat(&path).await?.last_modified().unwrap();

    let since = last_modified - chrono::Duration::seconds(3600);
    let res = op.read_with(&path).if_unmodified_since(since).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    sleep(Duration::from_secs(1)).await;

    let since = last_modified + chrono::Duration::seconds(1);
    let bs = op
        .read_with(&path)
        .if_unmodified_since(since)
        .await?
        .to_bytes();
    assert_eq!(bs, content);

    Ok(())
}

/// Read full content should match.
pub async fn test_read_only_read_full(op: Operator) -> anyhow::Result<()> {
    let bs = op.read("normal_file.txt").await?.to_bytes();
    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5",
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub async fn test_read_only_read_full_with_special_chars(op: Operator) -> anyhow::Result<()> {
    let bs = op
        .read("special_file  !@#$%^&()_+-=;',.txt")
        .await?
        .to_bytes();
    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5",
        "read content"
    );

    Ok(())
}

/// Read full content should match.
pub async fn test_read_only_read_with_range(op: Operator) -> anyhow::Result<()> {
    let bs = op
        .read_with("normal_file.txt")
        .range(1024..2048)
        .await?
        .to_bytes();
    assert_eq!(bs.len(), 1024, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "330c6d57fdc1119d6021b37714ca5ad0ede12edd484f66be799a5cff59667034",
        "read content"
    );

    Ok(())
}

/// Read not exist file should return NotFound
pub async fn test_read_only_read_not_exist(op: Operator) -> anyhow::Result<()> {
    let path = uuid::Uuid::new_v4().to_string();

    let bs = op.read(&path).await;
    assert!(bs.is_err());
    assert_eq!(bs.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}

/// Read with dir path should return an error.
pub async fn test_read_only_read_with_dir_path(op: Operator) -> anyhow::Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    let result = op.read(&path).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), ErrorKind::IsADirectory);

    Ok(())
}

/// Reader with if_match should match, else get a ConditionNotMatch error.
pub async fn test_reader_only_read_with_if_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_match {
        return Ok(());
    }

    let path = "normal_file.txt";

    let meta = op.stat(path).await?;

    let reader = op.reader_with(path).if_match("invalid_etag").await?;
    let res = reader.read(..).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let reader = op
        .reader_with(path)
        .if_match(meta.etag().expect("etag must exist"))
        .await?;

    let bs = reader.read(..).await.expect("read must succeed").to_bytes();

    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5",
        "read content"
    );

    Ok(())
}

/// Read with if_match should match, else get a ConditionNotMatch error.
pub async fn test_read_only_read_with_if_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_match {
        return Ok(());
    }

    let path = "normal_file.txt";

    let meta = op.stat(path).await?;

    let res = op.read_with(path).if_match("invalid_etag").await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let bs = op
        .read_with(path)
        .if_match(meta.etag().expect("etag must exist"))
        .await
        .expect("read must succeed")
        .to_bytes();
    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5",
        "read content"
    );

    Ok(())
}

/// Reader with if_none_match should match, else get a ConditionNotMatch error.
pub async fn test_reader_only_read_with_if_none_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_none_match {
        return Ok(());
    }

    let path = "normal_file.txt";

    let meta = op.stat(path).await?;

    let reader = op
        .reader_with(path)
        .if_none_match(meta.etag().expect("etag must exist"))
        .await?;

    let res = reader.read(..).await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let reader = op.reader_with(path).if_none_match("invalid_etag").await?;
    let bs = reader.read(..).await.expect("read must succeed").to_bytes();

    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5",
        "read content"
    );

    Ok(())
}

/// Read with if_none_match should match, else get a ConditionNotMatch error.
pub async fn test_read_only_read_with_if_none_match(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_if_none_match {
        return Ok(());
    }

    let path = "normal_file.txt";

    let meta = op.stat(path).await?;

    let res = op
        .read_with(path)
        .if_none_match(meta.etag().expect("etag must exist"))
        .await;
    assert!(res.is_err());
    assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);

    let bs = op
        .read_with(path)
        .if_none_match("invalid_etag")
        .await
        .expect("read must succeed")
        .to_bytes();
    assert_eq!(bs.len(), 30482, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        "943048ba817cdcd786db07d1f42d5500da7d10541c2f9353352cd2d3f66617e5",
        "read content"
    );

    Ok(())
}

pub async fn test_read_with_version(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_version {
        return Ok(());
    }

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(path.as_str(), content.clone())
        .await
        .expect("write must success");
    let meta = op.stat(path.as_str()).await.expect("stat must success");
    let version = meta.version().expect("must have version");

    let data = op
        .read_with(path.as_str())
        .version(version)
        .await
        .expect("read must success");
    assert_eq!(content, data.to_vec());

    op.write(path.as_str(), "1")
        .await
        .expect("write must success");

    // After writing new data, we can still read the first version data
    let second_data = op
        .read_with(path.as_str())
        .version(version)
        .await
        .expect("read must success");
    assert_eq!(content, second_data.to_vec());

    Ok(())
}

pub async fn test_read_with_not_existing_version(op: Operator) -> anyhow::Result<()> {
    if !op.info().full_capability().read_with_version {
        return Ok(());
    }

    // retrieve a valid version
    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(path.as_str(), content.clone())
        .await
        .expect("write must success");
    let version = op
        .stat(path.as_str())
        .await
        .expect("stat must success")
        .version()
        .expect("must have version")
        .to_string();

    let (path, content, _) = TEST_FIXTURE.new_file(op.clone());
    op.write(path.as_str(), content)
        .await
        .expect("write must success");
    let ret = op.read_with(path.as_str()).version(&version).await;
    assert!(ret.is_err());
    assert_eq!(ret.unwrap_err().kind(), ErrorKind::NotFound);

    Ok(())
}
