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
//! `behavior` requires most of the logic is correct, especially `write` and `delete`. We will not depend on service specific functions to prepare the fixtures.
//!
//! For examples, we depend on `write` to create a file before testing `read`. If `write` doesn't work well, we can't test `read` correctly too.

use std::collections::HashMap;
use std::io;

use anyhow::Result;
use futures::TryStreamExt;
use http::header;
use http::header::ETAG;
use isahc::AsyncReadResponseExt;
use log::debug;
use opendal::layers::*;
use opendal::ObjectMode;
use opendal::ObjectPart;
use opendal::Operator;
use opendal::Scheme::*;
use sha2::Digest;
use sha2::Sha256;
use time::Duration;

use super::init_logger;
use super::utils::*;

/// Generate real test cases.
/// Update function list while changed.
macro_rules! behavior_tests {
    ($($service:ident),*) => {
        $(
            behavior_test!(
                $service,

                test_presign_read,
                test_presign_write,

                test_multipart_complete,
                test_multipart_abort,
                test_presign_write_multipart,
            );
        )*
        $(
            blocking_behavior_test!(
                $service,

                test_blocking_list_dir,
            );
        )*
    };
}

macro_rules! behavior_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower>] {
                use super::*;

                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> Result<()> {
                        init_logger();
                        let _ = dotenv::dotenv();

                        let prefix = format!("opendal_{}_", $service);

                        let mut cfg = std::env::vars()
                            .filter_map(|(k, v)| {
                                k.to_lowercase()
                                    .strip_prefix(&prefix)
                                    .map(|k| (k.to_string(), v))
                            })
                            .collect::<HashMap<String, String>>();

                        if cfg.get("test").is_none() || cfg.get("test").unwrap() != "on" {
                            return Ok(());
                        }

                        let root = cfg.get("root").cloned().unwrap_or_else(|| "/".to_string());
                        let root = format!("{}{}/", root, uuid::Uuid::new_v4());
                        cfg.insert("root".into(), root);

                        let op = Operator::from_iter($service, cfg.into_iter())?.layer(LoggingLayer);
                        super::$test(op).await
                    }
                )*
            }
        }
    };
}

macro_rules! blocking_behavior_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _blocking>] {
                use super::*;

                $(
                    #[test]
                    $(
                        #[$meta]
                    )*
                    fn [< $test >]() -> Result<()> {
                        init_logger();
                        let _ = dotenv::dotenv();

                        let prefix = format!("opendal_{}_", $service);

                        let mut cfg = std::env::vars()
                            .filter_map(|(k, v)| {
                                k.to_lowercase()
                                    .strip_prefix(&prefix)
                                    .map(|k| (k.to_string(), v))
                            })
                            .collect::<HashMap<String, String>>();

                        if cfg.get("test").is_none() || cfg.get("test").unwrap() != "on" {
                            return Ok(());
                        }

                        let root = cfg.get("root").cloned().unwrap_or_else(|| "/".to_string());
                        let root = format!("{}{}/", root, uuid::Uuid::new_v4());
                        cfg.insert("root".into(), root);

                        let op = Operator::from_iter($service, cfg.into_iter())?.layer(LoggingLayer);

                         // Ignore this test if not supported.
                        if !op.metadata().can_blocking() {
                            return Ok(());
                        }
                        super::$test(op)
                    }
                )*
            }
        }
    };
}

behavior_tests!(Azblob, Fs, Memory, S3, Gcs, Obs, Ipmfs);
cfg_if::cfg_if! {
    if #[cfg(feature = "services-hdfs")] {
        behavior_tests!(Hdfs);
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "services-http")] {
        behavior_tests!(Http);
    }
}
cfg_if::cfg_if! {
    if #[cfg(feature = "services-ftp")] {
        behavior_tests!(Ftp);
    }
}

/// List dir should return newly created file.
fn test_blocking_list_dir(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .blocking_write(content)
        .expect("write must succeed");

    let obs = op.object("/").blocking_list()?;
    let mut found = false;
    for de in obs {
        let de = de?;
        let meta = de.blocking_metadata()?;
        if de.path() == path {
            assert_eq!(meta.mode(), ObjectMode::FILE);
            assert_eq!(meta.content_length(), size as u64);

            found = true
        }
    }
    assert!(found, "file should be found in list");

    op.object(&path)
        .blocking_delete()
        .expect("delete must succeed");
    Ok(())
}

/// Presign write should succeed.
async fn test_presign_write(op: Operator) -> Result<()> {
    // Ignore this test if not supported.
    if !op.metadata().can_presign() {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    let signed_req = op.object(&path).presign_write(Duration::hours(1))?;
    debug!("Generated request: {signed_req:?}");

    let mut req = isahc::Request::builder()
        .method(signed_req.method())
        .uri(signed_req.uri())
        .body(isahc::AsyncBody::from_bytes_static(content.clone()))?;
    *req.headers_mut() = signed_req.header().clone();
    req.headers_mut()
        .insert(header::CONTENT_LENGTH, content.len().to_string().parse()?);

    let client = isahc::HttpClient::new().expect("must init succeed");
    let _ = client.send_async(req).await?;

    let meta = op
        .object(&path)
        .metadata()
        .await
        .expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

// Presign read should read content successfully.
async fn test_presign_read(op: Operator) -> Result<()> {
    // Ignore this test if not supported.
    if !op.metadata().can_presign() {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let signed_req = op.object(&path).presign_read(Duration::hours(1))?;
    debug!("Generated request: {signed_req:?}");

    let mut req = isahc::Request::builder()
        .method(signed_req.method())
        .uri(signed_req.uri())
        .body(isahc::AsyncBody::empty())?;
    *req.headers_mut() = signed_req.header().clone();

    let client = isahc::HttpClient::new().expect("must init succeed");
    let mut resp = client.send_async(req).await?;

    let bs: Vec<u8> = resp.bytes().await?;
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

// Multipart complete should succeed.
async fn test_multipart_complete(op: Operator) -> Result<()> {
    // Ignore this test if not supported.
    if !op.metadata().can_multipart() {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();

    // Create multipart
    let mp = op.object(&path).create_multipart().await?;

    // Upload first part
    let mut p1_content = gen_fixed_bytes(5 * 1024 * 1024);
    let p1 = mp.write(1, p1_content.clone()).await?;

    // Upload second part
    let mut p2_content = gen_fixed_bytes(5 * 1024 * 1024);
    let p2 = mp.write(2, p2_content.clone()).await?;

    // Complete
    let o = mp.complete(vec![p1, p2]).await?;

    let meta = o.metadata().await?;

    assert_eq!(10 * 1024 * 1024, meta.content_length(), "complete size");
    assert_eq!(
        format!("{:x}", Sha256::digest(o.read().await?)),
        format!(
            "{:x}",
            Sha256::digest({
                let mut bs = Vec::with_capacity(10 * 1024 * 1024);
                bs.append(&mut p1_content);
                bs.append(&mut p2_content);
                bs
            })
        ),
        "complete content"
    );

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

// Multipart abort should succeed.
async fn test_multipart_abort(op: Operator) -> Result<()> {
    // Ignore this test if not supported.
    if !op.metadata().can_multipart() {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();

    // Create multipart
    let mp = op.object(&path).create_multipart().await?;

    // Upload first part
    let p1_content = gen_fixed_bytes(5 * 1024 * 1024);
    let _ = mp.write(1, p1_content).await?;

    // Upload second part
    let p2_content = gen_fixed_bytes(5 * 1024 * 1024);
    let _ = mp.write(2, p2_content).await?;

    // Abort
    mp.abort().await?;
    Ok(())
}

/// Presign write multipart should succeed.
async fn test_presign_write_multipart(op: Operator) -> Result<()> {
    // Ignore this test if not supported.
    if !op.metadata().can_presign() || !op.metadata().can_multipart() {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();

    // Create multipart
    let mp = op.object(&path).create_multipart().await?;

    let (content, size) = gen_bytes();

    let signed_req = mp.presign_write(1, Duration::hours(1))?;
    debug!("Generated request: {signed_req:?}");

    let mut req = isahc::Request::builder()
        .method(signed_req.method())
        .uri(signed_req.uri())
        .body(isahc::AsyncBody::from_bytes_static(content.clone()))?;
    *req.headers_mut() = signed_req.header().clone();
    req.headers_mut()
        .insert(header::CONTENT_LENGTH, content.len().to_string().parse()?);

    let client = isahc::HttpClient::new().expect("must init succeed");
    let resp = client.send_async(req).await?;
    let etag = resp
        .headers()
        .get(ETAG)
        .expect("must have etag")
        .to_str()
        .expect("must be valid string");

    let o = mp.complete(vec![ObjectPart::new(1, etag)]).await?;

    let meta = o.metadata().await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}
