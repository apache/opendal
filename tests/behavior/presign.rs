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

use std::str::FromStr;

use anyhow::Result;
use http::header;
use log::debug;
use opendal::raw;
use opendal::Operator;
use reqwest::Url;
use sha2::Digest;
use sha2::Sha256;
use time::Duration;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_presign
macro_rules! behavior_presign_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _presign>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op) if op.metadata().can_read() && op.metadata().can_write() && op.metadata().can_presign() => $crate::presign::$test(op).await,
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
macro_rules! behavior_presign_tests {
     ($($service:ident),*) => {
        $(
            behavior_presign_test!(
                $service,

                test_presign_write,
                test_presign_read,
                test_presign_stat,
            );
        )*
    };
}

/// Presign write should succeed.
pub async fn test_presign_write(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    let signed_req = op.object(&path).presign_write(Duration::hours(1))?;
    debug!("Generated request: {signed_req:?}");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }
    req = req.header(header::CONTENT_LENGTH, content.len());
    req = req.body(reqwest::Body::from(content));

    let resp = req.send().await.expect("send request must succeed");
    debug!(
        "write response: {:?}",
        resp.text().await.expect("read response must succeed")
    );

    let meta = op.object(&path).stat().await.expect("stat must succeed");
    assert_eq!(meta.content_length(), size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

pub async fn test_presign_stat(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");
    let signed_req = op.object(&path).presign_stat(Duration::hours(1))?;
    debug!("Generated request: {signed_req:?}");
    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }
    let resp = req.send().await.expect("send request must succeed");
    assert_eq!(resp.status(), http::StatusCode::OK, "status ok",);

    let content_length = raw::parse_content_length(resp.headers())
        .expect("no content length")
        .expect("content length must be present");
    assert_eq!(content_length, size as u64);

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

// Presign read should read content successfully.
pub async fn test_presign_read(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let signed_req = op.object(&path).presign_read(Duration::hours(1))?;
    debug!("Generated request: {signed_req:?}");

    let client = reqwest::Client::new();
    let mut req = client.request(
        signed_req.method().clone(),
        Url::from_str(&signed_req.uri().to_string()).expect("must be valid url"),
    );
    for (k, v) in signed_req.header() {
        req = req.header(k, v);
    }

    let resp = req.send().await.expect("send request must succeed");

    let bs = resp.bytes().await.expect("read response must succeed");
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
