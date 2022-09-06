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

use std::io::Result;

use http::header;
use log::debug;
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;
use time::Duration;
use AsyncReadResponseExt;

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
                    async fn [< $test >]() -> std::io::Result<()> {
                        let op = $crate::utils::init_service(opendal::Scheme::$service, true);
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

    let mut req = Request::builder()
        .method(signed_req.method())
        .uri(signed_req.uri())
        .body(AsyncBody::from_bytes_static(content.clone()))
        .expect("build request must succeed");
    *req.headers_mut() = signed_req.header().clone();
    req.headers_mut().insert(
        header::CONTENT_LENGTH,
        content
            .len()
            .to_string()
            .parse()
            .expect("parse header must succeed"),
    );

    let client = HttpClient::new().expect("must init succeed");
    let _ = client
        .send_async(req)
        .await
        .expect("send request must succeed");

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

    let mut req = Request::builder()
        .method(signed_req.method())
        .uri(signed_req.uri())
        .body(AsyncBody::empty())
        .expect("build request must succeed");
    *req.headers_mut() = signed_req.header().clone();

    let client = HttpClient::new().expect("must init succeed");
    let mut resp = client
        .send_async(req)
        .await
        .expect("send request must succeed");

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
