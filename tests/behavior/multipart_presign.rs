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
use http::header::ETAG;
use log::debug;
use opendal::ObjectPart;
use opendal::Operator;
use time::Duration;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_multipart
/// - can_presign
macro_rules! behavior_multipart_presign_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _multipart_presign>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> std::io::Result<()> {
                        let op = $crate::utils::init_service(opendal::Scheme::$service, true);
                        match op {
                            Some(op) if op.metadata().can_read() && op.metadata().can_write() && op.metadata().can_multipart() && op.metadata().can_presign() => $crate::multipart_presign::$test(op).await,
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
macro_rules! behavior_multipart_presign_tests {
     ($($service:ident),*) => {
        $(
            behavior_multipart_presign_test!(
                $service,

                test_presign_write_multipart,
            );
        )*
    };
}

/// Presign write multipart should succeed.
pub async fn test_presign_write_multipart(op: Operator) -> Result<()> {
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
        .body(isahc::AsyncBody::from_bytes_static(content.clone()))
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

    let client = isahc::HttpClient::new().expect("must init succeed");
    let resp = client
        .send_async(req)
        .await
        .expect("send request must succeed");
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
