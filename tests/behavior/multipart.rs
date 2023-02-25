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
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_multipart
macro_rules! behavior_multipart_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _multipart>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op) if op.metadata().can_read() && op.metadata().can_write() && op.metadata().can_multipart() => $crate::multipart::$test(op).await,
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
macro_rules! behavior_multipart_tests {
     ($($service:ident),*) => {
        $(
            behavior_multipart_test!(
                $service,

                test_multipart_complete,
                test_multipart_abort,
            );
        )*
    };
}

// Multipart complete should succeed.
pub async fn test_multipart_complete(op: Operator) -> Result<()> {
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

    let meta = o.stat().await?;

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
pub async fn test_multipart_abort(op: Operator) -> Result<()> {
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
