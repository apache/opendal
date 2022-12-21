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

use anyhow::Result;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use log::debug;
use opendal::Operator;
use sha2::Digest;
use sha2::Sha256;
use std::io::SeekFrom;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_open
macro_rules! behavior_open_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _open>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service(opendal::Scheme::$service, true);
                        match op {
                            Some(op) if op.metadata().can_read() && op.metadata().can_write() && op.metadata().can_open() => $crate::open::$test(op).await,
                            Some(_) => {
                                log::warn!("service {} doesn't support open, ignored", opendal::Scheme::$service);
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
macro_rules! behavior_open_tests {
     ($($service:ident),*) => {
        $(
            behavior_open_test!(
                $service,

                test_open,
            );
        )*
    };
}

pub async fn test_open(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();
    let (offset, _) = gen_offset_length(size as usize);

    op.object(&path)
        .write(content.clone())
        .await
        .expect("write must succeed");

    let mut oh = op.object(&path).open().await?;
    let n = oh.seek(SeekFrom::End(0)).await?;
    assert_eq!(n as usize, size, "open size");

    let _ = oh.seek(SeekFrom::Start(offset)).await?;
    let mut bs = Vec::new();
    let n = oh.read_to_end(&mut bs).await?;
    assert_eq!(n, size - offset as usize, "read size");
    assert_eq!(
        format!("{:x}", Sha256::digest(&bs)),
        format!("{:x}", Sha256::digest(&content[offset as usize..])),
        "read content"
    );

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}
