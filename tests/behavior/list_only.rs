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

use std::collections::HashMap;

use anyhow::Result;
use futures::TryStreamExt;
use opendal::ObjectMode;
use opendal::Operator;

/// Test services that meet the following capability:
///
/// - !can_write
/// - can_list
macro_rules! behavior_list_only_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _list_only>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(false);
                        match op {
                            Some(op) if op.metadata().can_list() && !op.metadata().can_write() => $crate::list_only::$test(op).await,
                            Some(_) => {
                                log::warn!("service {} doesn't support list, ignored", opendal::Scheme::$service);
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
macro_rules! behavior_list_only_tests {
     ($($service:ident),*) => {
        $(
            behavior_list_only_test!(
                $service,

                test_list,
            );
        )*
    };
}

/// Stat normal file and dir should return metadata
pub async fn test_list(op: Operator) -> Result<()> {
    let mut entries = HashMap::new();

    let mut ds = op.object("/").list().await?;
    while let Some(de) = ds.try_next().await? {
        entries.insert(de.path().to_string(), de.stat().await?.mode());
    }

    assert_eq!(entries["normal_file"], ObjectMode::FILE);
    assert_eq!(entries["special_file  !@#$%^&()_+-=;',"], ObjectMode::FILE);

    assert_eq!(entries["normal_dir/"], ObjectMode::DIR);
    assert_eq!(entries["special_dir  !@#$%^&()_+-=;',/"], ObjectMode::DIR);

    Ok(())
}
