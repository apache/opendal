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

use std::collections::HashMap;

use anyhow::Result;
use futures::TryStreamExt;
use opendal::EntryMode;
use opendal::Operator;

/// Test services that meet the following capability:
///
/// - !can_write
/// - can_list
macro_rules! behavior_list_only_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            $(
                #[tokio::test]
                $(
                    #[$meta]
                )*
                async fn [<list_only_ $test >]() -> anyhow::Result<()> {
                    match OPERATOR.as_ref() {
                        Some(op) if op.info().can_list() && !op.info().can_write() => $crate::list_only::$test(op.clone()).await,
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

    let mut ds = op.list("/").await?;
    while let Some(de) = ds.try_next().await? {
        entries.insert(de.path().to_string(), op.stat(de.path()).await?.mode());
    }

    assert_eq!(entries["normal_file"], EntryMode::FILE);
    assert_eq!(entries["special_file  !@#$%^&()_+-=;',"], EntryMode::FILE);

    assert_eq!(entries["normal_dir/"], EntryMode::DIR);
    assert_eq!(entries["special_dir  !@#$%^&()_+-=;',/"], EntryMode::DIR);

    Ok(())
}
