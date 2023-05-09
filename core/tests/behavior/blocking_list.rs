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
use std::collections::HashSet;

use anyhow::Result;
use log::debug;
use opendal::BlockingOperator;
use opendal::EntryMode;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_blocking
/// - can_list or can_scan
macro_rules! behavior_blocking_list_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            $(
                #[test]
                $(
                    #[$meta]
                )*
                fn [<blocking_list_ $test >]() -> anyhow::Result<()> {
                    match OPERATOR.as_ref() {
                        Some(op) if op.info().can_read()
                            && op.info().can_write()
                            && op.info().can_blocking() && op.info().can_list() => $crate::blocking_list::$test(op.blocking()),
                        Some(_) => {
                            log::warn!("service {} doesn't support blocking_list, ignored", opendal::Scheme::$service);
                            Ok(())
                        },
                        None => {
                            Ok(())
                        }
                    }
                }
            )*
        }
    };
}

#[macro_export]
macro_rules! behavior_blocking_list_tests {
     ($($service:ident),*) => {
        $(
            behavior_blocking_list_test!(
                $service,

                test_list_dir,
                test_list_non_exist_dir,
                test_scan,
            );
        )*
    };
}

/// List dir should return newly created file.
pub fn test_list_dir(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    let path = format!("{parent}/{}", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.write(&path, content).expect("write must succeed");

    let obs = op.list(&format!("{parent}/"))?;
    let mut found = false;
    for de in obs {
        let de = de?;
        let meta = op.stat(de.path())?;
        if de.path() == path {
            assert_eq!(meta.mode(), EntryMode::FILE);

            assert_eq!(meta.content_length(), size as u64);

            found = true
        }
    }
    assert!(found, "file should be found in list");

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// List non exist dir should return nothing.
pub fn test_list_non_exist_dir(op: BlockingOperator) -> Result<()> {
    let dir = format!("{}/", uuid::Uuid::new_v4());

    let obs = op.list(&dir)?;
    let mut objects = HashMap::new();
    for de in obs {
        let de = de?;
        objects.insert(de.path().to_string(), de);
    }
    debug!("got objects: {:?}", objects);

    assert_eq!(objects.len(), 0, "dir should only return empty");
    Ok(())
}

// Walk top down should output as expected
pub fn test_scan(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let expected = vec![
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];
    for path in expected.iter() {
        if path.ends_with('/') {
            op.create_dir(&format!("{parent}/{path}"))?;
        } else {
            op.write(&format!("{parent}/{path}"), "test_scan")?;
        }
    }

    let w = op.scan(&format!("{parent}/x/"))?;
    let actual = w
        .collect::<Vec<_>>()
        .into_iter()
        .map(|v| {
            v.unwrap()
                .path()
                .strip_prefix(&format!("{parent}/"))
                .unwrap()
                .to_string()
        })
        .collect::<HashSet<_>>();

    debug!("walk top down: {:?}", actual);

    assert!(actual.contains("x/y"));
    assert!(actual.contains("x/x/y"));
    assert!(actual.contains("x/x/x/y"));
    Ok(())
}
