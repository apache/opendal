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
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use opendal::ops::OpList;
use opendal::EntryMode;
use opendal::ErrorKind;
use opendal::Operator;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_list or can_scan
macro_rules! behavior_list_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            $(
                #[test]
                $(
                    #[$meta]
                )*
                fn [<list_ $test >]() -> anyhow::Result<()> {
                    match OPERATOR.as_ref() {
                        Some(op) if op.info().can_read()
                            && op.info().can_write()
                            && op.info().can_list()
                                 => RUNTIME.block_on($crate::list::$test(op.clone())),
                        Some(_) => {
                            log::warn!("service {} doesn't support list, ignored", opendal::Scheme::$service);
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
macro_rules! behavior_list_tests {
     ($($service:ident),*) => {
        $(
            behavior_list_test!(
                $service,

                test_check,
                test_list_dir,
                test_list_rich_dir,
                test_list_empty_dir,
                test_list_non_exist_dir,
                test_list_sub_dir,
                test_list_nested_dir,
                test_list_dir_with_file_path,
                test_list_with_start_after,
                test_scan,
                test_scan_root,
                test_remove_all,
            );
        )*
    };
}

/// Check should be OK.
pub async fn test_check(op: Operator) -> Result<()> {
    op.check().await.expect("operator check is ok");

    Ok(())
}

/// List dir should return newly created file.
pub async fn test_list_dir(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    let path = format!("{parent}/{}", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.write(&path, content).await.expect("write must succeed");

    let mut obs = op.list(&format!("{parent}/")).await?;
    let mut found = false;
    while let Some(de) = obs.try_next().await? {
        let meta = op.stat(de.path()).await?;
        if de.path() == path {
            assert_eq!(meta.mode(), EntryMode::FILE);

            assert_eq!(meta.content_length(), size as u64);

            found = true
        }
    }
    assert!(found, "file should be found in list");

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// listing a directory, which contains more objects than a single page can take.
pub async fn test_list_rich_dir(op: Operator) -> Result<()> {
    op.create_dir("test_list_rich_dir/").await?;

    let mut expected: Vec<String> = (0..=1000)
        .map(|num| format!("test_list_rich_dir/file-{num}"))
        .collect();

    expected
        .iter()
        .map(|v| async {
            op.write(v, "test_list_rich_dir")
                .await
                .expect("create must succeed");
        })
        // Collect into a FuturesUnordered.
        .collect::<FuturesUnordered<_>>()
        // Collect to consume all features.
        .collect::<Vec<_>>()
        .await;

    let mut objects = op.list("test_list_rich_dir/").await?;
    let mut actual = vec![];
    while let Some(o) = objects.try_next().await? {
        let path = o.path().to_string();
        actual.push(path)
    }
    expected.sort_unstable();
    actual.sort_unstable();

    assert_eq!(actual, expected);

    op.remove_all("test_list_rich_dir/").await?;
    Ok(())
}

/// List empty dir should return nothing.
pub async fn test_list_empty_dir(op: Operator) -> Result<()> {
    let dir = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&dir).await.expect("write must succeed");

    let mut obs = op.list(&dir).await?;
    let mut objects = HashMap::new();
    while let Some(de) = obs.try_next().await? {
        objects.insert(de.path().to_string(), de);
    }
    debug!("got objects: {:?}", objects);

    assert_eq!(objects.len(), 0, "dir should only return empty");

    op.delete(&dir).await.expect("delete must succeed");
    Ok(())
}

/// List non exist dir should return nothing.
pub async fn test_list_non_exist_dir(op: Operator) -> Result<()> {
    let dir = format!("{}/", uuid::Uuid::new_v4());

    let mut obs = op.list(&dir).await?;
    let mut objects = HashMap::new();
    while let Some(de) = obs.try_next().await? {
        objects.insert(de.path().to_string(), de);
    }
    debug!("got objects: {:?}", objects);

    assert_eq!(objects.len(), 0, "dir should only return empty");
    Ok(())
}

/// List dir should return correct sub dir.
pub async fn test_list_sub_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path).await.expect("creat must succeed");

    let mut obs = op.list("/").await?;
    let mut found = false;
    while let Some(de) = obs.try_next().await? {
        if de.path() == path {
            let meta = op.stat(&path).await?;
            assert_eq!(meta.mode(), EntryMode::DIR);
            assert_eq!(de.name(), path);

            found = true
        }
    }
    assert!(found, "dir should be found in list");

    op.delete(&path).await.expect("delete must succeed");
    Ok(())
}

/// List dir should also to list nested dir.
pub async fn test_list_nested_dir(op: Operator) -> Result<()> {
    let dir = format!("{}/{}/", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());

    let file_name = uuid::Uuid::new_v4().to_string();
    let file_path = format!("{dir}{file_name}");
    let dir_name = format!("{}/", uuid::Uuid::new_v4());
    let dir_path = format!("{dir}{dir_name}");

    op.create_dir(&dir).await.expect("creat must succeed");
    op.write(&file_path, "test_list_nested_dir")
        .await
        .expect("creat must succeed");
    op.create_dir(&dir_path).await.expect("creat must succeed");

    let mut obs = op.list(&dir).await?;
    let mut objects = HashMap::new();

    while let Some(de) = obs.try_next().await? {
        objects.insert(de.path().to_string(), de);
    }
    debug!("got objects: {:?}", objects);

    assert_eq!(objects.len(), 2, "dir should only got 2 objects");

    // Check file
    let meta = op
        .stat(
            objects
                .get(&file_path)
                .expect("file should be found in list")
                .path(),
        )
        .await?;
    assert_eq!(meta.mode(), EntryMode::FILE);
    assert_eq!(meta.content_length(), 20);

    // Check dir
    let meta = op
        .stat(
            objects
                .get(&dir_path)
                .expect("file should be found in list")
                .path(),
        )
        .await?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&file_path).await.expect("delete must succeed");
    op.delete(&dir_path).await.expect("delete must succeed");
    op.delete(&dir).await.expect("delete must succeed");
    Ok(())
}

/// List with path file should auto add / suffix.
pub async fn test_list_dir_with_file_path(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let obs = op.list(&parent).await.map(|_| ());
    assert!(obs.is_err());
    assert_eq!(obs.unwrap_err().kind(), ErrorKind::NotADirectory);

    Ok(())
}

/// List with start after should start listing after the specified key
pub async fn test_list_with_start_after(op: Operator) -> Result<()> {
    if !op.info().capability().list_with_start_after {
        return Ok(());
    }

    let dir = &format!("{}/", uuid::Uuid::new_v4());
    op.create_dir(dir).await?;

    let given: Vec<String> = vec!["file-0", "file-1", "file-2", "file-3", "file-4", "file-5"]
        .iter()
        .map(|name| format!("{dir}{name}-{}", uuid::Uuid::new_v4()))
        .collect();

    given
        .iter()
        .map(|name| async {
            op.write(name, "content")
                .await
                .expect("create must succeed");
        })
        .collect::<FuturesUnordered<_>>()
        .collect::<Vec<_>>()
        .await;

    let option = OpList::new().with_start_after(&given[2]);
    let mut objects = op.list_with(dir, option).await?;
    let mut actual = vec![];
    while let Some(o) = objects.try_next().await? {
        let path = o.path().to_string();
        actual.push(path)
    }

    let expected: Vec<String> = given.into_iter().skip(3).collect();

    assert_eq!(expected, actual);

    op.remove_all(dir).await?;

    Ok(())
}

pub async fn test_scan_root(op: Operator) -> Result<()> {
    let w = op.scan("").await?;
    let actual = w
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|v| v.path().to_string())
        .collect::<HashSet<_>>();

    assert!(!actual.contains("/"), "empty root should return itself");
    assert!(!actual.contains(""), "empty root should return empty");
    Ok(())
}

// Walk top down should output as expected
pub async fn test_scan(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let expected = vec![
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];
    for path in expected.iter() {
        if path.ends_with('/') {
            op.create_dir(&format!("{parent}/{path}")).await?;
        } else {
            op.write(&format!("{parent}/{path}"), "test_scan").await?;
        }
    }

    let w = op.scan(&format!("{parent}/x/")).await?;
    let actual = w
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|v| {
            v.path()
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

// Remove all should remove all in this path.
pub async fn test_remove_all(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let expected = vec![
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];
    for path in expected.iter() {
        if path.ends_with('/') {
            op.create_dir(&format!("{parent}/{path}")).await?;
        } else {
            op.write(&format!("{parent}/{path}"), "test_scan").await?;
        }
    }

    op.remove_all(&format!("{parent}/x/")).await?;

    for path in expected.iter() {
        if path.ends_with('/') {
            continue;
        }
        assert!(
            !op.is_exist(&format!("{parent}/{path}")).await?,
            "{parent}/{path} should be removed"
        )
    }
    Ok(())
}
