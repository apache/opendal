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
use std::collections::HashSet;

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use futures::TryStreamExt;
use log::debug;
use opendal::ErrorKind;
use opendal::ObjectMode;
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
            mod [<services_ $service:lower _list>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> anyhow::Result<()> {
                        let op = $crate::utils::init_service::<opendal::services::$service>(true);
                        match op {
                            Some(op) if op.metadata().can_read()
                                && op.metadata().can_write()
                                && (op.metadata().can_list()
                                    || op.metadata().can_scan()) => $crate::list::$test(op).await,
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
                test_scan,
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
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, size) = gen_bytes();

    op.object(&path)
        .write(content)
        .await
        .expect("write must succeed");

    let mut obs = op.object("/").list().await?;
    let mut found = false;
    while let Some(de) = obs.try_next().await? {
        let meta = de.stat().await?;
        if de.path() == path {
            assert_eq!(meta.mode(), ObjectMode::FILE);
            assert_eq!(meta.content_length(), size as u64);

            found = true
        }
    }
    assert!(found, "file should be found in list");

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// listing a directory, which contains more objects than a single page can take.
pub async fn test_list_rich_dir(op: Operator) -> Result<()> {
    op.object("test_list_rich_dir/").create().await?;

    let mut expected: Vec<String> = (0..=1000)
        .map(|num| format!("test_list_rich_dir/file-{num}"))
        .collect();

    expected
        .iter()
        .map(|v| async {
            let o = op.object(v);
            o.create().await.expect("create must succeed");
        })
        // Collect into a FuturesUnordered.
        .collect::<FuturesUnordered<_>>()
        // Collect to consume all features.
        .collect::<Vec<_>>()
        .await;

    let mut objects = op.object("test_list_rich_dir/").list().await?;
    let mut actual = vec![];
    while let Some(o) = objects.try_next().await? {
        let path = o.path().to_string();
        actual.push(path)
    }
    expected.sort_unstable();
    actual.sort_unstable();

    assert_eq!(actual, expected);

    op.batch().remove_all("test_list_rich_dir/").await?;
    Ok(())
}

/// List empty dir should return nothing.
pub async fn test_list_empty_dir(op: Operator) -> Result<()> {
    let dir = format!("{}/", uuid::Uuid::new_v4());

    op.object(&dir).create().await.expect("write must succeed");

    let mut obs = op.object(&dir).list().await?;
    let mut objects = HashMap::new();
    while let Some(de) = obs.try_next().await? {
        objects.insert(de.path().to_string(), de);
    }
    debug!("got objects: {:?}", objects);

    assert_eq!(objects.len(), 0, "dir should only return empty");

    op.object(&dir).delete().await.expect("delete must succeed");
    Ok(())
}

/// List non exist dir should return nothing.
pub async fn test_list_non_exist_dir(op: Operator) -> Result<()> {
    let dir = format!("{}/", uuid::Uuid::new_v4());

    let mut obs = op.object(&dir).list().await?;
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

    op.object(&path).create().await.expect("creat must succeed");

    let mut obs = op.object("/").list().await?;
    let mut found = false;
    while let Some(de) = obs.try_next().await? {
        if de.path() == path {
            assert_eq!(de.stat().await?.mode(), ObjectMode::DIR);
            assert_eq!(de.name(), path);

            found = true
        }
    }
    assert!(found, "dir should be found in list");

    op.object(&path)
        .delete()
        .await
        .expect("delete must succeed");
    Ok(())
}

/// List dir should also to list nested dir.
pub async fn test_list_nested_dir(op: Operator) -> Result<()> {
    let dir = format!("{}/{}/", uuid::Uuid::new_v4(), uuid::Uuid::new_v4());

    let file_name = uuid::Uuid::new_v4().to_string();
    let file_path = format!("{dir}{file_name}");
    let dir_name = format!("{}/", uuid::Uuid::new_v4());
    let dir_path = format!("{dir}{dir_name}");

    op.object(&dir).create().await.expect("creat must succeed");
    op.object(&file_path)
        .create()
        .await
        .expect("creat must succeed");
    op.object(&dir_path)
        .create()
        .await
        .expect("creat must succeed");

    let mut obs = op.object(&dir).list().await?;
    let mut objects = HashMap::new();

    while let Some(de) = obs.try_next().await? {
        objects.insert(de.path().to_string(), de);
    }
    debug!("got objects: {:?}", objects);

    assert_eq!(objects.len(), 2, "dir should only got 2 objects");

    // Check file
    let meta = objects
        .get(&file_path)
        .expect("file should be found in list")
        .stat()
        .await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    // Check dir
    let meta = objects
        .get(&dir_path)
        .expect("file should be found in list")
        .stat()
        .await?;
    assert_eq!(meta.mode(), ObjectMode::DIR);

    op.object(&file_path)
        .delete()
        .await
        .expect("delete must succeed");
    op.object(&dir_path)
        .delete()
        .await
        .expect("delete must succeed");
    op.object(&dir).delete().await.expect("delete must succeed");
    Ok(())
}

/// List with path file should auto add / suffix.
pub async fn test_list_dir_with_file_path(op: Operator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let obs = op.object(&parent).list().await.map(|_| ());
    assert!(obs.is_err());
    assert_eq!(obs.unwrap_err().kind(), ErrorKind::ObjectNotADirectory);

    Ok(())
}

// Walk top down should output as expected
pub async fn test_scan(op: Operator) -> Result<()> {
    let expected = vec![
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];
    for path in expected.iter() {
        op.object(path).create().await?;
    }

    let w = op.object("x/").scan().await?;
    let actual = w
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|v| v.path().to_string())
        .collect::<HashSet<_>>();

    debug!("walk top down: {:?}", actual);

    assert!(actual.contains("x/y"));
    assert!(actual.contains("x/x/y"));
    assert!(actual.contains("x/x/x/y"));
    Ok(())
}

// Remove all should remove all in this path.
pub async fn test_remove_all(op: Operator) -> Result<()> {
    let expected = vec![
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];
    for path in expected.iter() {
        op.object(path).create().await?;
    }

    op.batch().remove_all("x/").await?;

    for path in expected.iter() {
        if path.ends_with('/') {
            continue;
        }
        assert!(
            !op.object(path).is_exist().await?,
            "{path} should be removed"
        )
    }
    Ok(())
}
