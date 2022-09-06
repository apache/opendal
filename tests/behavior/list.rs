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

use std::collections::HashMap;
use std::io::Result;

use futures::TryStreamExt;
use log::debug;
use opendal::ObjectMode;
use opendal::Operator;

use super::utils::*;

/// Test services that meet the following capability:
///
/// - can_read
/// - can_write
/// - can_list
macro_rules! behavior_list_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            mod [<services_ $service:lower _list>] {
                $(
                    #[tokio::test]
                    $(
                        #[$meta]
                    )*
                    async fn [< $test >]() -> std::io::Result<()> {
                        let op = $crate::utils::init_service(opendal::Scheme::$service, true);
                        match op {
                            Some(op) if op.metadata().can_read() && op.metadata().can_write() && op.metadata().can_list() => $crate::list::$test(op).await,
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
                test_list_empty_dir,
                test_list_sub_dir,
                test_list_nested_dir,
                test_list_dir_with_file_path,
                test_walk_top_down,
                test_walk_top_down_within_empty_dir,
                test_walk_bottom_up,
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
        let meta = de.metadata().await?;
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

/// List dir should return correct sub dir.
pub async fn test_list_sub_dir(op: Operator) -> Result<()> {
    let path = format!("{}/", uuid::Uuid::new_v4());

    op.object(&path).create().await.expect("creat must succeed");

    let mut obs = op.object("/").list().await?;
    let mut found = false;
    while let Some(de) = obs.try_next().await? {
        if de.path() == path {
            assert_eq!(de.mode(), ObjectMode::DIR);
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
        .metadata()
        .await?;
    assert_eq!(meta.mode(), ObjectMode::FILE);
    assert_eq!(meta.content_length(), 0);

    // Check dir
    let meta = objects
        .get(&dir_path)
        .expect("file should be found in list")
        .metadata()
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
    assert!(obs.unwrap_err().to_string().contains("Not a directory"));

    Ok(())
}

// Walk top down should output as expected
pub async fn test_walk_top_down(op: Operator) -> Result<()> {
    let mut expected = vec![
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];
    for path in expected.iter() {
        op.object(path).create().await?;
    }

    let w = op.batch().walk_top_down("x/")?;
    let mut actual = w
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|v| v.path().to_string())
        .collect::<Vec<_>>();

    debug!("walk top down: {:?}", actual);

    fn get_position(vs: &[String], s: &str) -> usize {
        vs.iter()
            .position(|v| v == s)
            .unwrap_or_else(|| panic!("{s} is not found in {vs:?}"))
    }

    assert!(get_position(&actual, "x/x/x/x/") > get_position(&actual, "x/x/x/"));
    assert!(get_position(&actual, "x/x/x/") > get_position(&actual, "x/x/"));
    assert!(get_position(&actual, "x/x/") > get_position(&actual, "x/"));

    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
    Ok(())
}

// Walk top down within empty dir should output as expected
pub async fn test_walk_top_down_within_empty_dir(op: Operator) -> Result<()> {
    let mut expected = vec!["x/", "x/x/x/x/"];
    for path in expected.iter() {
        op.object(path).create().await?;
    }

    let w = op.batch().walk_top_down("x/")?;
    let mut actual = w
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|v| v.path().to_string())
        .collect::<Vec<_>>();

    debug!("walk top down: {:?}", actual);

    fn get_position(vs: &[String], s: &str) -> usize {
        vs.iter()
            .position(|v| v == s)
            .unwrap_or_else(|| panic!("{s} is not found in {vs:?}"))
    }

    assert!(get_position(&actual, "x/x/x/x/") > get_position(&actual, "x/"));

    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, vec!["x/", "x/x/", "x/x/x/", "x/x/x/x/"]);
    Ok(())
}

// Walk bottom up should output as expected
pub async fn test_walk_bottom_up(op: Operator) -> Result<()> {
    let mut expected = vec![
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];
    for path in expected.iter() {
        op.object(path).create().await?;
    }

    let w = op.batch().walk_bottom_up("x/")?;
    let mut actual = w
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .map(|v| v.path().to_string())
        .collect::<Vec<_>>();

    debug!("walk bottom up: {:?}", actual);

    fn get_position(vs: &[String], s: &str) -> usize {
        vs.iter()
            .position(|v| v == s)
            .unwrap_or_else(|| panic!("{s} is not found in {vs:?}"))
    }

    assert!(get_position(&actual, "x/x/x/x/") < get_position(&actual, "x/x/x/"));
    assert!(get_position(&actual, "x/x/x/") < get_position(&actual, "x/x/"));
    assert!(get_position(&actual, "x/x/") < get_position(&actual, "x/"));

    expected.sort_unstable();
    actual.sort_unstable();
    assert_eq!(actual, expected);
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
