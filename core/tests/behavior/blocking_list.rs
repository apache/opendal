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
use log::debug;

use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.read && cap.write && cap.copy && cap.blocking && cap.list {
        tests.extend(blocking_trials!(
            op,
            test_blocking_list_dir,
            test_blocking_list_non_exist_dir,
            test_blocking_list_not_exist_dir_with_recursive,
            test_blocking_list_dir_with_recursive,
            test_blocking_list_dir_with_recursive_no_trailing_slash,
            test_blocking_list_file_with_recursive,
            test_blocking_remove_all
        ))
    }
}

/// List dir should return newly created file.
pub fn test_blocking_list_dir(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    let path = format!("{parent}/{}", uuid::Uuid::new_v4());
    debug!("Generate a random file: {}", &path);
    let (_, content, size) = TEST_FIXTURE.new_file_with_path(op.clone(), &path);

    op.write(&path, content).expect("write must succeed");

    let obs = op.lister(&format!("{parent}/"))?;
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
    Ok(())
}

/// List non exist dir should return nothing.
pub fn test_blocking_list_non_exist_dir(op: BlockingOperator) -> Result<()> {
    let dir = TEST_FIXTURE.new_dir_path();

    let obs = op.lister(&dir)?;
    let mut objects = HashMap::new();
    for de in obs {
        let de = de?;
        objects.insert(de.path().to_string(), de);
    }
    debug!("got objects: {:?}", objects);

    assert_eq!(objects.len(), 0, "dir should only return empty");
    Ok(())
}

// Remove all should remove all in this path.
pub fn test_blocking_remove_all(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().delete {
        return Ok(());
    }

    let parent = uuid::Uuid::new_v4().to_string();

    let expected = [
        "x/", "x/y", "x/x/", "x/x/y", "x/x/x/", "x/x/x/y", "x/x/x/x/",
    ];

    for path in expected.iter() {
        if path.ends_with('/') {
            op.create_dir(&format!("{parent}/{path}"))?;
        } else {
            op.write(&format!("{parent}/{path}"), "test_scan")?;
        }
    }

    op.remove_all(&format!("{parent}/x/"))?;

    for path in expected.iter() {
        if path.ends_with('/') {
            continue;
        }
        assert!(
            !op.exists(&format!("{parent}/{path}"))?,
            "{parent}/{path} should be removed"
        )
    }

    Ok(())
}

pub fn test_blocking_list_not_exist_dir_with_recursive(op: BlockingOperator) -> Result<()> {
    let dir = format!("{}/", uuid::Uuid::new_v4());

    let obs = op.lister_with(&dir).recursive(true).call()?;
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
pub fn test_blocking_list_dir_with_recursive(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let paths = [
        "x/", "x/x/", "x/x/x/", "x/x/x/x/", "x/x/x/y", "x/x/y", "x/y", "x/yy",
    ];
    for path in paths.iter() {
        if path.ends_with('/') {
            op.create_dir(&format!("{parent}/{path}"))?;
        } else {
            op.write(&format!("{parent}/{path}"), "test_scan")?;
        }
    }
    let w = op
        .lister_with(&format!("{parent}/x/"))
        .recursive(true)
        .call()?;
    let mut actual = w
        .collect::<Vec<_>>()
        .into_iter()
        .map(|v| {
            v.unwrap()
                .path()
                .strip_prefix(&format!("{parent}/"))
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    actual.sort();
    let expected = vec![
        "x/", "x/x/", "x/x/x/", "x/x/x/x/", "x/x/x/y", "x/x/y", "x/y", "x/yy",
    ];
    assert_eq!(actual, expected);
    Ok(())
}

// Walk top down should output as expected
// same as test_list_dir_with_recursive except listing 'x' instead of 'x/'
pub fn test_blocking_list_dir_with_recursive_no_trailing_slash(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let paths = [
        "x/", "x/x/", "x/x/x/", "x/x/x/x/", "x/x/x/y", "x/x/y", "x/y", "x/yy",
    ];
    for path in paths.iter() {
        if path.ends_with('/') {
            op.create_dir(&format!("{parent}/{path}"))?;
        } else {
            op.write(&format!("{parent}/{path}"), "test_scan")?;
        }
    }
    let w = op
        .lister_with(&format!("{parent}/x"))
        .recursive(true)
        .call()?;
    let mut actual = w
        .collect::<Vec<_>>()
        .into_iter()
        .map(|v| {
            v.unwrap()
                .path()
                .strip_prefix(&format!("{parent}/"))
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    actual.sort();
    let expected = paths.to_vec();
    assert_eq!(actual, expected);
    Ok(())
}

// Walk top down should output as expected
// same as test_list_dir_with_recursive except listing 'x' instead of 'x/'
pub fn test_blocking_list_file_with_recursive(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();

    let paths = ["y", "yy"];
    for path in paths.iter() {
        if path.ends_with('/') {
            op.create_dir(&format!("{parent}/{path}"))?;
        } else {
            op.write(&format!("{parent}/{path}"), "test_scan")?;
        }
    }
    let w = op
        .lister_with(&format!("{parent}/y"))
        .recursive(true)
        .call()?;
    let mut actual = w
        .collect::<Vec<_>>()
        .into_iter()
        .map(|v| {
            v.unwrap()
                .path()
                .strip_prefix(&format!("{parent}/"))
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    actual.sort();
    let expected = vec!["y", "yy"];
    assert_eq!(actual, expected);
    Ok(())
}
