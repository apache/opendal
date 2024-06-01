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

use anyhow::Result;
use log::debug;

use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.stat && cap.write && cap.delete && cap.blocking {
        tests.extend(blocking_trials!(
            op,
            test_blocking_delete_file,
            test_blocking_remove_one_file
        ));
        if cap.list_with_recursive {
            tests.extend(blocking_trials!(op, test_blocking_remove_all_basic));
            if !cap.create_dir {
                tests.extend(blocking_trials!(
                    op,
                    test_blocking_remove_all_with_prefix_exists
                ));
            }
        }
    }
}

// Delete existing file should succeed.
pub fn test_blocking_delete_file(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content).expect("write must succeed");

    op.delete(&path)?;

    // Stat it again to check.
    assert!(!op.is_exist(&path)?);

    Ok(())
}

/// Remove one file
pub fn test_blocking_remove_one_file(op: BlockingOperator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());

    op.write(&path, content).expect("write must succeed");

    op.remove(vec![path.clone()])?;

    // Stat it again to check.
    assert!(!op.is_exist(&path)?);

    Ok(())
}

fn test_blocking_remove_all_with_objects(
    op: BlockingOperator,
    parent: String,
    paths: impl IntoIterator<Item = &'static str>,
) -> Result<()> {
    for path in paths {
        let path = format!("{parent}/{path}");
        let (content, _) = gen_bytes(op.info().full_capability());
        op.write(&path, content).expect("write must succeed");
    }

    op.remove_all(&parent)?;

    let found = op
        .lister_with(&format!("{parent}/"))
        .recursive(true)
        .call()
        .expect("list must succeed")
        .into_iter()
        .next()
        .is_some();

    assert!(!found, "all objects should be removed");

    Ok(())
}

/// Remove all under a prefix
pub fn test_blocking_remove_all_basic(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    test_blocking_remove_all_with_objects(op, parent, ["a/b", "a/c", "a/d/e"])
}

/// Remove all under a prefix, while the prefix itself is also an object
pub fn test_blocking_remove_all_with_prefix_exists(op: BlockingOperator) -> Result<()> {
    let parent = uuid::Uuid::new_v4().to_string();
    let (content, _) = gen_bytes(op.info().full_capability());
    op.write(&parent, content).expect("write must succeed");
    test_blocking_remove_all_with_objects(op, parent, ["a", "a/b", "a/c", "a/b/e"])
}
