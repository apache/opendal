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
        ))
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
