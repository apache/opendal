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

use crate::*;

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    let cap = op.info().full_capability();

    if cap.stat && cap.write && cap.create_dir && cap.blocking {
        tests.extend(blocking_trials!(
            op,
            test_blocking_create_dir,
            test_blocking_create_dir_existing
        ))
    }
}

/// Create dir with dir path should succeed.
pub fn test_blocking_create_dir(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path)?;

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}

/// Create dir on existing dir should succeed.
pub fn test_blocking_create_dir_existing(op: BlockingOperator) -> Result<()> {
    if !op.info().full_capability().create_dir {
        return Ok(());
    }

    let path = format!("{}/", uuid::Uuid::new_v4());

    op.create_dir(&path)?;

    op.create_dir(&path)?;

    let meta = op.stat(&path)?;
    assert_eq!(meta.mode(), EntryMode::DIR);

    op.delete(&path).expect("delete must succeed");
    Ok(())
}
