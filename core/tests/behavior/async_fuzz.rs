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

use std::io::SeekFrom;
use std::vec;

use anyhow::Result;
use opendal::raw::tests::ReadAction;
use opendal::raw::tests::ReadChecker;
use opendal::raw::BytesRange;

use crate::*;

// pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
//     tests.extend(async_trials!(op))
// }

async fn test_fuzz_read(
    op: Operator,
    size: usize,
    range: impl Into<BytesRange>,
    actions: &[ReadAction],
) -> Result<()> {
    let cap = op.info().full_capability();

    if !(cap.read && cap.write & cap.read_with_range) {
        return Ok(());
    }

    let path = uuid::Uuid::new_v4().to_string();

    let mut checker = ReadChecker::new(size, range.into());

    op.write(&path, checker.data())
        .await
        .expect("write must succeed");

    let r = op.reader(&path).await.expect("reader must be created");

    checker.check(r, actions).await;
    Ok(())
}
