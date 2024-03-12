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

pub fn tests(op: &Operator, tests: &mut Vec<Trial>) {
    tests.extend(async_trials!(
        op,
        test_fuzz_issue_2717,
        test_fuzz_pr_3395_case_1,
        test_fuzz_pr_3395_case_2
    ))
}

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

/// This fuzz test is to reproduce <https://github.com/apache/opendal/issues/2717>.
///
/// The simplified cases could be seen as:
///
/// ```
/// FuzzInput {
///     actions: [
///         Seek(
///             End(
///                 -2,
///             ),
///         ),
///         Read {
///             size: 0,
///         },
///     ],
///     data: [
///         0,
///         0,
///     ],
///     range: (
///         1,
///         2,
///     )
///   ]
/// }
/// ```
///
/// Which means:
///
/// - A file with 2 bytes of content.
/// - Open as an range reader of `1..2`.
/// - Seek to `End(-2)` first
///
/// The expected result is seek returns InvalidInput error because the seek position
/// is invalid for given range `1..2`. However, the actual behavior is we seek to `0`
/// and results in a panic.
pub async fn test_fuzz_issue_2717(op: Operator) -> Result<()> {
    let actions = [ReadAction::Seek(SeekFrom::End(-2))];

    test_fuzz_read(op, 2, .., &actions).await
}

/// This fuzz test is to reproduce bug inside <https://github.com/apache/opendal/pull/3395>.
///
/// The simplified cases could be seen as:
///
/// ```
/// FuzzInput {
///     path: "06ae5d93-c0e9-43f2-ae5a-225cfaaa40a0",
///     size: 1,
///     range: BytesRange(Some(0), None),
///     actions: [Seek(Current(1)), Next, Seek(End(-1))],
/// }
/// ```
pub async fn test_fuzz_pr_3395_case_1(op: Operator) -> Result<()> {
    let actions = [
        ReadAction::Seek(SeekFrom::Current(1)),
        ReadAction::Read(1024),
        ReadAction::Seek(SeekFrom::End(-1)),
    ];
    test_fuzz_read(op, 1, 0.., &actions).await
}

/// This fuzz test is to reproduce bug inside <https://github.com/apache/opendal/pull/3395>.
///
/// The simplified cases could be seen as:
///
/// ```
/// FuzzInput {
///     path: "e6056989-7c7c-4075-b975-5ae380884333",
///     size: 1,
///     range: BytesRange(Some(0), None),
///     actions: [Next, Seek(Current(1)), Next, Seek(End(0))],
/// }
/// ```
pub async fn test_fuzz_pr_3395_case_2(op: Operator) -> Result<()> {
    let actions = [
        ReadAction::Read(1024),
        ReadAction::Seek(SeekFrom::Current(1)),
        ReadAction::Read(1024),
        ReadAction::Seek(SeekFrom::End(0)),
    ];
    test_fuzz_read(op, 1, 0.., &actions).await
}
