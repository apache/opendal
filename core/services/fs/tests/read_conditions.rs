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

use std::fs::File;
use std::io::Write;
use std::time::Duration;

use opendal_core::options::ReadOptions;
use opendal_core::raw::Timestamp;
use opendal_core::{ErrorKind, Operator};
use opendal_service_fs::Fs;

#[tokio::test]
async fn read_time_conditions() {
    check_read_time_conditions(false).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn direct_io_read_time_conditions() {
    check_read_time_conditions(true).await;
}

async fn check_read_time_conditions(direct_io: bool) {
    let root = tempfile::tempdir().unwrap();
    let contents = b"conditional read";
    let mut file = File::create(root.path().join("data")).unwrap();
    file.write_all(contents).unwrap();
    let timestamp = Timestamp::new(1_700_000_000, 500_000_000).unwrap();
    file.set_modified(timestamp.into()).unwrap();
    let modified = Timestamp::try_from(file.metadata().unwrap().modified().unwrap()).unwrap();
    drop(file);

    let op = Operator::new(
        Fs::default()
            .root(root.path().to_str().unwrap())
            .direct_io(direct_io),
    )
    .unwrap();
    if direct_io {
        // The test directory may be on a filesystem without direct I/O support.
        match op.read("data").await {
            Ok(data) => assert_eq!(data.to_bytes(), contents.as_slice()),
            Err(err) if err.kind() == ErrorKind::Unsupported => {
                eprintln!("Skipping direct I/O conditions: {err}");
                return;
            }
            Err(err) => panic!("Direct I/O probe failed: {err}"),
        }
    }

    let earlier = modified - Duration::from_nanos(1);
    for (if_modified_since, if_unmodified_since, matches) in [
        (Some(earlier), None, true),
        (Some(modified), None, false),
        (None, Some(modified), true),
        (None, Some(earlier), false),
        (Some(earlier), Some(modified), true),
        (Some(modified), Some(modified), false),
        (Some(earlier), Some(earlier), false),
    ] {
        let result = op
            .read_options(
                "data",
                ReadOptions {
                    if_modified_since,
                    if_unmodified_since,
                    ..Default::default()
                },
            )
            .await;
        if matches {
            assert_eq!(result.unwrap().to_bytes(), contents.as_slice());
        } else {
            let err = result.unwrap_err();
            assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);
            assert!(!err.is_temporary());
        }
    }

    let err = op
        .read_with("missing")
        .if_modified_since(modified)
        .if_unmodified_since(earlier)
        .await
        .unwrap_err();
    assert_eq!(err.kind(), ErrorKind::NotFound);
}
