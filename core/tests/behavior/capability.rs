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

use std::time::Duration;

use anyhow::Result;
use log::debug;
use opendal::ErrorKind;
use opendal::Operator;

/// Test the capability check for all operations.
///
/// All declared tests will be executed if the operator exists.
macro_rules! behavior_capability_check_test {
    ($service:ident, $($(#[$meta:meta])* $test:ident),*,) => {
        paste::item! {
            $(
                #[test]
                $(
                    #[$meta]
                )*
                fn [<capability_check_ $test >]() -> anyhow::Result<()> {
                    match OPERATOR.as_ref() {
                        Some(op) => RUNTIME.block_on($crate::capability::$test(op.clone())),
                        None => {
                            Ok(())
                        }
                    }
                }
            )*
        }
    }
}

#[macro_export]
macro_rules! behavior_capability_check_tests {
    ($($service:ident),*) => {
        $(
            behavior_capability_check_test!(
                $service,

                test_read_file,
                test_presign,
            );
        )*
    };
}

pub async fn test_read_file(op: Operator) -> Result<()> {
    let res = op.read("normal_file").await;

    if op.info().can_read() {
        if let Err(err) = res {
            assert_ne!(err.kind(), ErrorKind::Unsupported);
        };
    } else {
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);
    }

    Ok(())
}

pub async fn test_presign(op: Operator) -> Result<()> {
    let path = uuid::Uuid::new_v4().to_string();
    debug!("Generate a random file: {}", &path);

    let res = op.presign_read(&path, Duration::from_secs(3600)).await;

    if op.info().can_presign() {
        if let Err(err) = res {
            assert_ne!(err.kind(), ErrorKind::Unsupported);
        };
    } else {
        assert_eq!(res.unwrap_err().kind(), ErrorKind::Unsupported);
    }

    Ok(())
}
