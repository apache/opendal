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

use std::future::{Future, IntoFuture};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::task::{ArcWake, waker};
use opendal::{ErrorKind, Operator, Result, services};
use tokio::sync::{Barrier, Notify};

const FIRST: &[u8] = b"first complete source";
const SECOND: &[u8] = b"second complete source";

fn operators() -> Result<(Operator, Operator)> {
    let node = std::env::var("OPENDAL_HDFS_NATIVE_NAME_NODE")
        .expect("the real Hadoop fixture must provide its NameNode URI");
    let root = format!("/tmp/opendal-pr8090/{}/", uuid::Uuid::new_v4());
    let first = Operator::new(services::HdfsNative::default().root(&root).name_node(&node))?;
    let second = Operator::new(services::HdfsNative::default().root(&root).name_node(&node))?;
    Ok((first, second))
}

#[derive(Default)]
struct WakeGate(Notify);

impl ArcWake for WakeGate {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.notify_one();
    }
}

#[derive(Debug)]
struct Interleaving {
    placeholder_observed: bool,
    competing_write_error: Option<ErrorKind>,
    competing_data_observed: bool,
    rename_error: Option<ErrorKind>,
    destination: Vec<u8>,
    source_exists: bool,
}

async fn observe_interleaving(first: &Operator, second: &Operator) -> Result<Interleaving> {
    first.write("source", FIRST.to_vec()).await?;
    // Warm the independent observer's connection before the rename starts.
    assert_eq!(second.read("source").await?.to_vec(), FIRST);
    let mut rename = Box::pin(
        first
            .rename_with("source", "destination")
            .if_not_exists(true)
            .into_future(),
    );
    let gate = Arc::new(WakeGate::default());
    let wake = waker(gate.clone());
    let mut placeholder_observed = false;
    let mut competing_write_error = None;
    let mut competing_data_observed = false;
    let rename_result = loop {
        let progress = {
            let mut context = Context::from_waker(&wake);
            rename.as_mut().poll(&mut context)
        };
        if let Poll::Ready(result) = progress {
            break result;
        }
        gate.0.notified().await;
        // A wake has no RPC-stage meaning. Observe actual service state while
        // the first caller remains unpolled, then let that caller continue.
        if !placeholder_observed {
            match second.stat("destination").await {
                Ok(metadata) if metadata.is_file() && metadata.content_length() == 0 => {
                    placeholder_observed = true;
                    let written = second.write("destination", SECOND.to_vec()).await;
                    competing_write_error = written.as_ref().err().map(|err| err.kind());
                    if written.is_ok() {
                        competing_data_observed =
                            second.read("destination").await?.to_vec() == SECOND;
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(err),
                _ => {}
            }
        }
    };
    Ok(Interleaving {
        placeholder_observed,
        competing_write_error,
        competing_data_observed,
        rename_error: rename_result.as_ref().err().map(|err| err.kind()),
        destination: second.read("destination").await?.to_vec(),
        source_exists: second.exists("source").await?,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_rename_does_not_replace_a_completed_competing_write() {
    let (first, second) = operators().expect("construct two real HDFS operators");
    let result = tokio::time::timeout(
        Duration::from_secs(45),
        observe_interleaving(&first, &second),
    )
    .await;
    let cleanup = second.remove_all("").await;
    cleanup.expect("remove only this test's unique HDFS root");
    let observed = result.expect("bounded service probe").expect("HDFS I/O");
    println!("INTERLEAVING {observed:?}");
    if observed.competing_data_observed {
        assert_eq!(
            observed.rename_error,
            Some(ErrorKind::ConditionNotMatch),
            "CONCURRENT_DESTINATION_REPLACED: a completed competing write was visible before rename resumed"
        );
        assert_eq!(observed.destination, SECOND);
        assert!(observed.source_exists);
    } else {
        println!(
            "NO_COMPLETED_COMPETING_WRITE placeholder={} write_error={:?}; this branch does not establish a concurrent-write result",
            observed.placeholder_observed, observed.competing_write_error
        );
    }
}

async fn compete(first: &Operator, second: &Operator) -> Result<Vec<String>> {
    let mut violations = Vec::new();
    for trial in 0..8 {
        first.write("first", FIRST.to_vec()).await?;
        second.write("second", SECOND.to_vec()).await?;
        first.delete("destination").await?;
        let barrier = Barrier::new(2);
        let (left, right) = tokio::join!(
            async {
                barrier.wait().await;
                first
                    .rename_with("first", "destination")
                    .if_not_exists(true)
                    .await
            },
            async {
                barrier.wait().await;
                second
                    .rename_with("second", "destination")
                    .if_not_exists(true)
                    .await
            }
        );
        let left_error = left.as_ref().err().map(|err| err.kind());
        let right_error = right.as_ref().err().map(|err| err.kind());
        let destination = first.read("destination").await?.to_vec();
        let first_exists = first.exists("first").await?;
        let second_exists = second.exists("second").await?;
        let valid = if left.is_ok() && right_error == Some(ErrorKind::ConditionNotMatch) {
            destination == FIRST
                && !first_exists
                && second_exists
                && second.read("second").await?.to_vec() == SECOND
        } else if right.is_ok() && left_error == Some(ErrorKind::ConditionNotMatch) {
            destination == SECOND
                && !second_exists
                && first_exists
                && first.read("first").await?.to_vec() == FIRST
        } else {
            false
        };
        let state = format!(
            "trial={trial} left={left_error:?} right={right_error:?} destination={destination:?} first_exists={first_exists} second_exists={second_exists} valid={valid}"
        );
        println!("CONDITIONAL_RACE {state}");
        if !valid {
            violations.push(state);
        }
    }
    Ok(violations)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_rename_reports_one_success_and_one_condition_conflict() {
    let (first, second) = operators().expect("construct two real HDFS operators");
    let result = tokio::time::timeout(Duration::from_secs(60), compete(&first, &second)).await;
    let cleanup = second.remove_all("").await;
    cleanup.expect("remove only this test's unique HDFS root");
    let violations = result.expect("bounded service probe").expect("HDFS I/O");
    assert!(
        violations.is_empty(),
        "CONDITIONAL_RENAME_CONTRACT_VIOLATION: {violations:?}"
    );
}
