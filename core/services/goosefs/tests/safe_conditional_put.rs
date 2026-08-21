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

//! Acceptance tests for GooseFS safe Create / conditional rename.
//!
//! Requires a live GooseFS master. Skip unless `OPENDAL_GOOSEFS_MASTER_ADDR`
//! is set (same env the behavior CI fixture exports).

use std::sync::Arc;

use opendal::ErrorKind;
use opendal::Operator;
use opendal::services::GooseFs;

fn maybe_operator() -> Option<Operator> {
    let master = std::env::var("OPENDAL_GOOSEFS_MASTER_ADDR").ok()?;
    if master.trim().is_empty() {
        return None;
    }
    let root = std::env::var("OPENDAL_GOOSEFS_ROOT").unwrap_or_else(|_| "/".to_string());
    let write_type =
        std::env::var("OPENDAL_GOOSEFS_WRITE_TYPE").unwrap_or_else(|_| "must_cache".to_string());

    let builder = GooseFs::default()
        .root(&root)
        .master_addr(&master)
        .write_type(&write_type);
    Some(Operator::new(builder).expect("build GooseFS operator"))
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[tokio::test]
async fn capability_declares_rename_with_if_not_exists() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };
    let cap = op.info().capability();
    assert!(cap.write_with_if_not_exists);
    assert!(cap.rename_with_if_not_exists);
}

/// Conditional rename against an existing dst → ConditionNotMatch;
/// destination content is unchanged (Master no-replace).
#[tokio::test]
async fn rename_if_not_exists_rejects_existing_dst() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };

    let src = unique("cond-rename-src");
    let dst = unique("cond-rename-dst");
    op.write(&src, "from-src").await.expect("write src");
    op.write(&dst, "dst-original").await.expect("write dst");

    let err = op
        .rename_with(&src, &dst)
        .if_not_exists(true)
        .await
        .expect_err("rename must fail when dst exists");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

    let dst_after = op.read(&dst).await.expect("read dst").to_bytes();
    assert_eq!(&dst_after[..], b"dst-original");

    let _ = op.delete(&src).await;
    let _ = op.delete(&dst).await;
}

/// Serial second Create (write_with if_not_exists) must fail.
#[tokio::test]
async fn write_if_not_exists_serial_conflict() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };

    let path = unique("serial-create");
    op.write_with(&path, "winner")
        .if_not_exists(true)
        .await
        .expect("first Create succeeds");

    let err = op
        .write_with(&path, "loser")
        .if_not_exists(true)
        .await
        .expect_err("second Create must fail");
    assert_eq!(err.kind(), ErrorKind::ConditionNotMatch);

    let body = op.read(&path).await.expect("read").to_bytes();
    assert_eq!(&body[..], b"winner");

    let meta = op.stat(&path).await.expect("stat");
    assert!(
        meta.etag().is_some(),
        "etag (file_id) should be present after write"
    );

    let _ = op.delete(&path).await;
}

/// Overwrite (if_not_exists=false) still replaces an existing file.
#[tokio::test]
async fn overwrite_rename_still_works() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };

    let src = unique("overwrite-src");
    let dst = unique("overwrite-dst");
    op.write(&src, "new-content").await.expect("write src");
    op.write(&dst, "old-content").await.expect("write dst");

    op.rename(&src, &dst).await.expect("overwrite rename");

    let body = op.read(&dst).await.expect("read dst").to_bytes();
    assert_eq!(&body[..], b"new-content");

    let _ = op.delete(&dst).await;
}

/// A rename whose source is missing must leave the destination intact.
/// Master rename is attempted before the overwrite delete, so the doomed
/// rename cannot destroy `dst`.
#[tokio::test]
async fn rename_missing_source_preserves_dst() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };

    let src = unique("missing-src");
    let dst = unique("missing-src-dst");
    op.write(&dst, "dst-original").await.expect("write dst");

    let err = op
        .rename(&src, &dst)
        .await
        .expect_err("rename must fail when src is missing");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let dst_after = op
        .read(&dst)
        .await
        .expect("dst must survive a failed rename")
        .to_bytes();
    assert_eq!(&dst_after[..], b"dst-original");

    let _ = op.delete(&dst).await;
}

/// Concurrent Create on the same path — exactly one wins; content is winner's.
#[tokio::test]
async fn concurrent_write_if_not_exists_exactly_one_wins() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };
    let op = Arc::new(op);
    let path = unique("concurrent-create");

    let rounds = 8u32;
    for round in 0..rounds {
        let p = format!("{path}-r{round}");
        let op_a = op.clone();
        let op_b = op.clone();
        let path_a = p.clone();
        let path_b = p.clone();

        let (ra, rb) = tokio::join!(
            async move {
                op_a.write_with(&path_a, "A")
                    .if_not_exists(true)
                    .await
                    .map(|_| "A")
            },
            async move {
                op_b.write_with(&path_b, "B")
                    .if_not_exists(true)
                    .await
                    .map(|_| "B")
            },
        );

        match (&ra, &rb) {
            (Ok(w), Err(e)) => {
                assert_eq!(e.kind(), ErrorKind::ConditionNotMatch);
                let body = op.read(&p).await.expect("read").to_bytes();
                assert_eq!(&body[..], w.as_bytes());
            }
            (Err(e), Ok(w)) => {
                assert_eq!(e.kind(), ErrorKind::ConditionNotMatch);
                let body = op.read(&p).await.expect("read").to_bytes();
                assert_eq!(&body[..], w.as_bytes());
            }
            (Ok(_), Ok(_)) => panic!("both concurrent Creates succeeded for {p}"),
            (Err(ea), Err(eb)) => panic!("both concurrent Creates failed for {p}: {ea:?} / {eb:?}"),
        }

        let _ = op.delete(&p).await;
    }
}

/// Re-closing through the public API must never disturb published data.
///
/// Note this exercises `CompleteWriter`, not the service writer: after a
/// *successful* close it drops its inner writer, so the second call is
/// rejected upstream and never reaches GooseFS. The service-level guard
/// matters on the other branch — `CompleteWriter` deliberately keeps the
/// inner writer when close *fails* so `RetryLayer` can retry it (see the
/// comment in `layers/complete.rs`), and that retry is what used to fall
/// into the zero-write path. That path is covered by the unit tests in
/// `writer.rs`; this test is the end-to-end canary for the layer contract.
#[tokio::test]
async fn public_double_close_leaves_data_intact() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };
    let path = unique("double-close");
    let payload = b"payload-that-must-survive-a-repeated-close";

    let mut w = op.writer(&path).await.expect("open writer");
    w.write(payload.to_vec()).await.expect("write");
    w.close().await.expect("first close");

    assert!(
        w.close().await.is_err(),
        "a second close must be rejected, not silently republish the target"
    );

    let after = op.read(&path).await.expect("read back").to_bytes();
    assert_eq!(
        &after[..],
        &payload[..],
        "second close() clobbered the data"
    );

    let _ = op.delete(&path).await;
}

/// An aborted write must leave nothing behind, and must not be resurrectable
/// as an empty object by a trailing `close()`.
#[tokio::test]
async fn close_after_abort_leaves_no_object() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };
    let path = unique("abort-then-close");

    let mut w = op.writer(&path).await.expect("open writer");
    w.write(b"discarded".to_vec()).await.expect("write");
    w.abort().await.expect("abort");

    let _ = w.close().await;

    let err = op
        .stat(&path)
        .await
        .expect_err("aborted write must leave nothing behind");
    assert_eq!(err.kind(), ErrorKind::NotFound);
}

/// The state machine must not regress OpenDAL's `write(path, "")` contract:
/// closing a writer that never received data still materialises an empty
/// object.
#[tokio::test]
async fn close_without_write_creates_empty_object() {
    let Some(op) = maybe_operator() else {
        eprintln!("skip: OPENDAL_GOOSEFS_MASTER_ADDR unset");
        return;
    };
    let path = unique("zero-write");

    let mut w = op.writer(&path).await.expect("open writer");
    w.close().await.expect("close without any write");

    let meta = op.stat(&path).await.expect("empty object must exist");
    assert_eq!(meta.content_length(), 0);

    let _ = op.delete(&path).await;
}
