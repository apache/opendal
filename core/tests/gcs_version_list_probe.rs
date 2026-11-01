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

#![cfg(all(feature = "tests", feature = "services-gcs"))]

use std::collections::HashSet;
use std::env;

use anyhow::{Result, anyhow, ensure};
use opendal::{ErrorKind, Operator};
use uuid::Uuid;

#[tokio::test]
async fn test_public_gcs_version_list_pagination() -> Result<()> {
    ensure!(
        env::var("OPENDAL_TEST").ok().as_deref() == Some("gcs"),
        "GCS fixture is required"
    );
    ensure!(
        env::var("OPENDAL_DISABLE_RANDOM_ROOT").ok().as_deref() == Some("true"),
        "fixed fixture root is required"
    );
    let op = opendal::tests::init_test_service()
        .map_err(|e| anyhow!("operator initialization failed (kind={:?})", e.kind()))?
        .ok_or_else(|| anyhow!("GCS fixture configuration is missing"))?;
    ensure!(
        op.info().capability().list_with_versions,
        "version-list capability is missing"
    );
    let object_uuid = Uuid::new_v4();
    let path = format!("probe-5548-{object_uuid}/object");
    let mut generations = Vec::with_capacity(2);
    eprintln!("5548 candidate: object_uuid={object_uuid} capability=true public_versions=true");

    let probe: Result<()> = async {
        for payload in [
            b"old-version".as_slice(),
            b"new-version-is-longer".as_slice(),
        ] {
            let metadata = op
                .write(&path, payload.to_vec())
                .await
                .map_err(|e| anyhow!("version write failed (kind={:?})", e.kind()))?;
            let generation = metadata
                .version()
                .filter(|v| v.parse::<u64>().is_ok())
                .ok_or_else(|| anyhow!("write returned no valid generation"))?
                .to_owned();
            eprintln!("5548 write: generation={generation}");
            generations.push(generation);
        }
        ensure!(
            generations[0] != generations[1],
            "writes returned the same generation"
        );
        let entries = op
            .list_with(&path)
            .versions(true)
            .recursive(true)
            .limit(1)
            .await
            .map_err(|e| anyhow!("public version list failed (kind={:?})", e.kind()))?;
        ensure!(
            entries.len() == 2,
            "public list did not return exactly two items"
        );
        ensure!(
            entries.iter().all(|entry| entry.path() == path),
            "public list returned another path"
        );
        let listed: HashSet<_> = entries
            .iter()
            .filter_map(|entry| entry.metadata().version())
            .collect();
        let expected: HashSet<_> = generations.iter().map(String::as_str).collect();
        ensure!(
            listed == expected,
            "public list did not match the write-returned generations"
        );
        eprintln!("5548 list: items=2 generations_match=true");
        Ok(())
    }
    .await;

    // Only generations observed from successful writes are eligible for cleanup.
    // Unknown writes retain their UUID for recovery; never scan or remove a prefix.
    let cleanup = cleanup_generations(&op, &path, &generations).await;
    eprintln!(
        "5548 result: object_uuid={object_uuid} known_generations={} probe_ok={} cleanup_ok={}",
        generations.len(),
        probe.is_ok(),
        cleanup.is_ok()
    );
    match (probe, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(a), Err(b)) => Err(anyhow!("{a}; {b}")),
        (Err(e), _) | (_, Err(e)) => Err(e),
    }
}

async fn cleanup_generations(op: &Operator, path: &str, generations: &[String]) -> Result<()> {
    let mut failures = Vec::new();
    for generation in generations {
        if let Err(e) = op.delete_with(path).version(generation).await {
            failures.push(format!("exact delete failed (kind={:?})", e.kind()));
        }
    }
    for generation in generations {
        let absent = match op.stat_with(path).version(generation).await {
            Err(e) if e.kind() == ErrorKind::NotFound => true,
            Err(e) => {
                failures.push(format!("post-delete stat failed (kind={:?})", e.kind()));
                false
            }
            Ok(_) => {
                failures.push("generation still exists after exact delete".to_owned());
                false
            }
        };
        eprintln!("5548 cleanup: generation={generation} not_found={absent}");
    }
    ensure!(failures.is_empty(), "{}", failures.join("; "));
    Ok(())
}
