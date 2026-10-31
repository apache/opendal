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

use std::env;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use anyhow::ensure;
use http::header::CONTENT_LENGTH;
use opendal::ErrorKind;
use opendal::Operator;
use opendal::options;
use opendal::raw::PresignedRequest;
use reqwest::Client;
use reqwest::Response;
use reqwest::Url;
use uuid::Uuid;

const OLD_PAYLOAD: &[u8] = b"gcs-version-presign-old";
const NEW_PAYLOAD: &[u8] = b"gcs-version-presign-new-is-longer";
const PRESIGN_TTL: Duration = Duration::from_secs(300);
const GENERATION_HEADER: &str = "x-goog-generation";

#[derive(Default)]
struct ProbeState {
    path: String,
    old_version: Option<String>,
    new_version: Option<String>,
}

impl ProbeState {
    fn versions(&self) -> Vec<(&'static str, String)> {
        let mut versions = Vec::with_capacity(2);

        if let Some(version) = &self.old_version {
            versions.push(("old", version.clone()));
        }
        if let Some(version) = &self.new_version {
            versions.push(("new", version.clone()));
        }

        versions
    }
}

#[tokio::test]
async fn test_versioned_presign_get_and_head() -> Result<()> {
    ensure!(
        env::var("OPENDAL_TEST").ok().as_deref() == Some("gcs"),
        "OPENDAL_TEST must be set to gcs"
    );
    ensure!(
        env::var("OPENDAL_DISABLE_RANDOM_ROOT").ok().as_deref() == Some("true"),
        "OPENDAL_DISABLE_RANDOM_ROOT must be true for the task's recoverable object path"
    );

    let op = init_operator()?;
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| anyhow!("presigned HTTP client setup failed"))?;

    let suffix = Uuid::new_v4().to_string();
    let mut state = ProbeState {
        path: format!("pr-8389-versioned-presign-{suffix}"),
        ..Default::default()
    };

    // Every reusable resource and payload is ready before the first write.
    eprintln!("PR_REVIEW_8389 object_path={}", state.path);
    let probe_result = run_probe(&op, &client, &mut state).await;
    // An unknown write result has no generation to delete. Keep the UUID for
    // follow-up instead of falling back to a namespace scan.
    let cleanup_result = cleanup_known_versions(&op, &state).await;
    let probe_ok = probe_result.is_ok();
    let cleanup_ok = cleanup_result.is_ok();

    eprintln!(
        "PR_REVIEW_8389 cleanup: object_path={} \
         old_generation={} new_generation={} probe_ok={probe_ok} cleanup_ok={cleanup_ok}",
        state.path,
        state.old_version.as_deref().unwrap_or("unknown"),
        state.new_version.as_deref().unwrap_or("unknown"),
    );

    match (probe_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(probe), Err(cleanup)) => Err(anyhow!("{probe}; {cleanup}")),
        (Err(probe), Ok(())) => Err(probe),
        (Ok(()), Err(cleanup)) => Err(cleanup),
    }
}

fn init_operator() -> Result<Operator> {
    let op = opendal::tests::init_test_service()
        .map_err(|err| redacted_service_error("operator initialization", err))?;
    op.ok_or_else(|| anyhow!("GCS operator configuration is missing"))
}

async fn run_probe(op: &Operator, client: &Client, state: &mut ProbeState) -> Result<()> {
    let old_metadata = op
        .write(&state.path, OLD_PAYLOAD.to_vec())
        .await
        .map_err(|err| redacted_service_error("old-version write", err))?;
    let old_version = old_metadata
        .version()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("old-version write returned no generation"))?;
    state.old_version = Some(old_version.clone());
    eprintln!("PR_REVIEW_8389 old_generation={old_version}");

    let new_metadata = op
        .write(&state.path, NEW_PAYLOAD.to_vec())
        .await
        .map_err(|err| redacted_service_error("new-version write", err))?;
    let new_version = new_metadata
        .version()
        .map(str::to_owned)
        .ok_or_else(|| anyhow!("new-version write returned no generation"))?;
    state.new_version = Some(new_version.clone());
    eprintln!("PR_REVIEW_8389 new_generation={new_version}");

    ensure!(
        old_version != new_version,
        "GCS returned the same generation for the old and new payloads"
    );

    let direct_old = op
        .read_with(&state.path)
        .version(&old_version)
        .await
        .map_err(|err| redacted_service_error("direct old-version read", err))?
        .to_vec();
    ensure!(
        direct_old.as_slice() == OLD_PAYLOAD,
        "direct old-version read returned different bytes"
    );

    let signed_get = op
        .presign_read_options(
            &state.path,
            PRESIGN_TTL,
            options::ReadOptions {
                version: Some(old_version.clone()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| redacted_service_error("versioned read presign", err))?;
    verify_presigned_get(client, &signed_get).await?;

    let signed_head = op
        .presign_stat_options(
            &state.path,
            PRESIGN_TTL,
            options::StatOptions {
                version: Some(old_version.clone()),
                ..Default::default()
            },
        )
        .await
        .map_err(|err| redacted_service_error("versioned stat presign", err))?;
    verify_presigned_head(client, &signed_head, &old_version).await
}

async fn verify_presigned_get(client: &Client, signed: &PresignedRequest) -> Result<()> {
    let response = send_presigned(client, signed, "GET").await?;
    let status = response.status();
    ensure!(
        status.is_success(),
        "presigned GET returned HTTP status {status}"
    );

    let body = response
        .bytes()
        .await
        .map_err(|_| anyhow!("presigned GET body read failed"))?;
    ensure!(
        body.as_ref() == OLD_PAYLOAD,
        "presigned GET returned bytes different from the old payload"
    );

    Ok(())
}

async fn verify_presigned_head(
    client: &Client,
    signed: &PresignedRequest,
    old_version: &str,
) -> Result<()> {
    let response = send_presigned(client, signed, "HEAD").await?;
    let status = response.status();
    ensure!(
        status.is_success(),
        "presigned HEAD returned HTTP status {status}"
    );

    let generation = response
        .headers()
        .get(GENERATION_HEADER)
        .ok_or_else(|| anyhow!("presigned HEAD returned no generation header"))?
        .to_str()
        .map_err(|_| anyhow!("presigned HEAD returned an invalid generation header"))?;
    ensure!(
        generation == old_version,
        "presigned HEAD generation did not match the old generation"
    );

    let content_length = response
        .headers()
        .get(CONTENT_LENGTH)
        .ok_or_else(|| anyhow!("presigned HEAD returned no content-length header"))?
        .to_str()
        .map_err(|_| anyhow!("presigned HEAD returned an invalid content-length header"))?
        .parse::<u64>()
        .map_err(|_| anyhow!("presigned HEAD content-length was not an integer"))?;
    ensure!(
        content_length == OLD_PAYLOAD.len() as u64,
        "presigned HEAD content-length did not match the old payload"
    );

    Ok(())
}

async fn send_presigned(
    client: &Client,
    signed: &PresignedRequest,
    method: &'static str,
) -> Result<Response> {
    ensure!(
        signed.method().as_str() == method,
        "presign returned an unexpected HTTP method"
    );
    let url = signed.uri().to_string();
    let url =
        Url::parse(&url).map_err(|_| anyhow!("presigned {method} URL construction failed"))?;
    let mut request = client.request(signed.method().clone(), url);

    for (key, value) in signed.header() {
        request = request.header(key, value);
    }

    // Request-building and transport errors may contain the signed URL, so
    // callers receive fixed messages instead of the original error.
    let request = request
        .build()
        .map_err(|_| anyhow!("presigned {method} request construction failed"))?;
    client
        .execute(request)
        .await
        .map_err(|_| anyhow!("presigned {method} request failed"))
}

async fn cleanup_known_versions(op: &Operator, state: &ProbeState) -> Result<()> {
    // Cleanup is deliberately limited to generations observed from stat.
    // Never list or remove a prefix: an unknown write result is reported by
    // suffix and left for explicit follow-up.
    let versions = state.versions();
    if versions.is_empty() {
        return Ok(());
    }

    let mut failures = Vec::new();
    for (label, version) in &versions {
        if let Err(err) = op.delete_with(&state.path).version(version).await {
            failures.push(format!(
                "{label} generation exact delete failed (kind={:?})",
                err.kind()
            ));
        }
    }

    for (label, version) in &versions {
        match op.stat_with(&state.path).version(version).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => failures.push(format!(
                "{label} generation post-delete stat failed (kind={:?})",
                err.kind()
            )),
            Ok(_) => failures.push(format!(
                "{label} generation still exists after exact delete"
            )),
        }
    }

    if failures.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "known-generation cleanup failed: {}",
            failures.join("; ")
        ))
    }
}

// Service errors can retain signed URIs; expose only their error class.
fn redacted_service_error(operation: &'static str, err: opendal::Error) -> anyhow::Error {
    anyhow!("{operation} failed (kind={:?})", err.kind())
}
