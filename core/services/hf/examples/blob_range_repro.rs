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

//! Live reproducer for the HF `/resolve` and CAS-token request amplification
//! seen when fetching many small byte ranges (e.g. lance blob columns) from
//! an `hf://...` operator, and for the fix that collapses it. No lance
//! involved -- just opendal.
//!
//! Runs the same batch of ranges against one file three ways:
//! - **Phase A**: N independent `read_with().range()` calls -- each opens
//!   its own `Reader`, so each independently resolves and fetches a CAS
//!   token. Unavoidable baseline, not what the fix targets.
//! - **Phase B**: one `Reader`, one `.fetch(ranges)` call -- the shape
//!   `object_store::get_ranges` actually uses. Should cost 1 resolve, 0
//!   token requests (reused from Phase A).
//! - **Phase C**: a brand-new `Reader` on the same file, simulating a
//!   dataloader moving to a different file. Should cost its own resolve
//!   (that cache is per-`Reader`) but still 0 token requests (that cache is
//!   shared operator-wide, on `HfCore`).
//!
//! Usage (defaults to a public XET-backed dataset file, no token needed):
//! ```sh
//! cargo run --example blob_range_repro -p opendal-service-hf
//! ```
//! Against your own bucket:
//! ```sh
//! HF_OPENDAL_REPO_TYPE=bucket HF_OPENDAL_REPO_ID=org/bucket-name \
//! HF_OPENDAL_TOKEN=hf_xxx HF_OPENDAL_FILE=path/to/xet-backed/file \
//! cargo run --example blob_range_repro -p opendal-service-hf
//! ```
//! Target file must be XET-backed and large enough for `N_RANGES` ranges
//! spaced `RANGE_GAP` bytes apart.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use http::{Request, Response};
use opendal_core::{
    Buffer, HttpBody, HttpTransport, HttpTransporter, OperationContext, Operator, Result,
};
use opendal_service_hf::Hf;

const N_RANGES: usize = 16;
const RANGE_LEN: u64 = 4 * 1024;
/// Gap between ranges. Must be non-zero -- Phase B pins `.gap(0)` on its
/// `Reader`, and even a tiny real byte gap keeps ranges from being merged
/// into fewer `open()` calls before the fix even gets exercised.
const RANGE_GAP: u64 = 1024;

/// Wraps a real transport and tallies requests by endpoint, so we can see
/// which HF API surface gets hit and how often.
struct CountingTransport<T> {
    inner: T,
    counts: Mutex<BTreeMap<&'static str, u64>>,
}

impl<T> CountingTransport<T> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            counts: Mutex::new(BTreeMap::new()),
        }
    }

    fn classify(path: &str) -> &'static str {
        if path.contains("/resolve/") {
            "resolve"
        } else if path.contains("xet-read-token") || path.contains("xet-write-token") {
            "xet-token"
        } else if path.contains("paths-info") {
            "paths-info"
        } else {
            "other"
        }
    }

    fn count(&self, kind: &str) -> u64 {
        *self.counts.lock().unwrap().get(kind).unwrap_or(&0)
    }

    fn reset(&self) {
        self.counts.lock().unwrap().clear();
    }

    fn report(&self, label: &str) {
        let counts = self.counts.lock().unwrap();
        println!("--- {label} ---");
        for (kind, count) in counts.iter() {
            println!("{count:>3}  {kind}");
        }
    }
}

impl<T: HttpTransport> HttpTransport for CountingTransport<T> {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        let kind = Self::classify(req.uri().path());
        *self.counts.lock().unwrap().entry(kind).or_default() += 1;
        if std::env::var("HF_OPENDAL_VERBOSE").is_ok() {
            eprintln!("-> {} {} [{kind}]", req.method(), req.uri());
        }
        self.inner.fetch(req).await
    }
}

/// `Arc<CountingTransport<T>>` isn't itself an `HttpTransport` (blanket
/// impls don't cover `Arc<T>`), so forward through it.
struct CountingProxy<T>(Arc<CountingTransport<T>>);

impl<T: HttpTransport> HttpTransport for CountingProxy<T> {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.0.fetch(req).await
    }
}

async fn run_phase(op: &Operator, path: &str, ranges: &[std::ops::Range<u64>]) -> Result<()> {
    let reader = op.reader_with(path).gap(0).await?;
    reader.fetch(ranges.to_vec()).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let repo_type = std::env::var("HF_OPENDAL_REPO_TYPE").unwrap_or_else(|_| "dataset".to_string());
    let repo_id = std::env::var("HF_OPENDAL_REPO_ID")
        .unwrap_or_else(|_| "google-research-datasets/mbpp".to_string());
    let token = std::env::var("HF_OPENDAL_TOKEN").unwrap_or_default();
    let path = std::env::var("HF_OPENDAL_FILE")
        .unwrap_or_else(|_| "full/train-00000-of-00001.parquet".to_string());

    let op = Operator::new(
        Hf::default()
            .repo_type(&repo_type)
            .repo_id(&repo_id)
            .token(&token),
    )?;

    let counting = Arc::new(CountingTransport::new(
        opendal_http_transport_reqwest::ReqwestTransport::default(),
    ));
    let op = op.with_context(
        OperationContext::new()
            .with_http_transport(HttpTransporter::new(CountingProxy(counting.clone()))),
    );

    let stride = RANGE_LEN + RANGE_GAP;
    let needed = stride * N_RANGES as u64;
    let size = op.stat(&path).await?.content_length();
    assert!(
        size >= needed,
        "{path} is only {size} bytes; need at least {needed} to space \
         {N_RANGES} ranges {RANGE_GAP} bytes apart -- pick a larger file \
         or shrink N_RANGES/RANGE_GAP"
    );
    let ranges: Vec<_> = (0..N_RANGES as u64)
        .map(|i| i * stride..i * stride + RANGE_LEN)
        .collect();

    println!("Target: {repo_type}/{repo_id} :: {path} ({size} bytes)\n");

    println!(">>> Phase A: {N_RANGES} independent single-range reads");
    for range in &ranges {
        op.read_with(&path).range(range.clone()).await?;
    }
    counting.report("Phase A");
    counting.reset();

    println!("\n>>> Phase B: one Reader, one fetch({N_RANGES} ranges)");
    run_phase(&op, &path, &ranges).await?;
    counting.report("Phase B");
    let phase_b_resolves = counting.count("resolve");
    let phase_b_tokens = counting.count("xet-token");
    counting.reset();

    println!("\n>>> Phase C: a new Reader (simulating a different file)");
    run_phase(&op, &path, &ranges).await?;
    counting.report("Phase C");
    let phase_c_resolves = counting.count("resolve");
    let phase_c_tokens = counting.count("xet-token");

    println!(
        "\nExpected: Phase B and C each show 1 resolve (per-Reader cache; got {phase_b_resolves} \
         and {phase_c_resolves}), and 0 xet-token (shared operator-wide cache, warmed by Phase A; \
         got {phase_b_tokens} and {phase_c_tokens})."
    );

    Ok(())
}
