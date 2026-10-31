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

#![cfg(unix)]

use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use futures::stream;
use http::Request;
use http::Response;
use http::header::ACCEPT_ENCODING;
use http::header::CONTENT_ENCODING;
use opendal::Buffer;
use opendal::Builder as _;
use opendal::ErrorKind;
use opendal::HttpBody;
use opendal::HttpTransport;
use opendal::HttpTransporter;
use opendal::OperationContext;
use opendal::Operator;
use opendal::raw::Servicer;
use opendal::raw::oio::ReadStream;
use opendal::services::Gcs;
use opendal_http_transport_reqwest::ReqwestTransport;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use uuid::Uuid;

const CREDENTIAL_ENV: &str = "OPENDAL_GCS_CREDENTIAL";
const EXPECTED_ENV: &str = "PR_REVIEW_8421_EXPECTED";
const PROBE_PAYLOAD: &[u8] = b"opendal-pr-review-8421";

struct TemporaryCredential {
    path: PathBuf,
}

impl TemporaryCredential {
    fn create() -> Result<Self> {
        let encoded =
            std::env::var(CREDENTIAL_ENV).context("GCS fixture credential is unavailable")?;
        let mut content = STANDARD
            .decode(encoded.trim())
            .context("GCS fixture credential is not valid base64")?;
        if !content.starts_with(b"{") {
            bail!("decoded GCS fixture credential is not a JSON object");
        }

        let path =
            std::env::temp_dir().join(format!("opendal-pr-review-8421-{}.json", Uuid::new_v4()));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .context("failed to create temporary GCS credential file")?;
        let credential = Self { path };
        let write_result = file
            .write_all(&content)
            .context("failed to write temporary GCS credential file");
        content.fill(0);
        write_result?;
        file.sync_all()
            .context("failed to sync temporary GCS credential file")?;

        Ok(credential)
    }
}

impl Drop for TemporaryCredential {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Clone, Debug)]
struct TokenObservation {
    status: u16,
    content_encoding: Option<String>,
    gzip_magic: bool,
}

#[derive(Clone)]
struct TokenObservingTransport {
    inner: ReqwestTransport,
    observations: Arc<Mutex<Vec<TokenObservation>>>,
}

impl TokenObservingTransport {
    fn new(inner: ReqwestTransport) -> Self {
        Self {
            inner,
            observations: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

fn is_google_token_request(req: &Request<Buffer>) -> bool {
    req.uri().scheme_str() == Some("https")
        && req.uri().host() == Some("oauth2.googleapis.com")
        && req.uri().path() == "/token"
}

impl HttpTransport for TokenObservingTransport {
    async fn fetch(&self, req: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        let observe_token = is_google_token_request(&req);
        let resp = self.inner.fetch(req).await?;
        if !observe_token {
            return Ok(resp);
        }

        let status = resp.status().as_u16();
        let content_encoding = resp
            .headers()
            .get(CONTENT_ENCODING)
            .and_then(|value| value.to_str().ok())
            .map(str::to_ascii_lowercase);
        let (parts, mut body) = resp.into_parts();
        let buffer = body.read_all().await?;
        let bytes = buffer.to_bytes();
        let gzip_magic = bytes.starts_with(&[0x1f, 0x8b]);

        self.observations
            .lock()
            .expect("lock poisoned")
            .push(TokenObservation {
                status,
                content_encoding,
                gzip_magic,
            });

        let size = bytes.len() as u64;
        let body = HttpBody::new(stream::iter(vec![Ok(Buffer::from(bytes))]), Some(size));
        Ok(Response::from_parts(parts, body))
    }
}

fn gcs_builder(bucket: &str, root: &str, credential_path: &str) -> Gcs {
    Gcs::default()
        .bucket(bucket)
        .root(root)
        .credential_path(credential_path)
        .disable_config_load()
        .disable_vm_metadata()
}

fn build_operator(
    bucket: &str,
    root: &str,
    credential_path: &str,
    transport: HttpTransporter,
) -> Result<Operator> {
    let operator = Operator::new(gcs_builder(bucket, root, credential_path))?
        .with_context(OperationContext::new().with_http_transport(transport));
    Ok(operator)
}

fn build_exact_cleanup_operator(
    bucket: &str,
    root: &str,
    credential_path: &str,
) -> Result<Operator> {
    // GCS maps OpDelete.version to the generation query, but this versioned
    // capability is not advertised by the service. The task-only cleanup uses
    // the public low-level constructor so CorrectnessCheckLayer does not reject
    // the service-supported version delete.
    let service = Arc::new(gcs_builder(bucket, root, credential_path).build()?) as Servicer;
    let context = OperationContext::new().with_http_transport(HttpTransporter::new(
        ReqwestTransport::new(reqwest::Client::new()),
    ));
    Ok(Operator::from_parts(context, service))
}

#[tokio::test]
async fn real_gcs_credential_path_gzip_contrast() -> Result<()> {
    let bucket =
        std::env::var("OPENDAL_GCS_BUCKET").context("GCS fixture bucket is unavailable")?;
    let base_root = std::env::var("OPENDAL_GCS_ROOT").unwrap_or_default();
    let run_suffix = format!("opendal-pr-review-8421-{}", Uuid::new_v4());
    let run_root = format!("{}/{run_suffix}", base_root.trim_end_matches('/'));
    let expected = std::env::var(EXPECTED_ENV).context("probe expectation is unavailable")?;

    let credential = TemporaryCredential::create()?;
    let credential_path = credential
        .path
        .to_str()
        .context("temporary credential path is not UTF-8")?;

    let plain_transport = HttpTransporter::new(ReqwestTransport::new(reqwest::Client::new()));
    let plain = build_operator(&bucket, &run_root, credential_path, plain_transport)?;
    let object_path = "object.txt";

    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("gzip"));
    let gzip_client = reqwest::Client::builder()
        .default_headers(headers)
        .no_gzip()
        .build()
        .context("failed to build gzip/no-gzip client")?;
    let observing_transport = TokenObservingTransport::new(ReqwestTransport::new(gzip_client));
    let observations = observing_transport.observations.clone();
    let custom = build_operator(
        &bucket,
        &run_root,
        credential_path,
        HttpTransporter::new(observing_transport),
    )?;
    let cleanup = build_exact_cleanup_operator(&bucket, &run_root, credential_path)?;

    println!("PR_REVIEW_8421 resource_suffix={run_suffix}");
    let write_result = plain.write(object_path, PROBE_PAYLOAD).await;
    let written_version = write_result
        .as_ref()
        .ok()
        .and_then(|metadata| metadata.version())
        .map(str::to_owned);
    println!("PR_REVIEW_8421 written_generation={written_version:?}");

    let read_result = match &write_result {
        Ok(_) => custom.read(object_path).await,
        Err(_) => Err(opendal::Error::new(
            ErrorKind::Unexpected,
            "skipped read after plain write failed",
        )),
    };
    let cleanup_result = match written_version.as_deref() {
        Some(version) => cleanup.delete_with(object_path).version(version).await,
        None => cleanup.delete(object_path).await,
    };

    let observations = observations.lock().expect("lock poisoned").clone();
    let token_status_ok = !observations.is_empty()
        && observations
            .iter()
            .all(|observation| observation.status == 200);
    let token_gzip = observations.iter().any(|observation| {
        observation.gzip_magic
            || observation
                .content_encoding
                .as_deref()
                .is_some_and(|encoding| encoding == "gzip")
    });
    let read_ok = read_result.is_ok();
    let read_bytes = read_result.as_ref().map_or(0, Buffer::len);
    let read_matches = read_result
        .as_ref()
        .ok()
        .is_some_and(|buffer| buffer.to_bytes().as_ref() == PROBE_PAYLOAD);
    let read_error_is_token = read_result.as_ref().err().is_some_and(|err| {
        let message = err.to_string();
        message.contains("failed to parse token response")
            || message.contains("failed to load signing credential")
    });

    println!(
        "PR_REVIEW_8421 expected={expected:?} token_requests={} token_statuses={:?} \
         content_encodings={:?} gzip_magic={:?} read_ok={read_ok} read_bytes={read_bytes} \
         read_matches={read_matches} read_error_is_token={read_error_is_token} cleanup_ok={} \
         cleanup_exact_version={}",
        observations.len(),
        observations
            .iter()
            .map(|observation| observation.status)
            .collect::<Vec<_>>(),
        observations
            .iter()
            .map(|observation| observation.content_encoding.clone())
            .collect::<Vec<_>>(),
        observations
            .iter()
            .map(|observation| observation.gzip_magic)
            .collect::<Vec<_>>(),
        cleanup_result.is_ok(),
        written_version.is_some(),
    );

    write_result.context("plain operator failed to create the probe object")?;
    if written_version.is_none() {
        bail!("plain write succeeded without a GCS generation/version");
    }
    cleanup_result.context("plain operator failed to delete the probe object")?;
    if observations.is_empty() {
        bail!("fresh custom operator did not perform a Google token exchange");
    }
    if !token_status_ok {
        bail!("Google token request did not return HTTP 200");
    }

    match expected.as_str() {
        "baseline_gzip_failure" => {
            if read_ok {
                bail!("baseline unexpectedly read the object successfully");
            }
            if !read_error_is_token {
                bail!("baseline read failed for a reason other than token parsing");
            }
            if !token_gzip {
                bail!("baseline token response was not observably gzip-compressed");
            }
        }
        "fixed_identity_success" => {
            if !read_ok || !read_matches {
                bail!("fixed head did not read the written payload correctly");
            }
            if token_gzip {
                bail!("fixed head token response was still gzip-compressed");
            }
        }
        value => bail!("unknown probe expectation {value:?}"),
    }

    Ok(())
}
