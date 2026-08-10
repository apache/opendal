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

//! Interactive connection check for the SharePoint service.
//!
//! Signs in with the OAuth 2.0 device authorization grant, then exercises a
//! full round trip against the configured document library folder.
//!
//! ```shell
//! cargo run -p opendal-service-sharepoint --example connection_check
//! ```
//!
//! Reads from `.env` (searched upwards from the working directory):
//!
//! - `OPENDAL_SHAREPOINT_FOLDER_URL` (required)
//! - `OPENDAL_SHAREPOINT_CLIENT_ID` (required)
//! - `OPENDAL_SHAREPOINT_TENANT_ID` (optional, defaults to `common`)
//! - `OPENDAL_SHAREPOINT_ROOT` (optional)
//! - `OPENDAL_SHAREPOINT_REFRESH_TOKEN` (optional; skips the interactive step)
//!
//! The app registration must be a public client with "Allow public client
//! flows" enabled, and must have the delegated `Sites.ReadWrite.All` and
//! `offline_access` permissions.
//!
//! Pass `--print-refresh-token` to print the refresh token obtained from an
//! interactive sign-in so it can be saved for non-interactive runs.

use std::env;
use std::io::Write as _;
use std::time::Duration;

use bytes::Buf;
use http::Request;
use http::StatusCode;
use http::header;
use opendal_core::Buffer;
use opendal_core::Error;
use opendal_core::ErrorKind;
use opendal_core::Operator;
use opendal_core::OperationContext;
use opendal_core::Result;
use opendal_core::raw::percent_encode_path;
use opendal_service_sharepoint::Sharepoint;
use serde::Deserialize;

/// Matches the scope requested by the service's own token refresh, so the
/// refresh token this yields is usable by the service unchanged.
const SCOPE: &str = "offline_access%20Sites.ReadWrite.All";

/// Values still carrying the `.env.example` placeholder are treated as unset.
fn env_var(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) => {
            let value = value.trim().to_string();
            if value.is_empty() || (value.starts_with('<') && value.ends_with('>')) {
                None
            } else {
                Some(value)
            }
        }
        Err(_) => None,
    }
}

fn require(key: &str) -> Result<String> {
    env_var(key).ok_or_else(|| {
        Error::new(
            ErrorKind::ConfigInvalid,
            format!("{key} must be set in the environment or .env"),
        )
    })
}

#[derive(Debug, Deserialize)]
struct DeviceCodeResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default = "default_interval")]
    interval: u64,
    message: Option<String>,
}

fn default_interval() -> u64 {
    5
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TokenErrorResponse {
    error: String,
    error_description: Option<String>,
}

async fn post_form(ctx: &OperationContext, url: &str, body: String) -> Result<(StatusCode, Buffer)> {
    let request = Request::post(url)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .body(Buffer::from(body))
        .map_err(|err| Error::new(ErrorKind::Unexpected, "build request").set_source(err))?;

    let response = ctx.http_transport().send(request).await?;
    let (parts, body) = response.into_parts();
    Ok((parts.status, body))
}

fn decode<T: serde::de::DeserializeOwned>(body: Buffer) -> Result<T> {
    serde_json::from_reader(body.reader())
        .map_err(|err| Error::new(ErrorKind::Unexpected, "decode response").set_source(err))
}

/// Run the device authorization grant and return `(access_token, refresh_token)`.
async fn device_code_login(
    ctx: &OperationContext,
    tenant_id: &str,
    client_id: &str,
) -> Result<(String, Option<String>)> {
    let tenant = percent_encode_path(tenant_id);

    let (status, body) = post_form(
        ctx,
        &format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/devicecode"),
        format!(
            "client_id={}&scope={SCOPE}",
            percent_encode_path(client_id)
        ),
    )
    .await?;

    if status != StatusCode::OK {
        let text = String::from_utf8_lossy(&body.to_bytes()).into_owned();
        return Err(Error::new(
            ErrorKind::ConfigInvalid,
            format!("device code request failed with {status}: {text}"),
        ));
    }

    let device: DeviceCodeResponse = decode(body)?;

    println!();
    match &device.message {
        Some(message) => println!("  {message}"),
        None => println!(
            "  Open {} and enter the code {}",
            device.verification_uri, device.user_code
        ),
    }
    println!();
    print!("  Waiting for sign-in");
    let _ = std::io::stdout().flush();

    let mut interval = Duration::from_secs(device.interval);
    let token_url = format!("https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token");
    let poll_body = format!(
        "grant_type=urn:ietf:params:oauth:grant-type:device_code&client_id={}&device_code={}",
        percent_encode_path(client_id),
        percent_encode_path(&device.device_code)
    );

    loop {
        tokio::time::sleep(interval).await;
        print!(".");
        let _ = std::io::stdout().flush();

        let (status, body) = post_form(ctx, &token_url, poll_body.clone()).await?;
        if status == StatusCode::OK {
            println!(" done");
            let token: TokenResponse = decode(body)?;
            if let Some(expires_in) = token.expires_in {
                println!("  Access token valid for {expires_in}s");
            }
            return Ok((token.access_token, token.refresh_token));
        }

        let err: TokenErrorResponse = decode(body)?;
        match err.error.as_str() {
            // The user has not finished signing in yet.
            "authorization_pending" => continue,
            // Microsoft asks us to back off; the documented step is 5 seconds.
            "slow_down" => {
                interval += Duration::from_secs(5);
                continue;
            }
            _ => {
                println!();
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    format!(
                        "device code sign-in failed: {} ({})",
                        err.error,
                        err.error_description.unwrap_or_default()
                    ),
                ));
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    opendal_http_transport_reqwest::install_default();

    let print_refresh_token = env::args().any(|arg| arg == "--print-refresh-token");

    let folder_url = require("OPENDAL_SHAREPOINT_FOLDER_URL")?;
    let client_id = require("OPENDAL_SHAREPOINT_CLIENT_ID")?;
    let tenant_id =
        env_var("OPENDAL_SHAREPOINT_TENANT_ID").unwrap_or_else(|| "common".to_string());
    let root = env_var("OPENDAL_SHAREPOINT_ROOT").unwrap_or_else(|| "/".to_string());

    println!("SharePoint connection check");
    println!("  folder_url: {folder_url}");
    println!("  tenant_id:  {tenant_id}");
    println!("  client_id:  {client_id}");
    println!("  root:       {root}");

    let mut builder = Sharepoint::default()
        .folder_url(&folder_url)
        .tenant_id(&tenant_id)
        .client_id(&client_id)
        .root(&root);

    // A stored refresh token makes the run non-interactive; the service refreshes
    // it on demand. Otherwise sign in interactively and pass the resulting
    // short-lived access token.
    let mut obtained_refresh_token = None;
    match env_var("OPENDAL_SHAREPOINT_REFRESH_TOKEN") {
        Some(refresh_token) => {
            println!("\n[1/6] Using OPENDAL_SHAREPOINT_REFRESH_TOKEN (no sign-in needed)");
            builder = builder.refresh_token(&refresh_token);
            if let Some(client_secret) = env_var("OPENDAL_SHAREPOINT_CLIENT_SECRET") {
                builder = builder.client_secret(&client_secret);
            }
        }
        None => {
            println!("\n[1/6] Signing in interactively (device code flow)");
            let ctx = OperationContext::new();
            let (access_token, refresh_token) =
                device_code_login(&ctx, &tenant_id, &client_id).await?;
            builder = builder.access_token(&access_token);
            obtained_refresh_token = refresh_token;
        }
    }

    let op = Operator::new(builder)?;
    println!("[2/6] Operator built");

    // `check` lists a single entry, which forces the folder URL to be resolved
    // through `/shares` and the resulting token to be exercised against Graph.
    op.check().await?;
    println!("[3/6] Reachable: folder URL resolved and credentials accepted");

    let entries = op.list("/").await?;
    println!("[4/6] Listed root: {} entries", entries.len());
    for entry in entries.iter().take(10) {
        let meta = entry.metadata();
        let kind = if meta.is_dir() { "dir " } else { "file" };
        println!("        {kind} {}", entry.path());
    }
    if entries.len() > 10 {
        println!("        ... and {} more", entries.len() - 10);
    }

    // Round-trip a throwaway file. The name is prefixed so a failed run leaves
    // something obviously identifiable behind.
    let probe_path = format!(
        ".opendal-connection-check-{}.txt",
        std::process::id()
    );
    let payload = b"opendal sharepoint connection check".as_slice();

    op.write(&probe_path, payload).await?;
    println!("[5/6] Wrote {probe_path}");

    let stat = op.stat(&probe_path).await?;
    let read_back = op.read(&probe_path).await?;
    let read_back = read_back.to_bytes();

    let mut round_trip_ok = true;
    if read_back.as_ref() != payload {
        println!("        MISMATCH: read back {} bytes, expected {}", read_back.len(), payload.len());
        round_trip_ok = false;
    }
    if stat.content_length() != payload.len() as u64 {
        println!(
            "        WARNING: stat reported {} bytes, expected {}",
            stat.content_length(),
            payload.len()
        );
    }

    op.delete(&probe_path).await?;
    println!("[6/6] Read back and deleted {probe_path}");

    println!();
    if round_trip_ok {
        println!("Connection check PASSED");
    } else {
        println!("Connection check FAILED: content did not round-trip");
    }

    if let Some(refresh_token) = obtained_refresh_token {
        println!();
        if print_refresh_token {
            println!("Refresh token (secret, store it safely):");
            println!("OPENDAL_SHAREPOINT_REFRESH_TOKEN={refresh_token}");
        } else {
            println!(
                "A refresh token was obtained ({} chars). Re-run with --print-refresh-token",
                refresh_token.len()
            );
            println!("to print it for OPENDAL_SHAREPOINT_REFRESH_TOKEN and skip sign-in next time.");
        }
    }

    if !round_trip_ok {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "content did not round-trip",
        ));
    }

    Ok(())
}
