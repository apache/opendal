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

use std::error::Error;
use std::time::Duration;

use opendal::HttpTransporter;
use opendal::OperationContext;
use opendal::Operator;
use opendal::services::Http;
use opendal_http_transport_reqwest::ReqwestTransport;
use rustls::ClientConfig;
use rustls::RootCertStore;

const ENDPOINT: &str = "https://www.iana.org";
const PATH: &str = "domains/reserved";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    rustls::crypto::default_fips_provider()
        .install_default()
        .expect("a Rustls crypto provider is already installed");

    let roots = RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    assert!(config.fips(), "expected a FIPS TLS client configuration");

    let client = reqwest::Client::builder()
        .tls_backend_preconfigured(config)
        .https_only(true)
        .timeout(Duration::from_secs(15))
        .build()?;
    let transport = HttpTransporter::new(ReqwestTransport::new(client));
    let context = OperationContext::new().with_http_transport(transport);
    let op = Operator::new(Http::default().endpoint(ENDPOINT))?.with_context(context);

    let body = op.read(PATH).await?.to_bytes();
    assert!(std::str::from_utf8(&body)?.contains("IANA-managed Reserved Domains"));
    println!("Rustls ClientConfig::fips() = true; HTTPS read from {ENDPOINT}/{PATH} succeeded");
    Ok(())
}
