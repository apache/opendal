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

#![cfg(all(not(feature = "auto-register-services"), not(target_arch = "wasm32")))]

use http::{Request, Response};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, HttpTransporter, Operator};
use opendal::{OperatorRegistry, Result};

struct CustomDefault;

impl HttpTransport for CustomDefault {
    async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
        let stream = futures::stream::iter([Ok(Buffer::from("custom default"))]);
        Ok(Response::new(HttpBody::new(stream, None)))
    }
}

async fn assert_custom_default(op: &Operator) -> Result<()> {
    let request = Request::get("/object").body(Buffer::new()).unwrap();
    let response = op.context().http_transport().send(request).await?;
    assert_eq!(response.into_body().to_bytes(), "custom default");
    Ok(())
}

#[tokio::test]
async fn explicit_initialization_keeps_custom_defaults_and_registers_http_once() -> Result<()> {
    HttpTransporter::install_default(CustomDefault);
    opendal::init_default_registry();
    assert_custom_default(&Operator::from_uri("memory:///")?).await?;
    #[cfg(feature = "layers-retry")]
    assert_custom_default(&Operator::from_uri("memory:///?layers=retry")?).await?;
    let proxy_uri = "memory:///?context.http.proxy=http://127.0.0.1:9";
    let error = Operator::from_uri(proxy_uri).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert!(error.to_string().contains("context is not registered"));

    let registry = OperatorRegistry::get();
    registry.register_context("http", |_, _| {
        Err(Error::new(ErrorKind::Unexpected, "custom before install"))
    });
    assert_eq!(
        Operator::from_uri(proxy_uri).unwrap_err().message(),
        "custom before install"
    );

    opendal::install_default();
    let unknown_uri = "memory:///?context.http.unknown_key=1";
    if cfg!(any(
        feature = "http-transport-reqwest-native-tls",
        feature = "http-transport-reqwest-rustls",
        feature = "http-transport-reqwest-rustls-no-provider"
    )) {
        assert_custom_default(&Operator::from_uri(unknown_uri)?).await?;
    } else {
        assert_eq!(
            Operator::from_uri(unknown_uri).unwrap_err().message(),
            "custom before install"
        );
    }
    assert_custom_default(&Operator::from_uri("memory:///")?).await?;

    registry.register_context("http", |_, _| {
        Err(Error::new(ErrorKind::Unexpected, "custom after install"))
    });
    opendal::install_default();
    assert_eq!(
        Operator::from_uri(unknown_uri).unwrap_err().message(),
        "custom after install"
    );
    Ok(())
}
