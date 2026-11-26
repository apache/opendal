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

#![cfg(not(target_arch = "wasm32"))]

use opendal::{ErrorKind, Operator, Result};

#[cfg(feature = "services-http")]
#[path = "operator_config/http.rs"]
mod http;

#[test]
fn enabled_layers_are_registered_by_the_facade() -> Result<()> {
    #[cfg(not(feature = "auto-register-services"))]
    opendal::init_default_registry();
    for (name, enabled, options) in [
        (
            "chaos",
            cfg!(feature = "layers-chaos"),
            "&layer.chaos.error_ratio=0",
        ),
        (
            "concurrent_limit",
            cfg!(feature = "layers-concurrent-limit"),
            "&layer.concurrent_limit.permits=1",
        ),
        ("logging", cfg!(feature = "layers-logging"), ""),
        ("mime_guess", cfg!(feature = "layers-mime-guess"), ""),
        ("retry", cfg!(feature = "layers-retry"), ""),
        (
            "throttle",
            cfg!(feature = "layers-throttle"),
            "&layer.throttle.bandwidth=1&layer.throttle.burst=1",
        ),
        ("timeout", cfg!(feature = "layers-timeout"), ""),
        ("tracing", cfg!(feature = "layers-tracing"), ""),
    ] {
        let result = Operator::from_uri(format!("memory:///?layers={name}{options}"));
        if enabled {
            result?;
        } else {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Unsupported, "{name}");
        }
    }
    Ok(())
}

#[test]
fn rejects_invalid_config_and_unregistered_components() {
    opendal::init_default_registry();
    for (query, kind) in [
        ("layers=retry,retry", ErrorKind::ConfigInvalid),
        ("layers=retry,", ErrorKind::ConfigInvalid),
        ("layers=nonexistent", ErrorKind::Unsupported),
        ("context.unknown.key=1", ErrorKind::Unsupported),
        #[cfg(feature = "layers-throttle")]
        (
            "layers=throttle&layer.throttle.bandwidth=0&layer.throttle.burst=1",
            ErrorKind::ConfigInvalid,
        ),
        #[cfg(feature = "layers-chaos")]
        (
            "layers=chaos&layer.chaos.error_ratio=1.5",
            ErrorKind::ConfigInvalid,
        ),
    ] {
        let error = Operator::from_uri(format!("memory:///?{query}")).unwrap_err();
        assert_eq!(error.kind(), kind, "{query}: {error}");
    }
}

#[cfg(all(
    feature = "services-s3",
    feature = "layers-logging",
    any(
        feature = "http-transport-reqwest-native-tls",
        feature = "http-transport-reqwest-rustls"
    ),
))]
#[test]
fn s3_uri_components_take_precedence_with_http_options_and_layers() -> Result<()> {
    #[cfg(not(feature = "auto-register-services"))]
    opendal::install_default();
    let op = Operator::from_uri((
        "s3://uri-bucket/uri-root?bucket=ignored-bucket&root=ignored-root",
        [
            ("region", "us-east-1"),
            ("disable_config_load", "true"),
            ("allow_anonymous", "true"),
            ("layers", "logging"),
            ("context.http.proxy", "http://127.0.0.1:9"),
        ],
    ))?;
    assert_eq!(op.info().name(), "uri-bucket");
    assert_eq!(op.info().root(), "/uri-root/");
    Ok(())
}

#[cfg(any(
    feature = "http-transport-reqwest-native-tls",
    feature = "http-transport-reqwest-rustls"
))]
#[test]
fn proxy_config_diagnostics_do_not_expose_values() -> Result<()> {
    use opendal::OperatorUri;

    #[cfg(not(feature = "auto-register-services"))]
    opendal::install_default();
    let dummy = "dummy-proxy-marker";
    let url = format!("http://dummy-user:{dummy}@127.0.0.1:9");
    let uri = OperatorUri::new("memory", [("context.http.proxy".to_owned(), url)])?;
    assert!(!format!("{uri:?}").contains(dummy));
    Operator::from_uri(uri)?;

    let invalid_url = format!("http://dummy-user:{dummy}@[broken-ipv6");
    let error =
        Operator::via_iter("memory", [("context.http.proxy".to_owned(), invalid_url)]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
    for diagnostic in [
        error.to_string(),
        format!("{error:?}"),
        format!("{error:#?}"),
    ] {
        assert!(!diagnostic.contains(dummy), "{diagnostic}");
    }
    Ok(())
}
