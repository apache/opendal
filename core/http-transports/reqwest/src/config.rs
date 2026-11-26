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

use std::collections::HashMap;
use std::fmt;
#[cfg(not(target_arch = "wasm32"))]
use std::time::Duration;

use http::HeaderValue;
use opendal_core::raw::ConfigDeserializer;
#[cfg(not(target_arch = "wasm32"))]
use opendal_core::raw::signed_to_duration;
use opendal_core::{Error, ErrorKind, HttpTransporter, OperationContext, OperatorRegistry, Result};
use serde::Deserialize;

use crate::ReqwestTransport;

/// Register the reqwest factory for `context.http.*` operator options.
///
/// The factory supports `user_agent`, `proxy` (an HTTP or HTTPS URL),
/// `no_proxy` (a comma-separated bypass list for that proxy), and positive
/// `connect_timeout`, `read_timeout`, and `timeout` durations (for example,
/// `500ms` or `PT5S`). Empty or invalid values return
/// [`ErrorKind::ConfigInvalid`]. On native targets, `no_proxy` requires `proxy`.
/// SOCKS proxy URLs return [`ErrorKind::Unsupported`]. On Wasm, only
/// `user_agent` is supported, subject to the host Fetch API.
///
/// Recognized options replace the base HTTP transport with a new reqwest
/// client. With only unknown options, the factory preserves the input context.
/// The client uses the same TLS defaults as [`ReqwestTransport::default`].
pub fn register_http_context(registry: &OperatorRegistry) {
    registry.register_context("http", |ctx, options| {
        ReqwestConfig::parse(options)?.apply(ctx)
    });
}

#[derive(Deserialize)]
struct ReqwestConfig {
    proxy: Option<String>,
    no_proxy: Option<String>,
    connect_timeout: Option<String>,
    read_timeout: Option<String>,
    timeout: Option<String>,
    user_agent: Option<String>,
}

impl fmt::Debug for ReqwestConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReqwestConfig")
            .field("proxy", &self.proxy.as_ref().map(|_| "[REDACTED]"))
            .finish_non_exhaustive()
    }
}

impl ReqwestConfig {
    fn parse(options: HashMap<String, String>) -> Result<Self> {
        // ConfigDeserializer treats an empty string as an absent Option.
        for key in [
            "proxy",
            "no_proxy",
            "connect_timeout",
            "read_timeout",
            "timeout",
            "user_agent",
        ] {
            if options.get(key).is_some_and(String::is_empty) {
                return Err(invalid_option(key, "HTTP context option must not be empty"));
            }
        }

        Self::deserialize(ConfigDeserializer::new(options)).map_err(|_| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "invalid HTTP context configuration",
            )
        })
    }

    fn apply(self, ctx: OperationContext) -> Result<OperationContext> {
        let Self {
            proxy,
            no_proxy,
            connect_timeout,
            read_timeout,
            timeout,
            user_agent,
        } = self;
        let has_native_options = proxy.is_some()
            || no_proxy.is_some()
            || connect_timeout.is_some()
            || read_timeout.is_some()
            || timeout.is_some();
        if !has_native_options && user_agent.is_none() {
            return Ok(ctx);
        }
        #[cfg(target_arch = "wasm32")]
        if has_native_options {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "HTTP context proxy and timeout options are unavailable on Wasm",
            ));
        }

        let mut builder = reqwest::Client::builder();
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Some(proxy) = proxy {
                builder = builder.proxy(http_proxy(&proxy, no_proxy.as_deref())?);
            } else if no_proxy.is_some() {
                return Err(invalid_option(
                    "no_proxy",
                    "HTTP context no_proxy requires a proxy",
                ));
            }
            if let Some(value) = connect_timeout {
                builder = builder.connect_timeout(positive_duration("connect_timeout", &value)?);
            }
            if let Some(value) = read_timeout {
                builder = builder.read_timeout(positive_duration("read_timeout", &value)?);
            }
            if let Some(value) = timeout {
                builder = builder.timeout(positive_duration("timeout", &value)?);
            }
        }
        if let Some(value) = user_agent {
            let header = HeaderValue::try_from(value).map_err(|_| {
                invalid_option(
                    "user_agent",
                    "HTTP context user_agent must be a valid header value",
                )
            })?;
            builder = builder.user_agent(header);
        }

        // A client builder error can include the configuration that caused it.
        let client = builder.build().map_err(|_| {
            Error::new(
                ErrorKind::Unexpected,
                "failed to build reqwest client for HTTP context",
            )
        })?;
        Ok(ctx.with_http_transport(HttpTransporter::new(ReqwestTransport::new(client))))
    }
}

fn invalid_option(option: &'static str, message: &'static str) -> Error {
    Error::new(ErrorKind::ConfigInvalid, message).with_context("option", option)
}

#[cfg(not(target_arch = "wasm32"))]
fn http_proxy(value: &str, no_proxy: Option<&str>) -> Result<reqwest::Proxy> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| invalid_option("proxy", "HTTP context proxy must be an absolute URL"))?;
    if !url.has_host() {
        return Err(invalid_option(
            "proxy",
            "HTTP context proxy URL must include a host",
        ));
    }
    match url.scheme() {
        "http" | "https" => {}
        scheme if scheme.starts_with("socks") => {
            return Err(Error::new(
                ErrorKind::Unsupported,
                "SOCKS proxies are unavailable in HTTP context configuration",
            )
            .with_context("option", "proxy"));
        }
        _ => {
            return Err(invalid_option(
                "proxy",
                "HTTP context proxy URL must use http or https",
            ));
        }
    }

    let proxy = reqwest::Proxy::all(url)
        .map_err(|_| invalid_option("proxy", "invalid HTTP context proxy URL"))?;
    match no_proxy {
        Some(list) if list.split(',').all(|entry| entry.trim().is_empty()) => Err(invalid_option(
            "no_proxy",
            "HTTP context no_proxy must contain a bypass entry",
        )),
        Some(list) => Ok(proxy.no_proxy(reqwest::NoProxy::from_string(list))),
        None => Ok(proxy),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn positive_duration(option: &'static str, value: &str) -> Result<Duration> {
    let invalid = || invalid_option(option, "HTTP context timeout must be a positive duration");
    let duration = signed_to_duration(value).map_err(|_| invalid())?;
    if duration.is_zero() {
        return Err(invalid());
    }
    Ok(duration)
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use super::*;

    #[test]
    fn empty_recognized_values_are_invalid() {
        for option in [
            "proxy",
            "no_proxy",
            "connect_timeout",
            "read_timeout",
            "timeout",
            "user_agent",
        ] {
            let error = ReqwestConfig::parse(HashMap::from([(option.to_owned(), String::new())]))
                .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ConfigInvalid);
            assert!(error.to_string().contains(option));
        }
    }

    #[test]
    fn invalid_user_agent_has_safe_diagnostics() {
        assert_rejected(
            "user_agent",
            "dummy-user-agent\r\n",
            ErrorKind::ConfigInvalid,
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn rejects_invalid_proxy_urls_and_unsupported_socks() {
        for value in [
            "http://",
            "http://:4317",
            "proxy.invalid:4317",
            "http://dummy-user:dummy-pass@[malformed-ipv6",
            "ftp://dummy-user:dummy-pass@proxy.invalid:4317",
        ] {
            assert_rejected("proxy", value, ErrorKind::ConfigInvalid);
        }
        assert_rejected(
            "proxy",
            "socks5h://dummy-user:dummy-pass@proxy.invalid:4317",
            ErrorKind::Unsupported,
        );
        assert_rejected("no_proxy", "localhost", ErrorKind::ConfigInvalid);
        assert!(http_proxy("https://localhost:4317", None).is_ok());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn rejects_invalid_or_nonpositive_timeouts() {
        for option in ["connect_timeout", "read_timeout", "timeout"] {
            for value in ["dummy-timeout", "0s", "-1s", "P1M"] {
                assert_rejected(option, value, ErrorKind::ConfigInvalid);
            }
        }
    }

    #[test]
    fn config_debug_redacts_even_malformed_proxy_credentials() -> Result<()> {
        let config = ReqwestConfig::parse(HashMap::from([(
            "proxy".to_owned(),
            "http://dummy-user:dummy-pass@[malformed-ipv6".to_owned(),
        )]))?;
        for diagnostic in [format!("{config:?}"), format!("{config:#?}")] {
            assert!(diagnostic.contains("[REDACTED]"));
            assert!(!diagnostic.contains("dummy-"));
        }
        Ok(())
    }

    fn assert_rejected(option: &str, value: &str, kind: ErrorKind) {
        let error = ReqwestConfig::parse(HashMap::from([(option.to_owned(), value.to_owned())]))
            .and_then(|config| config.apply(OperationContext::default()))
            .unwrap_err();
        assert_eq!(error.kind(), kind, "{error}");
        for diagnostic in [
            error.to_string(),
            format!("{error:?}"),
            format!("{error:#?}"),
        ] {
            assert!(diagnostic.contains(option), "{diagnostic}");
            assert!(!diagnostic.contains(value.trim_end()), "{diagnostic}");
            assert!(!diagnostic.contains("dummy-"), "{diagnostic}");
        }
        assert!(error.source().is_none());
    }
}
