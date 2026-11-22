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

use std::fmt::Debug;
use std::fmt::Formatter;

use super::HttpUri;
use crate::raw::SignedDuration;

/// Portable HTTP transport configuration.
///
/// `HttpConfig` describes how one operator talks HTTP. Pass it to
/// [`HttpTransport::configure`][crate::HttpTransport::configure] or apply it
/// through `HttpConfigLayer` from the `opendal-layer-http-config` crate to
/// replace that operator's transport.
///
/// The options apply to the operator, not to the process. Without a configured
/// transport, the HTTP client keeps reading environment proxy variables such as
/// `HTTP_PROXY`, `HTTPS_PROXY`, and `NO_PROXY`.
///
/// `HttpConfig` is a plain value: it does not parse text and is not a service
/// configurator. Each binding parses its own representation into this type.
#[derive(Clone, Default)]
pub struct HttpConfig {
    /// Proxy URL used for all requests from the configured transport.
    ///
    /// Setting this replaces environment proxy variables for the configured
    /// transport, including their exclusions. Use [`no_proxy`][Self::no_proxy]
    /// to exclude hosts from the proxy.
    ///
    /// The URL may carry credentials, for example
    /// `http://user:password@127.0.0.1:7890`.
    pub proxy: Option<String>,
    /// Hosts that bypass the configured [`proxy`][Self::proxy].
    ///
    /// The format is a comma-separated list of hosts and domains, matching the
    /// conventional `NO_PROXY` syntax. This option requires
    /// [`proxy`][Self::proxy]; without an explicit proxy the client keeps its
    /// environment proxy settings.
    pub no_proxy: Option<String>,
    /// Timeout for establishing a connection.
    pub connect_timeout: Option<SignedDuration>,
    /// Timeout for a single read from the connection.
    pub read_timeout: Option<SignedDuration>,
    /// Timeout for a whole HTTP request, including connecting, sending the
    /// request, and reading the response body.
    ///
    /// This bounds one request. To bound a whole storage operation, which may
    /// issue several requests and retries, combine it with `TimeoutLayer`.
    pub timeout: Option<SignedDuration>,
    /// `User-Agent` header sent with every request from the configured
    /// transport.
    pub user_agent: Option<String>,
}

impl Debug for HttpConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // A proxy URL may embed userinfo credentials.
        let proxy = self
            .proxy
            .as_deref()
            .map(|proxy| HttpUri::new(proxy).redacted_uri().to_string());
        f.debug_struct("HttpConfig")
            .field("proxy", &proxy)
            .field("no_proxy", &self.no_proxy)
            .field("connect_timeout", &self.connect_timeout)
            .field("read_timeout", &self.read_timeout)
            .field("timeout", &self.timeout)
            .field("user_agent", &self.user_agent)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_is_empty() {
        let config = HttpConfig::default();
        assert!(config.proxy.is_none());
        assert!(config.no_proxy.is_none());
        assert!(config.connect_timeout.is_none());
        assert!(config.read_timeout.is_none());
        assert!(config.timeout.is_none());
        assert!(config.user_agent.is_none());
    }

    #[test]
    fn test_debug_masks_proxy_credentials() {
        let config = HttpConfig {
            proxy: Some("http://user:password@127.0.0.1:7890".to_string()),
            ..Default::default()
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("user:password"), "{debug}");
        assert!(debug.contains("[REDACTED]@127.0.0.1:7890"), "{debug}");
    }
}
