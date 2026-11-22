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

#![doc = include_str!("../README.md")]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, doc(auto_cfg))]
#![deny(missing_docs)]

use std::fmt::Debug;

use opendal_core::raw::*;
use opendal_core::*;

/// `HttpConfigLayer` replaces an operator's HTTP transport with one built from
/// [`HttpConfig`].
///
/// Use it to give one operator its own proxy, timeouts, and user agent without
/// changing the process-wide environment or the default transport. The options
/// are defined by [`HttpConfig`] and implemented by the installed
/// [`HttpTransport`]; a transport that cannot apply them fails when the layer is
/// created.
///
/// # Construction
///
/// [`HttpConfigLayer::new`] resolves the configured transport immediately, so
/// invalid options fail while the layer is created rather than on the first
/// request. Applying the layer only clones the resolved transport handle.
///
/// # Replacement semantics
///
/// The layer sets the operator's transport instead of wrapping the current one,
/// because proxy and timeout options take effect when a connection is
/// established. Two consequences follow:
///
/// - The configured transport wins over a later context replacement. With this
///   layer applied, [`Operator::with_context`] replays the layer and replaces
///   any transport passed to [`OperationContext::with_http_transport`] again. A
///   caller that owns a custom transport must not use this layer and should call
///   [`HttpTransport::configure`] directly instead.
/// - Layer order matters. Apply this layer before layers that wrap the
///   transport, such as `ConcurrentLimitLayer`, so those wrappers wrap the
///   configured transport. Applying it after a wrapping layer replaces the
///   wrapper and silently drops it.
///
/// [`HttpTransport::configure`]: opendal_core::HttpTransport::configure
#[derive(Clone, Debug)]
pub struct HttpConfigLayer {
    config: HttpConfig,
    transport: HttpTransporter,
}

impl HttpConfigLayer {
    /// Build a layer that applies `config` to an operator.
    ///
    /// This resolves the configured transport from the installed default HTTP
    /// transport. It returns [`ErrorKind::ConfigInvalid`] when no default
    /// transport is installed or when `no_proxy` is set without `proxy`, and
    /// [`ErrorKind::Unsupported`] when the installed transport cannot apply
    /// [`HttpConfig`].
    pub fn new(config: HttpConfig) -> Result<Self> {
        if config.no_proxy.is_some() && config.proxy.is_none() {
            return Err(
                Error::new(ErrorKind::ConfigInvalid, "no_proxy requires proxy")
                    .with_operation("HttpConfigLayer::new")
                    .with_context(
                        "advice",
                        "set proxy, or configure the process-wide proxy environment variables",
                    ),
            );
        }

        let transport = HttpTransporter::from_config(&config)?;
        Ok(Self { config, transport })
    }

    /// Return the configuration this layer applies.
    pub fn config(&self) -> &HttpConfig {
        &self.config
    }
}

impl Layer for HttpConfigLayer {
    fn apply_context(&self, _srv: Servicer, inner: OperationContext) -> OperationContext {
        inner.with_http_transport(self.transport.clone())
    }
}
