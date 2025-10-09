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
use std::sync::{LazyLock, Mutex};

use http::Uri;
use percent_encoding::percent_decode_str;

use crate::services;
use crate::types::builder::{Builder, Configurator};
use crate::{Error, ErrorKind, Operator, Result};

/// Factory signature used to construct [`Operator`] from a URI and extra options.
pub type OperatorFactory = fn(&str, Vec<(String, String)>) -> Result<Operator>;

/// Default registry initialized with builtin services.
pub static DEFAULT_OPERATOR_REGISTRY: LazyLock<OperatorRegistry> = LazyLock::new(|| {
    let registry = OperatorRegistry::new();
    register_builtin_services(&registry);
    registry
});

/// Global registry that maps schemes to [`OperatorFactory`] functions.
#[derive(Debug, Default)]
pub struct OperatorRegistry {
    factories: Mutex<HashMap<String, OperatorFactory>>,
}

impl OperatorRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            factories: Mutex::new(HashMap::new()),
        }
    }

    /// Register a builder for the given scheme.
    pub fn register<B: Builder>(&self, scheme: &str) {
        let key = scheme.to_ascii_lowercase();
        let mut guard = self
            .factories
            .lock()
            .expect("operator registry mutex poisoned");
        guard.insert(key, factory::<B::Config>);
    }

    /// Load an [`Operator`] via the factory registered for the URI's scheme.
    pub fn load(
        &self,
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        let parsed = uri.parse::<Uri>().map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "failed to parse uri").set_source(err)
        })?;

        let scheme = parsed
            .scheme_str()
            .ok_or_else(|| Error::new(ErrorKind::ConfigInvalid, "uri scheme is required"))?;

        let key = scheme.to_ascii_lowercase();
        let factory = self
            .factories
            .lock()
            .expect("operator registry mutex poisoned")
            .get(key.as_str())
            .copied()
            .ok_or_else(|| {
                Error::new(ErrorKind::Unsupported, "scheme is not registered")
                    .with_context("scheme", scheme)
            })?;

        let opts: Vec<(String, String)> = options.into_iter().collect();
        factory(uri, opts)
    }
}

fn register_builtin_services(registry: &OperatorRegistry) {
    #[cfg(feature = "services-memory")]
    registry.register::<services::Memory>(services::MEMORY_SCHEME);
    #[cfg(feature = "services-fs")]
    registry.register::<services::Fs>(services::FS_SCHEME);
    #[cfg(feature = "services-s3")]
    registry.register::<services::S3>(services::S3_SCHEME);
}

/// Factory adapter that builds an operator from a configurator type.
fn factory<C: Configurator>(uri: &str, options: Vec<(String, String)>) -> Result<Operator> {
    let parsed = uri.parse::<Uri>().map_err(|err| {
        Error::new(ErrorKind::ConfigInvalid, "failed to parse uri").set_source(err)
    })?;

    let mut params = HashMap::new();
    if let Some(query) = parsed.query() {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let mut parts = pair.splitn(2, '=');
            let key = parts.next().unwrap_or("");
            let value = parts.next().unwrap_or("");
            let key = percent_decode_str(key).decode_utf8_lossy().to_string();
            let value = percent_decode_str(value).decode_utf8_lossy().to_string();
            params.insert(key.to_ascii_lowercase(), value);
        }
    }

    for (key, value) in options {
        params.insert(key.to_ascii_lowercase(), value);
    }

    let cfg = C::from_uri(&parsed, &params)?;
    Ok(Operator::from_config(cfg)?.finish())
}
