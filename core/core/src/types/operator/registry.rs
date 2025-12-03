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
use std::sync::{LazyLock, Mutex, OnceLock};

use crate::types::builder::{Builder, Configurator};
use crate::types::{IntoOperatorUri, OperatorUri};
use crate::{Error, ErrorKind, Operator, Result};

/// Factory signature used to construct [`Operator`] from a URI and extra options.
pub type OperatorFactory = fn(&OperatorUri) -> Result<Operator>;

static DEFAULT_OPERATOR_REGISTRY: OnceLock<OperatorRegistry> = OnceLock::new();
static FALLBACK_OPERATOR_REGISTRY: LazyLock<OperatorRegistry> =
    LazyLock::new(OperatorRegistry::new);

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
    pub fn load(&self, uri: impl IntoOperatorUri) -> Result<Operator> {
        let parsed = uri.into_operator_uri()?;
        let scheme = parsed.scheme();

        let factory = self
            .factories
            .lock()
            .expect("operator registry mutex poisoned")
            .get(scheme)
            .copied()
            .ok_or_else(|| {
                Error::new(ErrorKind::Unsupported, "scheme is not registered")
                    .with_context("scheme", scheme.to_string())
            })?;

        factory(&parsed)
    }
}

/// Install a process-wide default registry. Should be called by facade during initialization.
pub fn set_default_operator_registry(registry: OperatorRegistry) {
    let _ = DEFAULT_OPERATOR_REGISTRY.set(registry);
}

/// Get the process-wide default registry. Falls back to an empty registry if unset.
pub fn default_operator_registry() -> &'static OperatorRegistry {
    DEFAULT_OPERATOR_REGISTRY
        .get()
        .unwrap_or_else(|| &FALLBACK_OPERATOR_REGISTRY)
}

/// Factory adapter that builds an operator from a configurator type.
fn factory<C: Configurator>(uri: &OperatorUri) -> Result<Operator> {
    let cfg = C::from_uri(uri)?;
    let builder = Operator::from_config(cfg)?;
    Ok(builder.finish())
}
